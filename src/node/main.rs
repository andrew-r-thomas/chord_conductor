use std::{collections::BTreeMap, env, fs::File, sync::Arc};

use ethnum::U256;
use itertools::Itertools;
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::RwLock,
    task::JoinSet,
    time::{interval, Duration},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, instrument};

pub mod successor;
mod node_service {
    tonic::include_proto!("node");
}
use node_service::{
    get_request::Getter,
    get_response,
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    notify_response::HasData,
    set_request::Setter,
    Data, FindSuccRequest, FindSuccResponse, GetRequest, GetResponse, GetStateRequest,
    GetStateResponse, JoinRequest, JoinResponse, NotifyRequest, NotifyResponse, PredRequest,
    PredResponse, SetRequest, SetResponse,
};
use successor::Successor;

#[derive(Debug)]
pub struct Node {
    addr: String,
    id: U256,
    data: Arc<RwLock<BTreeMap<U256, String>>>,
    pred: Arc<RwLock<Option<PredHandle>>>,
    finger_table: Arc<Vec<Successor>>,
}

#[derive(Debug, Clone)]
pub struct PredHandle {
    addr: String,
    hash: U256,
}

// TODO: this pattern matching is gonna be real slow, might not be avoidable
// but if it is we should try to
#[tonic::async_trait]
impl NodeService for Node {
    async fn find_succ(
        &self,
        request: Request<FindSuccRequest>,
    ) -> Result<Response<FindSuccResponse>, Status> {
        todo!()
    }

    #[instrument(skip(self))]
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        info!("called set");
        let request_message = request.into_inner();
        let hash = match request_message.setter {
            Some(Setter::Key(key)) => {
                let mut hasher = Sha256::new();
                hasher.update(key);
                let h = hasher.finalize();
                U256::from_be_bytes(h.into())
            }
            Some(Setter::Hash(hash)) => U256::from_be_bytes(hash.try_into().unwrap()),
            None => panic!(),
        };

        info!("waiting for locks in set");
        let pred_read = { self.pred.read().await.clone() };
        info!("got pred read in set");
        let mut succ_write = { self.succ.write().await.clone() };
        info!("got succ write in set");

        let resp = match succ_write.is_none() {
            true => {
                info!("set succ is none");
                let mut data_write = self.data.write().await;
                data_write.insert(hash, request_message.val);
                Ok(Response::new(SetResponse {
                    loc: self.addr.clone(),
                }))
            }
            false => {
                info!("set succ is some");
                match pred_read.is_none() {
                    true => {
                        info!("pred read is none in set");
                        let mut data_write = self.data.write().await;
                        data_write.insert(hash, request_message.val);
                        Ok(Response::new(SetResponse {
                            loc: self.addr.clone(),
                        }))
                    }
                    false => {
                        info!("pred read is some in set");
                        match Self::within_range(&hash, &pred_read.as_ref().unwrap().hash, &self.id)
                        {
                            true => {
                                info!("pred within range");
                                let mut data_write = self.data.write().await;
                                data_write.insert(hash, request_message.val);
                                Ok(Response::new(SetResponse {
                                    loc: self.addr.clone(),
                                }))
                            }
                            false => {
                                info!("pred not in range");
                                succ_write
                                    .as_mut()
                                    .unwrap()
                                    .client
                                    .set(Request::new(SetRequest {
                                        val: request_message.val,
                                        setter: Some(Setter::Hash(hash.to_be_bytes().to_vec())),
                                    }))
                                    .await
                            }
                        }
                    }
                }
            }
        };
        info!("returning set");
        resp
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request_message = request.into_inner();
        let pred_read = { self.pred.read().await.clone() };
        let mut succ_write = { self.succ.read().await.clone() };

        let hash = match request_message.getter {
            Some(Getter::Key(key)) => {
                let mut hasher = Sha256::new();
                hasher.update(key);
                U256::from_be_bytes(hasher.finalize().into())
            }
            Some(Getter::Hash(hash)) => U256::from_be_bytes(hash.try_into().unwrap()),
            None => panic!(),
        };

        match succ_write.is_none() {
            true => {
                let data_write = self.data.write().await;
                let result = match data_write.get(&hash) {
                    Some(v) => Some(get_response::Result::Val(v.into())),
                    None => None,
                };
                Ok(Response::new(GetResponse {
                    result,
                    loc: self.addr.clone(),
                }))
            }
            false => match pred_read.is_none() {
                true => {
                    let data_write = self.data.write().await;
                    let result = match data_write.get(&hash) {
                        Some(v) => Some(get_response::Result::Val(v.into())),
                        None => None,
                    };
                    Ok(Response::new(GetResponse {
                        result,
                        loc: self.addr.clone(),
                    }))
                }
                false => {
                    match Self::within_range(&hash, &pred_read.as_ref().unwrap().hash, &self.id) {
                        true => {
                            let data_write = self.data.write().await;
                            let result = match data_write.get(&hash) {
                                Some(v) => Some(get_response::Result::Val(v.into())),
                                None => None,
                            };
                            Ok(Response::new(GetResponse {
                                result,
                                loc: self.addr.clone(),
                            }))
                        }
                        false => {
                            succ_write
                                .as_mut()
                                .unwrap()
                                .client
                                .get(Request::new(GetRequest {
                                    getter: Some(Getter::Hash(hash.to_be_bytes().to_vec())),
                                }))
                                .await
                        }
                    }
                }
            },
        }
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request_message = request.into_inner();

        let mut hasher = Sha256::new();
        hasher.update(request_message.ip.as_bytes());
        let hash = U256::from_be_bytes(hasher.finalize().into());

        let pred_read = { self.pred.read().await.clone() };
        let mut succ_write = { self.succ.write().await.clone() };

        match succ_write.is_none() {
            true => Ok(Response::new(JoinResponse {
                succ_ip: self.addr.clone(),
                succ_id: self.id.clone().to_be_bytes().to_vec(),
            })),
            false => match pred_read.is_none() {
                true => Ok(Response::new(JoinResponse {
                    succ_ip: self.addr.clone(),
                    succ_id: self.id.clone().to_be_bytes().to_vec(),
                })),
                false => {
                    match Self::within_range(&hash, &pred_read.as_ref().unwrap().hash, &self.id) {
                        true => Ok(Response::new(JoinResponse {
                            succ_ip: self.addr.clone(),
                            succ_id: self.id.clone().to_be_bytes().to_vec(),
                        })),
                        false => {
                            succ_write
                                .as_mut()
                                .unwrap()
                                .client
                                .join(Request::new(request_message))
                                .await
                        }
                    }
                }
            },
        }
    }

    async fn pred(&self, _request: Request<PredRequest>) -> Result<Response<PredResponse>, Status> {
        let pred_read = self.pred.read().await;

        match pred_read.is_none() {
            true => Ok(Response::new(PredResponse {
                addr: self.addr.clone(),
                hash: self.id.clone().to_be_bytes().to_vec(),
            })),
            false => {
                let pred = pred_read.as_ref().unwrap();
                Ok(Response::new(PredResponse {
                    addr: pred.addr.clone(),
                    hash: pred.hash.clone().to_be_bytes().to_vec(),
                }))
            }
        }
    }

    #[instrument(skip(self))]
    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        info!("notify called");
        let request_message = request.into_inner();
        let mut pred_write = self.pred.write().await;
        info!("got pred write in notify");

        let hash = U256::from_be_bytes(request_message.hash.try_into().unwrap());
        match pred_write.is_none() {
            true => {
                let mut data_write = self.data.write().await;
                let to_send = Self::shed_data(&mut data_write, &hash, &self.id, true);
                *pred_write = Some(PredHandle {
                    addr: request_message.addr,
                    hash,
                });
                Ok(Response::new(NotifyResponse {
                    has_data: Some(HasData::Data(Data {
                        keys: to_send.0,
                        vals: to_send.1,
                    })),
                }))
            }
            false => {
                match Self::within_range(&hash, &pred_write.as_ref().unwrap().hash, &self.id) {
                    true => {
                        let mut data_write = self.data.write().await;
                        let to_send = Self::shed_data(&mut data_write, &hash, &self.id, true);
                        *pred_write = Some(PredHandle {
                            addr: request_message.addr,
                            hash,
                        });
                        Ok(Response::new(NotifyResponse {
                            has_data: Some(HasData::Data(Data {
                                keys: to_send.0,
                                vals: to_send.1,
                            })),
                        }))
                    }
                    false => Ok(Response::new(NotifyResponse { has_data: None })),
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        info!("called get state");
        let succ_read = { self.succ.read().await.clone() };
        let len = { self.data.read().await.len() };

        // TODO: maybe just return None for empty vals
        let succ = {
            if succ_read.is_none() {
                "".to_string()
            } else {
                succ_read.as_ref().unwrap().addr.clone()
            }
        };

        Ok(Response::new(GetStateResponse {
            succ,
            len: len as u32,
        }))
    }
}

impl Node {
    pub fn new(
        addr: String,
        finger_table: Arc<Vec<Successor>>,
        data: Arc<RwLock<BTreeMap<U256, String>>>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(addr.as_bytes());
        let id = U256::from_be_bytes(hasher.finalize().into());

        Self {
            pred: Arc::new(RwLock::new(None)),
            id,
            addr,
            data,
            finger_table,
        }
    }

    #[instrument(skip(self))]
    pub fn start_periodics(
        &self,
        stabilize_interval: Duration,
        fix_fingers_interval: Duration,
        join_set: &mut JoinSet<()>,
    ) {
        let stabilize_task_id = self.id.clone();
        let stabilize_task_addr = self.addr.clone();
        let stabilize_task_pred = self.pred.clone();
        let stabilize_task_data = self.data.clone();
        let stabilize_finger_table = self.finger_table.clone();

        join_set.spawn(async move {
            // stabilize
            let mut interval = interval(stabilize_interval);
            loop {
                interval.tick().await;
                let pred_read = { stabilize_task_pred.read().await.clone() };

                let successor = stabilize_finger_table.get(0).unwrap();
                let succ_write = { successor.recv.borrow().clone() };

                match succ_write {
                    Some(mut s) => {
                        let pred_resp = s.client.pred(Request::new(PredRequest {})).await.unwrap();
                        let pred_data = pred_resp.into_inner();
                        let hash = U256::from_be_bytes(pred_data.hash.try_into().unwrap());
                        match Self::within_range(&hash, &stabilize_task_id, &s.hash) {
                            true => {
                                let mut succ_client = NodeServiceClient::connect(format!(
                                    "http://{}",
                                    pred_data.addr
                                ))
                                .await
                                .unwrap();

                                let notify_resp = succ_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: stabilize_task_addr.clone(),
                                        hash: stabilize_task_id.clone().to_be_bytes().to_vec(),
                                    }))
                                    .await
                                    .unwrap();
                                let notify_data = notify_resp.into_inner();

                                match notify_data.has_data {
                                    Some(HasData::Data(Data { keys, vals })) => {
                                        let mut to_append = BTreeMap::<U256, String>::from_iter(
                                            keys.into_iter()
                                                .map(|k| U256::from_be_bytes(k.try_into().unwrap()))
                                                .zip(vals.into_iter()),
                                        );
                                        let mut data_write = stabilize_task_data.write().await;
                                        data_write.append(&mut to_append);
                                    }
                                    None => {}
                                }

                                successor
                                    .send
                                    .send(successor::SuccessorData {
                                        addr: pred_data.addr,
                                        hash,
                                        client: succ_client,
                                    })
                                    .unwrap();
                            }
                            false => {
                                let notify_resp = s
                                    .client
                                    .notify(Request::new(NotifyRequest {
                                        addr: stabilize_task_addr.clone(),
                                        hash: stabilize_task_id.clone().to_be_bytes().to_vec(),
                                    }))
                                    .await
                                    .unwrap();
                                let notify_data = notify_resp.into_inner();
                                match notify_data.has_data {
                                    Some(HasData::Data(Data { keys, vals })) => {
                                        let mut to_append = BTreeMap::<U256, String>::from_iter(
                                            keys.into_iter()
                                                .map(|k| U256::from_be_bytes(k.try_into().unwrap()))
                                                .zip(vals.into_iter()),
                                        );
                                        let mut data_write = stabilize_task_data.write().await;
                                        data_write.append(&mut to_append);
                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                    None => match pred_read.is_none() {
                        true => {}
                        false => {
                            let pred_handle = pred_read.as_ref().unwrap();
                            let mut pred_client =
                                NodeServiceClient::connect(format!("http://{}", pred_handle.addr))
                                    .await
                                    .unwrap();

                            let notify_resp = pred_client
                                .notify(Request::new(NotifyRequest {
                                    addr: stabilize_task_addr.clone(),
                                    hash: stabilize_task_id.clone().to_be_bytes().to_vec(),
                                }))
                                .await
                                .unwrap();
                            let notify_data = notify_resp.into_inner();

                            match notify_data.has_data {
                                Some(HasData::Data(Data { keys, vals })) => {
                                    let mut to_append = BTreeMap::<U256, String>::from_iter(
                                        keys.into_iter()
                                            .map(|k| U256::from_be_bytes(k.try_into().unwrap()))
                                            .zip(vals.into_iter()),
                                    );
                                    let mut data_write = stabilize_task_data.write().await;
                                    data_write.append(&mut to_append);
                                }
                                None => {}
                            }
                            successor
                                .send
                                .send(successor::SuccessorData {
                                    addr: pred_handle.addr.clone(),
                                    hash: pred_handle.hash.clone(),
                                    client: pred_client,
                                })
                                .unwrap()
                        }
                    },
                }
            }
        });

        let fix_fingers_task_finger_table = self.finger_table.clone();
        let fix_fingers_hash = self.id.clone();
        let fix_fingers_addr = self.addr.clone();
        join_set.spawn(async move {
            // fix fingers
            let mut next: u32 = 0;
            let mut interval = interval(fix_fingers_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                next += 1;
                if next > 255 {
                    next = 0;
                }

                let finger = fix_fingers_task_finger_table.get(next as usize).unwrap();
                let succesor = Self::find_succ(
                    fix_fingers_addr.clone(),
                    fix_fingers_hash.clone(),
                    &fix_fingers_task_finger_table,
                    &(fix_fingers_hash + U256::new(2).pow(next)),
                )
                .await;

                if let Some(s) = { finger.recv.borrow().clone() } {
                    if s.addr == succesor.0 {
                        continue;
                    }
                }
                let client = NodeServiceClient::connect(format!("http://{}", succesor.0))
                    .await
                    .unwrap();

                finger
                    .send
                    .send(successor::SuccessorData {
                        addr: succesor.0,
                        hash: succesor.1,
                        client,
                    })
                    .unwrap();
            }
        });
    }

    pub async fn find_succ(
        self_addr: String,
        self_hash: U256,
        finger_table: &Vec<Successor>,
        hash: &U256,
    ) -> (String, U256) {
        let mut successor = (self_addr, self_hash);

        let maybe_succ = { finger_table.get(0).unwrap().recv.borrow().clone() };
        if let Some(succ) = maybe_succ {
            if Self::within_range(&hash, &self_hash, &succ.hash) {
                successor = (succ.addr, succ.hash);
            } else {
                for finger in finger_table.iter().rev() {
                    let maybe_s = { finger.recv.borrow().clone() };
                    if let Some(mut s) = maybe_s {
                        if Self::within_range(&hash, &self_hash, &s.hash) {
                            let find_succ_resp = s
                                .client
                                .find_succ(Request::new(FindSuccRequest {
                                    hash: hash.to_be_bytes().to_vec(),
                                }))
                                .await
                                .unwrap();

                            let find_succ_data = find_succ_resp.into_inner();
                            successor = (
                                find_succ_data.addr,
                                U256::from_be_bytes(find_succ_data.hash.try_into().unwrap()),
                            );
                        }
                    }
                }
            }
        }

        successor
    }

    pub fn within_range(target: &U256, start: &U256, end: &U256) -> bool {
        match start > end {
            true => target > start || target < end,
            false => target > start && target < end,
        }
    }

    pub fn shed_data(
        data: &mut BTreeMap<U256, String>,
        split_start: &U256,
        split_end: &U256,
        // TODO: might not need this is we're only using the function in notify
        keep_split: bool,
    ) -> (Vec<Vec<u8>>, Vec<String>) {
        match (keep_split, split_start > split_end) {
            (true, true) => {
                let mut first_keep = data.split_off(split_start);
                let to_send = data.split_off(split_end);

                data.append(&mut first_keep);

                (
                    to_send
                        .clone()
                        .into_keys()
                        .map(|k| k.to_be_bytes().to_vec())
                        .collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
            (true, false) => {
                let mut to_send = data.split_off(split_end);
                let to_keep = data.split_off(split_start);

                to_send.append(data);
                *data = to_keep;

                (
                    to_send
                        .clone()
                        .into_keys()
                        .map(|k| k.to_be_bytes().to_vec())
                        .collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
            (false, true) => {
                let mut to_send = data.split_off(split_start);
                let to_keep = data.split_off(split_end);

                to_send.append(data);
                *data = to_keep;

                (
                    to_send
                        .clone()
                        .into_keys()
                        .map(|k| k.to_be_bytes().to_vec())
                        .collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
            (false, false) => {
                let mut to_keep = data.split_off(split_end);
                let to_send = data.split_off(split_start);
                data.append(&mut to_keep);

                (
                    to_send
                        .clone()
                        .into_keys()
                        .map(|k| k.to_be_bytes().to_vec())
                        .collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().collect_vec();

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_string = addr.to_string();
    println!("{addr_string}");

    let log_file = File::create(format!("logs/{addr_string}_log.txt")).unwrap();
    tracing_subscriber::fmt()
        .with_writer(log_file)
        .compact()
        .init();

    let mut task_join_set = JoinSet::new();
    let first_succ = match args.get(1) {
        Some(join_addr) => {
            let join_resp_data = {
                let mut join_client =
                    NodeServiceClient::connect(format!("http://{}", join_addr.clone())).await?;
                let join_resp = join_client
                    .join(Request::new(JoinRequest {
                        ip: addr_string.clone(),
                    }))
                    .await?;
                join_resp.into_inner()
            };

            Successor::spawn(
                Some(successor::SuccessorData {
                    addr: join_resp_data.succ_ip.clone(),
                    hash: U256::from_be_bytes(join_resp_data.succ_id.try_into().unwrap()),
                    client: NodeServiceClient::connect(format!(
                        "http://{}",
                        join_resp_data.succ_ip
                    ))
                    .await
                    .unwrap(),
                }),
                &mut task_join_set,
            )
        }
        None => Successor::spawn(None, &mut task_join_set),
    };

    let mut succs = vec![first_succ];
    for _ in 1..256 {
        succs.push(Successor::spawn(None, &mut task_join_set));
    }

    let node = Node::new(
        addr_string,
        Arc::new(succs),
        Arc::new(RwLock::new(BTreeMap::<U256, String>::new())),
    );

    // TODO: figure out timing
    node.start_periodics(
        Duration::from_millis(100),
        Duration::from_millis(100),
        &mut task_join_set,
    );

    // TODO: also figure out the orders of these awaits
    // or if we should even do them
    Server::builder()
        .add_service(NodeServiceServer::new(node))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .unwrap();

    task_join_set.join_all().await;

    Ok(())
}
