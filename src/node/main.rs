use std::{collections::BTreeMap, env, sync::Arc};

use ethnum::U256;
use itertools::Itertools;
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{watch, RwLock},
    task::JoinSet,
    time::{interval, Duration},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod predecessor;
pub mod successor;
mod node_service {
    tonic::include_proto!("node");
}
use node_service::{
    get_response,
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    notify_response::HasData,
    set_request::Setter,
    Data, FindSuccRequest, FindSuccResponse, GetRequest, GetResponse, GetStateRequest,
    GetStateResponse, JoinRequest, JoinResponse, NotifyRequest, NotifyResponse, PredRequest,
    PredResponse, Quote, SetRequest, SetResponse, UpdateFixFingFreqRequest,
    UpdateFixFingFreqResponse, UpdateStabFreqRequest, UpdateStabFreqResponse,
};
use predecessor::Predecessor;
use successor::Successor;

/*
ok so what do we want to do here

make things configurable live:
- number of nodes
- failure rate of nodes
- frequency of periodics

node failure
node planed leave
data loss
*/

#[derive(Debug)]
pub struct Node {
    addr: String,
    id: U256,
    data: Arc<RwLock<BTreeMap<U256, Quote>>>,
    pred: Arc<Predecessor>,
    finger_table: Arc<Vec<Successor>>,
    stab_freq_send: watch::Sender<u64>,
    fix_fing_freq_send: watch::Sender<u64>,
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn find_succ(
        &self,
        request: Request<FindSuccRequest>,
    ) -> Result<Response<FindSuccResponse>, Status> {
        let request_data = request.into_inner();

        let succ = Self::find_succ(
            self.addr.clone(),
            self.id.clone(),
            &self.finger_table,
            &U256::from_be_bytes(request_data.hash.try_into().unwrap()),
        )
        .await;

        Ok(Response::new(FindSuccResponse {
            addr: succ.0,
            hash: succ.1.to_be_bytes().to_vec(),
        }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();
        let hash = match request_message.setter {
            Some(Setter::Key(hash)) => U256::from_be_bytes(hash.try_into().unwrap()),
            None => {
                let mut hasher = Sha256::new();
                hasher.update(request_message.val.clone().unwrap().quote);
                U256::from_be_bytes(hasher.finalize().into())
            }
        };

        let pred = { self.pred.recv.borrow().clone() };

        match pred {
            None => {
                let mut data_write = self.data.write().await;
                data_write.insert(hash, request_message.val.unwrap());
                Ok(Response::new(SetResponse {
                    hash: hash.to_be_bytes().into(),
                    path_len: 0,
                }))
            }
            Some(p) => match Self::within_range(&hash, &p.hash, &self.id) {
                true => {
                    let mut data_write = self.data.write().await;
                    data_write.insert(hash, request_message.val.unwrap());
                    Ok(Response::new(SetResponse {
                        hash: hash.to_be_bytes().into(),
                        path_len: 0,
                    }))
                }
                false => {
                    let mut out = None;
                    for s in self.finger_table.iter().rev() {
                        if let Some(ss) = { s.recv.borrow().clone() } {
                            out = Some(ss.client);
                        }
                    }

                    match out {
                        Some(mut c) => {
                            let resp = c
                                .set(Request::new(SetRequest {
                                    val: request_message.val,
                                    setter: Some(Setter::Key(hash.to_be_bytes().into())),
                                }))
                                .await
                                .unwrap();
                            let d = resp.into_inner();
                            Ok(Response::new(SetResponse {
                                hash: d.hash,
                                path_len: d.path_len + 1,
                            }))
                        }
                        None => {
                            let mut data_write = self.data.write().await;
                            data_write.insert(hash, request_message.val.unwrap());
                            Ok(Response::new(SetResponse {
                                hash: hash.to_be_bytes().into(),
                                path_len: 0,
                            }))
                        }
                    }
                }
            },
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request_message = request.into_inner();

        let pred = { self.pred.recv.borrow().clone() };

        let hash = U256::from_be_bytes(request_message.key.try_into().unwrap());
        match pred {
            None => {
                let data_read = self.data.read().await;
                let result = match data_read.get(&hash) {
                    Some(v) => Some(get_response::Result::Val(v.clone())),
                    None => None,
                };
                Ok(Response::new(GetResponse {
                    result,
                    path_len: 0,
                }))
            }
            Some(p) => match Self::within_range(&hash, &p.hash, &self.id) {
                true => {
                    let data_write = self.data.read().await;
                    let result = match data_write.get(&hash) {
                        Some(v) => Some(get_response::Result::Val(v.clone())),
                        None => None,
                    };
                    Ok(Response::new(GetResponse {
                        result,
                        path_len: 0,
                    }))
                }
                false => {
                    let mut out = None;
                    for s in self.finger_table.iter().rev() {
                        if let Some(ss) = { s.recv.borrow().clone() } {
                            out = Some(ss.client);
                        }
                    }

                    match out {
                        Some(mut c) => {
                            let resp = c
                                .get(Request::new(GetRequest {
                                    key: hash.to_be_bytes().to_vec(),
                                }))
                                .await
                                .unwrap();
                            let d = resp.into_inner();
                            Ok(Response::new(GetResponse {
                                path_len: d.path_len + 1,
                                result: d.result,
                            }))
                        }
                        None => {
                            let data_write = self.data.read().await;
                            let result = match data_write.get(&hash) {
                                Some(v) => Some(get_response::Result::Val(v.clone())),
                                None => None,
                            };
                            Ok(Response::new(GetResponse {
                                result,
                                path_len: 0,
                            }))
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

        let pred = { self.pred.recv.borrow().clone() };
        let succ = { self.finger_table.get(0).unwrap().recv.borrow().clone() };

        match succ {
            Some(mut s) => match pred {
                None => Ok(Response::new(JoinResponse {
                    succ_ip: self.addr.clone(),
                    succ_id: self.id.clone().to_be_bytes().to_vec(),
                })),
                Some(p) => match Self::within_range(&hash, &p.hash, &self.id) {
                    true => Ok(Response::new(JoinResponse {
                        succ_ip: self.addr.clone(),
                        succ_id: self.id.clone().to_be_bytes().to_vec(),
                    })),
                    false => s.client.join(Request::new(request_message)).await,
                },
            },
            None => Ok(Response::new(JoinResponse {
                succ_ip: self.addr.clone(),
                succ_id: self.id.clone().to_be_bytes().to_vec(),
            })),
        }
    }

    async fn pred(&self, _request: Request<PredRequest>) -> Result<Response<PredResponse>, Status> {
        let pred = { self.pred.recv.borrow().clone() };

        match pred {
            None => Ok(Response::new(PredResponse {
                addr: self.addr.clone(),
                hash: self.id.clone().to_be_bytes().to_vec(),
            })),
            Some(p) => Ok(Response::new(PredResponse {
                addr: p.addr,
                hash: p.hash.to_be_bytes().to_vec(),
            })),
        }
    }

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let request_message = request.into_inner();
        let pred = { self.pred.recv.borrow().clone() };

        let hash = U256::from_be_bytes(request_message.hash.try_into().unwrap());

        match pred {
            None => {
                let mut data_write = self.data.write().await;
                let to_send = Self::shed_data(&mut data_write, &hash, &self.id);

                self.pred
                    .send
                    .send(predecessor::PredecessorData {
                        addr: request_message.addr,
                        hash,
                    })
                    .unwrap();

                Ok(Response::new(NotifyResponse {
                    has_data: Some(HasData::Data(Data {
                        keys: to_send.0,
                        vals: to_send.1,
                    })),
                }))
            }
            Some(p) => match Self::within_range(&hash, &p.hash, &self.id) {
                true => {
                    let mut data_write = self.data.write().await;
                    let to_send = Self::shed_data(&mut data_write, &hash, &self.id);

                    self.pred
                        .send
                        .send(predecessor::PredecessorData {
                            addr: request_message.addr,
                            hash,
                        })
                        .unwrap();

                    Ok(Response::new(NotifyResponse {
                        has_data: Some(HasData::Data(Data {
                            keys: to_send.0,
                            vals: to_send.1,
                        })),
                    }))
                }
                false => Ok(Response::new(NotifyResponse { has_data: None })),
            },
        }
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let len = { self.data.read().await.len() };

        let mut fingers = vec![];
        for finger in self.finger_table.iter() {
            if let Some(f) = { finger.recv.borrow().clone() } {
                if !fingers.contains(&f.addr) {
                    fingers.push(f.addr);
                }
            }
        }

        Ok(Response::new(GetStateResponse {
            len: len as u32,
            fingers,
        }))
    }

    async fn update_stab_freq(
        &self,
        request: Request<UpdateStabFreqRequest>,
    ) -> Result<Response<UpdateStabFreqResponse>, Status> {
        let request_data = request.into_inner();
        self.stab_freq_send.send(request_data.new).unwrap();
        Ok(Response::new(UpdateStabFreqResponse {}))
    }

    async fn update_fix_fing_freq(
        &self,
        request: Request<UpdateFixFingFreqRequest>,
    ) -> Result<Response<UpdateFixFingFreqResponse>, Status> {
        let request_data = request.into_inner();
        self.fix_fing_freq_send.send(request_data.new).unwrap();
        Ok(Response::new(UpdateFixFingFreqResponse {}))
    }
}

impl Node {
    pub fn new(
        addr: String,
        finger_table: Arc<Vec<Successor>>,
        pred: Arc<Predecessor>,
        data: Arc<RwLock<BTreeMap<U256, Quote>>>,
        stab_freq_send: watch::Sender<u64>,
        fix_fing_freq_send: watch::Sender<u64>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(addr.as_bytes());
        let id = U256::from_be_bytes(hasher.finalize().into());

        Self {
            pred,
            id,
            addr,
            data,
            stab_freq_send,
            fix_fing_freq_send,
            finger_table,
        }
    }

    pub fn start_periodics(
        &self,
        stabilize_freq: u64,
        fix_fingers_freq: u64,
        stab_freq_recv: watch::Receiver<u64>,
        fix_fing_freq_recv: watch::Receiver<u64>,
        join_set: &mut JoinSet<()>,
    ) {
        let stabilize_task_id = self.id.clone();
        let stabilize_task_addr = self.addr.clone();
        let stabilize_task_pred = self.pred.clone();
        let stabilize_task_data = self.data.clone();
        let stabilize_finger_table = self.finger_table.clone();

        join_set.spawn(async move {
            // stabilize
            let mut ticker = interval(Duration::from_millis(stabilize_freq));
            let mut stab_freq = stabilize_freq;
            loop {
                ticker.tick().await;
                let current_stab_freq = { *stab_freq_recv.borrow() };
                if current_stab_freq != stab_freq {
                    ticker = interval(Duration::from_millis(current_stab_freq));
                    stab_freq = current_stab_freq;
                }
                let pred_read = { stabilize_task_pred.recv.borrow().clone() };

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
                                        let mut to_append = BTreeMap::<U256, Quote>::from_iter(
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
                                        let mut to_append = BTreeMap::<U256, Quote>::from_iter(
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
                    None => {
                        if let Some(p) = pred_read {
                            let mut pred_client =
                                NodeServiceClient::connect(format!("http://{}", p.addr))
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
                                    let mut to_append = BTreeMap::<U256, Quote>::from_iter(
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
                                    addr: p.addr,
                                    hash: p.hash,
                                    client: pred_client,
                                })
                                .unwrap()
                        }
                    }
                }
            }
        });

        let fix_fingers_task_finger_table = self.finger_table.clone();
        let fix_fingers_hash = self.id.clone();
        let fix_fingers_addr = self.addr.clone();
        join_set.spawn(async move {
            // fix fingers
            let mut next: u32 = 1;
            let mut ticker = interval(Duration::from_millis(fix_fingers_freq));
            let mut fix_fing_freq = fix_fingers_freq;
            loop {
                ticker.tick().await;
                let current_fix_fing_freq = { *fix_fing_freq_recv.borrow() };
                if current_fix_fing_freq != fix_fing_freq {
                    ticker = interval(Duration::from_millis(current_fix_fing_freq));
                    fix_fing_freq = current_fix_fing_freq;
                }
                next += 1;
                if next > 255 {
                    next = 1;
                }

                let finger = fix_fingers_task_finger_table.get(next as usize).unwrap();
                let succesor = Self::find_succ(
                    fix_fingers_addr.clone(),
                    fix_fingers_hash.clone(),
                    &fix_fingers_task_finger_table,
                    &(fix_fingers_hash.wrapping_add(U256::new(2).wrapping_pow(next))),
                )
                .await;

                if succesor.0 == fix_fingers_addr {
                    continue;
                }
                if let Some(s) = { finger.recv.borrow().clone() } {
                    if s.addr == succesor.0 {
                        continue;
                    }
                }

                if let Ok(client) =
                    NodeServiceClient::connect(format!("http://{}", succesor.0)).await
                {
                    finger
                        .send
                        .send(successor::SuccessorData {
                            addr: succesor.0,
                            hash: succesor.1,
                            client,
                        })
                        .unwrap();
                }
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
        data: &mut BTreeMap<U256, Quote>,
        split_start: &U256,
        split_end: &U256,
    ) -> (Vec<Vec<u8>>, Vec<Quote>) {
        match split_start > split_end {
            true => {
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
            false => {
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

    let mut task_join_set = JoinSet::new();

    let stab_freq: u64 = args.get(1).unwrap().parse().unwrap();
    let fix_fing_freq: u64 = args.get(2).unwrap().parse().unwrap();
    let (stab_freq_send, stab_freq_recv) = watch::channel(stab_freq);
    let (fix_fing_freq_send, fix_fing_freq_recv) = watch::channel(fix_fing_freq);

    let first_succ = match args.get(3) {
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

    let pred = Predecessor::spawn(&mut task_join_set);

    let node = Node::new(
        addr_string,
        Arc::new(succs),
        Arc::new(pred),
        Arc::new(RwLock::new(BTreeMap::<U256, Quote>::new())),
        stab_freq_send,
        fix_fing_freq_send,
    );

    node.start_periodics(
        stab_freq,
        fix_fing_freq,
        stab_freq_recv,
        fix_fing_freq_recv,
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
