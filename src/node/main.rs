use std::{collections::BTreeMap, env, fs::File, sync::Arc};

use itertools::Itertools;
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::RwLock,
    task::JoinHandle,
    time::{interval, Duration},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

mod node_service {
    tonic::include_proto!("node");
}
use node_service::{
    get_request::Getter,
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    notify_response::HasData,
    set_request::Setter,
    Data, GetRequest, GetResponse, GetStateRequest, GetStateResponse, JoinRequest, JoinResponse,
    NotifyRequest, NotifyResponse, PredRequest, PredResponse, SetRequest, SetResponse,
};
use tracing::{info, instrument};

#[derive(Debug)]
pub struct Node {
    addr: String,
    id: Vec<u8>,
    data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    pred: Arc<RwLock<Option<PredHandle>>>,
    succ: Arc<RwLock<Option<SuccHandle>>>,
}

#[derive(Debug, Clone)]
pub struct SuccHandle {
    addr: String,
    hash: Vec<u8>,
    client: NodeServiceClient<Channel>,
}
#[derive(Debug, Clone)]
pub struct PredHandle {
    addr: String,
    hash: Vec<u8>,
}

// TODO: this pattern matching is gonna be real slow, might not be avoidable
// but if it is we should try to
#[tonic::async_trait]
impl NodeService for Node {
    #[instrument(skip(self))]
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        info!("called set");
        let request_message = request.into_inner();
        let hash = match request_message.setter {
            Some(Setter::Key(key)) => {
                let mut hasher = Sha256::new();
                hasher.update(key);
                hasher.finalize().to_vec()
            }
            Some(Setter::Hash(hash)) => hash,
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
                                        setter: Some(Setter::Hash(hash)),
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

        let hash = match request_message.getter {
            Some(Getter::Key(key)) => {
                let mut hasher = Sha256::new();
                hasher.update(key);
                hasher.finalize().to_vec()
            }
            Some(Getter::Hash(hash)) => hash,
            None => panic!(),
        };

        let pred_read = self.pred.read().await;
        let mut succ_write = self.succ.write().await;

        match succ_write.is_none() {
            true => {
                let data_write = self.data.write().await;
                let val = data_write.get(&hash).unwrap();
                Ok(Response::new(GetResponse {
                    val: val.clone(),
                    loc: self.addr.clone(),
                }))
            }
            false => match pred_read.is_none() {
                true => {
                    match Self::within_range(&hash, &succ_write.as_ref().unwrap().hash, &self.id) {
                        true => {
                            let data_write = self.data.write().await;
                            let val = data_write.get(&hash).unwrap();
                            Ok(Response::new(GetResponse {
                                val: val.clone(),
                                loc: self.addr.clone(),
                            }))
                        }
                        false => {
                            succ_write
                                .as_mut()
                                .unwrap()
                                .client
                                .get(Request::new(GetRequest {
                                    getter: Some(Getter::Hash(hash)),
                                }))
                                .await
                        }
                    }
                }
                false => {
                    match Self::within_range(&hash, &pred_read.as_ref().unwrap().hash, &self.id) {
                        true => {
                            let data_write = self.data.write().await;
                            let val = data_write.get(&hash).unwrap();
                            Ok(Response::new(GetResponse {
                                val: val.clone(),
                                loc: self.addr.clone(),
                            }))
                        }
                        false => {
                            succ_write
                                .as_mut()
                                .unwrap()
                                .client
                                .get(Request::new(GetRequest {
                                    getter: Some(Getter::Hash(hash)),
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
        let hash = hasher.finalize().to_vec();

        let pred_read = { self.pred.read().await.clone() };
        let mut succ_write = { self.succ.write().await.clone() };

        match succ_write.is_none() {
            true => Ok(Response::new(JoinResponse {
                succ_ip: self.addr.clone(),
                succ_id: self.id.clone(),
            })),
            false => match pred_read.is_none() {
                true => Ok(Response::new(JoinResponse {
                    succ_ip: self.addr.clone(),
                    succ_id: self.id.clone(),
                })),
                false => {
                    match Self::within_range(&hash, &pred_read.as_ref().unwrap().hash, &self.id) {
                        true => Ok(Response::new(JoinResponse {
                            succ_ip: self.addr.clone(),
                            succ_id: self.id.clone(),
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
                hash: self.id.clone(),
            })),
            false => {
                let pred = pred_read.as_ref().unwrap();
                Ok(Response::new(PredResponse {
                    addr: pred.addr.clone(),
                    hash: pred.hash.clone(),
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
        info!("got data write in notify");

        match pred_write.is_none() {
            true => {
                let mut data_write = self.data.write().await;
                let to_send =
                    Self::shed_data(&mut data_write, &request_message.hash, &self.id, true);
                *pred_write = Some(PredHandle {
                    addr: request_message.addr,
                    hash: request_message.hash,
                });
                Ok(Response::new(NotifyResponse {
                    has_data: Some(HasData::Data(Data {
                        keys: to_send.0,
                        vals: to_send.1,
                    })),
                }))
            }
            false => match Self::within_range(
                &request_message.hash,
                &pred_write.as_ref().unwrap().hash,
                &self.id,
            ) {
                true => {
                    let mut data_write = self.data.write().await;
                    let to_send =
                        Self::shed_data(&mut data_write, &request_message.hash, &self.id, true);
                    *pred_write = Some(PredHandle {
                        addr: request_message.addr,
                        hash: request_message.hash,
                    });
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

    #[instrument(skip(self))]
    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        info!("called get state");
        let succ_read = { self.succ.read().await.clone() };
        let pred_read = { self.pred.read().await.clone() };
        let len = { self.data.read().await.len() };

        // TODO: maybe just return None for empty vals
        let succ = {
            if succ_read.is_none() {
                "".to_string()
            } else {
                succ_read.as_ref().unwrap().addr.clone()
            }
        };

        let pred = {
            if pred_read.is_none() {
                "".to_string()
            } else {
                pred_read.as_ref().unwrap().addr.clone()
            }
        };

        Ok(Response::new(GetStateResponse {
            hash: self.id.clone(),
            succ,
            pred,
            len: len as u32,
        }))
    }
}

impl Node {
    pub fn new(
        addr: String,
        succ: Arc<RwLock<Option<SuccHandle>>>,
        data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(addr.as_bytes());
        let id = hasher.finalize().to_vec();

        Self {
            pred: Arc::new(RwLock::new(None)),
            succ,
            id,
            addr,
            data,
        }
    }

    #[instrument(skip(self))]
    pub fn start_periodics(&self, stabilize_interval: Duration) -> JoinHandle<()> {
        let succ = self.succ.clone();
        let id = self.id.clone();
        let addr = self.addr.clone();
        let pred = self.pred.clone();
        let data = self.data.clone();

        tokio::spawn(async move {
            // stabilize
            let mut interval = interval(stabilize_interval);
            loop {
                interval.tick().await;
                info!("stabilizing");
                let mut succ_write = succ.write().await;
                info!("got succ write in stabilizing");
                let pred_read = { pred.read().await.clone() };
                info!("got pred read in stabilizing");

                match succ_write.is_none() {
                    true => {
                        info!("stabilizing succ write is none");
                        match pred_read.is_none() {
                            true => {}
                            false => {
                                let pred_handle = pred_read.as_ref().unwrap();
                                let mut pred_client = NodeServiceClient::connect(format!(
                                    "http://{}",
                                    pred_handle.addr
                                ))
                                .await
                                .unwrap();
                                let notify_resp = pred_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: addr.clone(),
                                        hash: id.clone(),
                                    }))
                                    .await
                                    .unwrap();
                                let notify_data = notify_resp.into_inner();
                                match notify_data.has_data {
                                    Some(HasData::Data(Data { keys, vals })) => {
                                        let mut to_append = BTreeMap::<Vec<u8>, String>::from_iter(
                                            keys.into_iter().zip(vals.into_iter()),
                                        );
                                        let mut data_write = data.write().await;
                                        data_write.append(&mut to_append);
                                    }
                                    None => {}
                                }
                                *succ_write = Some(SuccHandle {
                                    addr: pred_handle.addr.clone(),
                                    hash: pred_handle.hash.clone(),
                                    client: pred_client,
                                });
                            }
                        }
                    }
                    false => {
                        info!("stabilizing succ write is some");
                        let (pred_data, succ_hash) = {
                            let succ_handle = succ_write.as_mut().unwrap();
                            let succ_client = &mut succ_handle.client;
                            let pred_resp = succ_client
                                .pred(Request::new(PredRequest {}))
                                .await
                                .unwrap();
                            (pred_resp.into_inner(), succ_handle.hash.clone())
                        };
                        info!("got pred in stabilize");
                        match Self::within_range(&pred_data.hash, &id, &succ_hash) {
                            true => {
                                info!("pred within range");
                                let mut succ_client = NodeServiceClient::connect(format!(
                                    "http://{}",
                                    pred_data.addr
                                ))
                                .await
                                .unwrap();
                                info!("connected to new succ in stabilize");

                                let notify_resp = succ_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: addr.clone(),
                                        hash: id.clone(),
                                    }))
                                    .await
                                    .unwrap();
                                info!("got notify resp in stabilize");
                                let notify_data = notify_resp.into_inner();
                                match notify_data.has_data {
                                    Some(HasData::Data(Data { keys, vals })) => {
                                        let mut to_append = BTreeMap::<Vec<u8>, String>::from_iter(
                                            keys.into_iter().zip(vals.into_iter()),
                                        );
                                        let mut data_write = data.write().await;
                                        data_write.append(&mut to_append);
                                    }
                                    None => {}
                                }
                                *succ_write = Some(SuccHandle {
                                    addr: pred_data.addr.clone(),
                                    hash: pred_data.hash,
                                    client: succ_client,
                                });
                            }
                            false => {
                                info!("pred not in range");
                                info!("calling notify on {}", succ_write.as_ref().unwrap().addr);
                                let succ_client = &mut succ_write.as_mut().unwrap().client;
                                let notify_resp = succ_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: addr.clone(),
                                        hash: id.clone(),
                                    }))
                                    .await
                                    .unwrap();
                                info!("got notify resp in stabilize");
                                let notify_data = notify_resp.into_inner();
                                match notify_data.has_data {
                                    Some(HasData::Data(Data { keys, vals })) => {
                                        let mut to_append = BTreeMap::<Vec<u8>, String>::from_iter(
                                            keys.into_iter().zip(vals.into_iter()),
                                        );
                                        let mut data_write = data.write().await;
                                        data_write.append(&mut to_append);
                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                }
                info!("finished stabilizing");
            }
        })
    }

    pub fn within_range(target: &[u8], start: &[u8], end: &[u8]) -> bool {
        match start > end {
            true => target > start || target < end,
            false => target > start && target < end,
        }
    }

    pub fn shed_data(
        data: &mut BTreeMap<Vec<u8>, String>,
        split_start: &[u8],
        split_end: &[u8],
        // TODO: might not need this is we're only using the function in notify
        keep_split: bool,
    ) -> (Vec<Vec<u8>>, Vec<String>) {
        match (keep_split, split_start > split_end) {
            (true, true) => {
                let mut first_keep = data.split_off(split_start);
                let to_send = data.split_off(split_end);

                data.append(&mut first_keep);

                (
                    to_send.clone().into_keys().collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
            (true, false) => {
                let mut to_send = data.split_off(split_end);
                let to_keep = data.split_off(split_start);

                to_send.append(data);
                *data = to_keep;

                (
                    to_send.clone().into_keys().collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
            (false, true) => {
                let mut to_send = data.split_off(split_start);
                let to_keep = data.split_off(split_end);

                to_send.append(data);
                *data = to_keep;

                (
                    to_send.clone().into_keys().collect_vec(),
                    to_send.clone().into_values().collect_vec(),
                )
            }
            (false, false) => {
                let mut to_keep = data.split_off(split_end);
                let to_send = data.split_off(split_start);
                data.append(&mut to_keep);

                (
                    to_send.clone().into_keys().collect_vec(),
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
    println!("{}", addr_string);

    let log_file = File::create(format!("{}_log.txt", addr_string)).unwrap();
    tracing_subscriber::fmt()
        .with_writer(log_file)
        .compact()
        .init();

    let succ = match args.get(1) {
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

            let succ = Arc::new(RwLock::new(Some(SuccHandle {
                addr: join_resp_data.succ_ip.clone(),
                hash: join_resp_data.succ_id,
                client: NodeServiceClient::connect(format!("http://{}", join_resp_data.succ_ip))
                    .await?,
            })));
            succ
        }
        None => {
            let succ = Arc::new(RwLock::new(None));
            succ
        }
    };

    let node = Node::new(
        addr_string,
        succ.clone(),
        Arc::new(RwLock::new(BTreeMap::<Vec<u8>, String>::new())),
    );
    // TODO: figure out timing
    let stabilize_handle = node.start_periodics(Duration::from_millis(100));

    // TODO: also figure out the orders of these awaits
    // or if we should even do them
    Server::builder()
        .add_service(NodeServiceServer::new(node))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .unwrap();

    stabilize_handle.await?;

    Ok(())
}
