use std::{collections::BTreeMap, env, sync::Arc};

use itertools::Itertools;
use prost::Message;
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
    // notify_request::HasData,
    // notify_response, Data, GetRequest, GetResponse,
    GetStateRequest,
    GetStateResponse,
    JoinRequest,
    JoinResponse,
    NotifyRequest,
    NotifyResponse,
    PredRequest,
    PredResponse,
    SetRequest,
    SetResponse,
};

#[derive(Debug)]
pub struct Node {
    addr: String,
    id: Vec<u8>,
    // data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    // TODO: whenever theres a change to pred or succ, we need to do data splits
    pred: Arc<RwLock<Option<PredHandle>>>,
    succ: Arc<RwLock<Option<SuccHandle>>>,
}

#[derive(Debug)]
pub struct SuccHandle {
    addr: String,
    hash: Vec<u8>,
    client: NodeServiceClient<Channel>,
}
#[derive(Debug)]
pub struct PredHandle {
    addr: String,
    hash: Vec<u8>,
}

#[tonic::async_trait]
impl NodeService for Node {
    // async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
    //     let request_message = request.into_inner();
    //
    //     // TODO: probably want to do hashing on separate worker since we will
    //     // do it only once for everything
    //     let mut hasher = Sha256::new();
    //     hasher.update(request_message.key.as_bytes());
    //     let hash = hasher.finalize().to_vec();
    //
    //     let pred_read = self.pred.read().await;
    //     match pred_read.is_none()
    //         || Self::within_range(
    //             &hash,
    //             pred_read.as_ref().unwrap().hash.as_slice(),
    //             self.id.as_slice(),
    //         ) {
    //         true => {
    //             self.data.write().await.insert(hash, request_message.val);
    //             Ok(Response::new(SetResponse {
    //                 loc: self.addr.clone(),
    //             }))
    //         }
    //         // PERF: i kinda feel like we're holding on to the client for a while here
    //         false => {
    //             let mut succ_lock = self.succ.write().await;
    //             let succ_write = &mut succ_lock.as_mut().unwrap().client;
    //
    //             succ_write
    //                 .set(Request::new(SetRequest {
    //                     key: request_message.key,
    //                     val: request_message.val,
    //                 }))
    //                 .await
    //         }
    //     }
    // }

    // async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
    //     let request_message = request.into_inner();
    //
    //     let hash = match request_message.clone().getter.unwrap() {
    //         Getter::Key(key) => {
    //             let mut hasher = Sha256::new();
    //             hasher.update(key);
    //             hasher.finalize().to_vec()
    //         }
    //         Getter::Hash(hash) => hash,
    //     };
    //     let pred_read = self.pred.read().await;
    //     match pred_read.is_none()
    //         || Self::within_range(
    //             &hash,
    //             pred_read.as_ref().unwrap().hash.as_slice(),
    //             self.id.as_slice(),
    //         ) {
    //         true => {
    //             let reader = self.data.read().await;
    //             let val = reader.get(&hash).unwrap();
    //             Ok(Response::new(GetResponse {
    //                 val: val.into(),
    //                 loc: self.addr.clone(),
    //             }))
    //         }
    //         // PERF: i kinda feel like we're holding on to the client for a while here
    //         false => {
    //             let mut succ_lock = self.succ.write().await;
    //             let succ_write = &mut succ_lock.as_mut().unwrap().client;
    //             succ_write.get(Request::new(request_message)).await
    //         }
    //     }
    // }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request_message = request.into_inner();

        let mut hasher = Sha256::new();
        hasher.update(request_message.ip.as_bytes());
        let hash = hasher.finalize().to_vec();

        let pred_read = self.pred.read().await;
        let mut succ_write = self.succ.write().await;

        // TODO: this is so ugly but i needed it for my sanity
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

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let request_message = request.into_inner();
        let mut pred_write = self.pred.write().await;
        match pred_write.is_none() {
            true => {
                *pred_write = Some(PredHandle {
                    addr: request_message.addr,
                    hash: request_message.hash,
                });
            }
            false => match Self::within_range(
                &request_message.hash,
                &pred_write.as_ref().unwrap().hash,
                &self.id,
            ) {
                true => {
                    *pred_write = Some(PredHandle {
                        addr: request_message.addr,
                        hash: request_message.hash,
                    });
                }
                false => {}
            },
        }
        Ok(Response::new(NotifyResponse {}))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let succ_read = self.succ.read().await;
        let pred_read = self.pred.read().await;

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
        }))
    }
}

impl Node {
    pub fn new(
        addr: String,
        succ: Arc<RwLock<Option<SuccHandle>>>,
        // data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(addr.as_bytes());
        let id = hasher.finalize().to_vec();

        Self {
            pred: Arc::new(RwLock::new(None)),
            succ,
            id,
            addr,
            // data,
        }
    }

    pub fn start_periodics(&self, stabilize_interval: Duration) -> JoinHandle<()> {
        let succ = self.succ.clone();
        let id = self.id.clone();
        let addr = self.addr.clone();
        let pred = self.pred.clone();

        tokio::spawn(async move {
            // stabilize
            let mut interval = interval(stabilize_interval);
            loop {
                interval.tick().await;
                let mut succ_write = succ.write().await;
                match succ_write.is_none() {
                    true => {
                        let pred_read = pred.read().await;
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
                                pred_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: addr.clone(),
                                        hash: id.clone(),
                                    }))
                                    .await
                                    .unwrap();
                                *succ_write = Some(SuccHandle {
                                    addr: pred_handle.addr.clone(),
                                    hash: pred_handle.hash.clone(),
                                    client: pred_client,
                                });
                            }
                        }
                    }
                    false => {
                        let (pred_data, succ_hash) = {
                            let succ_handle = succ_write.as_mut().unwrap();
                            let succ_client = &mut succ_handle.client;
                            let pred_resp = succ_client
                                .pred(Request::new(PredRequest {}))
                                .await
                                .unwrap();
                            (pred_resp.into_inner(), succ_handle.hash.clone())
                        };
                        match Self::within_range(&pred_data.hash, &id, &succ_hash) {
                            true => {
                                let mut succ_client = NodeServiceClient::connect(format!(
                                    "http://{}",
                                    pred_data.addr
                                ))
                                .await
                                .unwrap();
                                succ_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: addr.clone(),
                                        hash: id.clone(),
                                    }))
                                    .await
                                    .unwrap();
                                *succ_write = Some(SuccHandle {
                                    addr: pred_data.addr.clone(),
                                    hash: pred_data.hash,
                                    client: succ_client,
                                })
                            }
                            false => {
                                let succ_client = &mut succ_write.as_mut().unwrap().client;
                                succ_client
                                    .notify(Request::new(NotifyRequest {
                                        addr: addr.clone(),
                                        hash: id.clone(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                }
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

    let node = Node::new(addr_string, succ.clone());
    // TODO: figure out timing
    let stabilize_handle = node.start_periodics(Duration::from_millis(500));

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
