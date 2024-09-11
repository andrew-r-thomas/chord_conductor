use std::{
    collections::{BTreeMap, VecDeque},
    env,
    sync::Arc,
};

use itertools::Itertools;
use sha2::{Digest, Sha256};
use tokio::{
    sync::RwLock,
    task::JoinHandle,
    time::{interval, Duration},
};
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
    notify_request::HasData,
    notify_response, Data, GetRequest, GetResponse, JoinRequest, JoinResponse, NotifyRequest,
    NotifyResponse, PredRequest, PredResponse, SetRequest, SetResponse,
};

#[derive(Debug)]
pub struct Node {
    addr: String,
    id: Vec<u8>,
    data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    // TODO: whenever theres a change to pred or succ, we need to do data splits
    pred: Arc<RwLock<(Vec<u8>, String)>>,
    succ: Arc<RwLock<VecDeque<(Vec<u8>, NodeServiceClient<Channel>)>>>,
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();

        // TODO: probably want to do hashing on separate worker since we will
        // do it only once for everything
        let mut hasher = Sha256::new();
        hasher.update(request_message.key.as_bytes());
        let hash = hasher.finalize().to_vec();

        match self.within_range(&hash).await || self.succ.read().await.is_empty() {
            true => {
                self.data.write().await.insert(hash, request_message.val);
                Ok(Response::new(SetResponse {
                    loc: self.addr.clone(),
                }))
            }
            // PERF: i kinda feel like we're holding on to the client for a while here
            false => {
                let mut succ_lock = self.succ.write().await;
                let first_succ = succ_lock.front_mut().unwrap();
                let succ_write = &mut first_succ.1;

                succ_write
                    .set(Request::new(SetRequest {
                        key: request_message.key,
                        val: request_message.val,
                    }))
                    .await
            }
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request_message = request.into_inner();

        let hash = match request_message.clone().getter.unwrap() {
            Getter::Key(key) => {
                let mut hasher = Sha256::new();
                hasher.update(key);
                hasher.finalize().to_vec()
            }
            Getter::Hash(hash) => hash,
        };
        match self.within_range(&hash).await || self.succ.read().await.is_empty() {
            true => {
                let reader = self.data.read().await;
                let val = reader.get(&hash).unwrap();
                Ok(Response::new(GetResponse {
                    val: val.into(),
                    loc: self.addr.clone(),
                }))
            }
            // PERF: i kinda feel like we're holding on to the client for a while here
            false => {
                let mut succ_lock = self.succ.write().await;
                let first_succ = succ_lock.front_mut().unwrap();
                let succ_write = &mut first_succ.1;

                succ_write.get(Request::new(request_message)).await
            }
        }
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request_message = request.into_inner();

        let mut hasher = Sha256::new();
        hasher.update(request_message.ip.as_bytes());
        let hash = hasher.finalize().to_vec();

        match self.within_range(&hash).await || self.succ.read().await.is_empty() {
            true => {
                let (keys, vals) = self.shed_data(&hash).await;
                *self.pred.write().await = (hash, request_message.ip);

                Ok(Response::new(JoinResponse {
                    succ_ip: self.addr.clone(),
                    succ_id: self.id.clone(),
                    keys,
                    vals,
                }))
            }
            false => {
                let mut succ_lock = self.succ.write().await;
                let first_succ = succ_lock.front_mut().unwrap();
                let succ_write = &mut first_succ.1;

                succ_write.join(Request::new(request_message)).await
            }
        }
    }

    async fn pred(&self, _request: Request<PredRequest>) -> Result<Response<PredResponse>, Status> {
        let pred_read = self.pred.read().await;
        Ok(Response::new(PredResponse {
            addr: pred_read.1.clone(),
            hash: pred_read.0.clone(),
        }))
    }

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let request_message = request.into_inner();
        let is_pred = {
            let pred_read = self.pred.read().await;
            pred_read.0.is_empty() || self.within_range(&request_message.hash).await
        };
        let response_message = match is_pred {
            true => {
                let mut pred_write = self.pred.write().await;
                let mut data_write = self.data.write().await;
                let out = match request_message.hash > self.id {
                    true => {
                        let first_chunk = data_write.split_off(key)
                        todo!()
                    }
                    false => {
                        let keeping = data_write.split_off(&request_message.hash);
                        let to_send = data_write.clone();
                        *data_write = keeping;

                        NotifyResponse {
                            has_data: Some(notify_response::HasData::Data(Data {
                                keys: to_send.clone().into_keys().collect_vec(),
                                vals: to_send.clone().into_values().collect_vec(),
                            })),
                        }
                    }
                };
                pred_write.0 = request_message.hash;
                pred_write.1 = request_message.addr;
                out
            }
            false => NotifyResponse { has_data: None },
        };

        Ok(Response::new(response_message))
    }
}

impl Node {
    pub fn new(
        addr: String,
        succ: Arc<RwLock<VecDeque<(Vec<u8>, NodeServiceClient<Channel>)>>>,
        data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(addr.as_bytes());
        let id = hasher.finalize().to_vec();

        Self {
            pred: Arc::new(RwLock::new((vec![], String::new()))),
            succ,
            id,
            addr,
            data,
        }
    }

    pub async fn within_range(&self, key: &[u8]) -> bool {
        // PERF: there is only one node that crosses this boundary,
        // and we will be doing this check a lot
        let p_read = self.pred.read().await;
        let p = p_read.0.as_slice();
        if p >= self.id.as_slice() {
            key > p || key <= self.id.as_slice()
        } else {
            key <= self.id.as_slice() && key > p
        }
    }

    pub async fn shed_data(&self, pred_hash: &[u8]) -> (Vec<Vec<u8>>, Vec<String>) {
        let mut data = self.data.write().await;
        let to_keep = data.split_off(pred_hash);
        let to_shed = data.clone();

        *data = to_keep;
        (
            to_shed.keys().cloned().collect_vec(),
            to_shed.values().cloned().collect_vec(),
        )
    }

    pub fn start_periodics(&self, stabilize_interval: Duration) -> JoinHandle<()> {
        let succ = self.succ.clone();
        let id = self.id.clone();
        let addr = self.addr.clone();
        let data = self.data.clone();
        let pred = self.pred.clone();

        tokio::spawn(async move {
            // stabilize
            let mut interval = interval(stabilize_interval);
            loop {
                interval.tick().await;
                let mut succ_lock = succ.write().await;
                if let Some(first_succ) = succ_lock.front_mut() {
                    let succ_write = &mut first_succ.1;
                    let pred = succ_write.pred(Request::new(PredRequest {})).await.unwrap();
                    let pred_data = pred.into_inner();

                    // check if its between this node and the successor
                    let in_range = {
                        let succ_hash = &first_succ.0;
                        if succ_hash < &id {
                            &pred_data.hash < succ_hash || &pred_data.hash > &id
                        } else {
                            &pred_data.hash > &id && &pred_data.hash < succ_hash
                        }
                    };

                    let notify_message = match in_range {
                        true => {
                            let new_succ =
                                NodeServiceClient::connect(format!("http://{}", pred_data.addr))
                                    .await
                                    .unwrap();

                            let mut data_write = data.write().await;
                            // FIXME: we need to fix splif off calls to handle
                            // crossing 0 boundary
                            let to_send = data_write.split_off(&pred_data.hash);
                            succ_lock.push_front((pred_data.hash, new_succ));

                            NotifyRequest {
                                addr: addr.clone(),
                                hash: id.clone(),
                                has_data: Some(HasData::Data(Data {
                                    keys: to_send.clone().into_keys().collect_vec(),
                                    vals: to_send.into_values().collect_vec(),
                                })),
                            }
                        }
                        false => NotifyRequest {
                            addr: addr.clone(),
                            hash: id.clone(),
                            has_data: None,
                        },
                    };

                    let current_succ = succ_lock.front_mut().unwrap();
                    let succ_client = &mut current_succ.1;
                    let notify_response = succ_client
                        .notify(Request::new(notify_message))
                        .await
                        .unwrap();
                    if let Some(has_data) = notify_response.into_inner().has_data {
                        match has_data {
                            notify_response::HasData::Data(new_data) => {
                                let mut data_write = data.write().await;
                                let mut new_data_map = BTreeMap::<Vec<u8>, String>::from_iter(
                                    new_data.keys.into_iter().zip(new_data.vals.into_iter()),
                                );
                                data_write.append(&mut new_data_map);
                            }
                        }
                    }
                } else {
                    let pred_read = pred.read().await;
                    if !pred_read.0.is_empty() {
                        let mut new_succ =
                            NodeServiceClient::connect(format!("http://{}", pred_read.1))
                                .await
                                .unwrap();

                        let mut data_write = data.write().await;
                        let first_chunk = data_write.split_off(&id);
                        let keeping = data_write.split_off(&pred_read.0);
                        let second_chunk = data_write.clone();
                        *data_write = keeping;

                        let notify_resp = new_succ
                            .notify(Request::new(NotifyRequest {
                                addr: addr.clone(),
                                hash: id.clone(),
                                has_data: Some(HasData::Data(Data {
                                    keys: first_chunk
                                        .clone()
                                        .into_keys()
                                        .chain(second_chunk.clone().into_keys())
                                        .collect_vec(),
                                    vals: first_chunk
                                        .clone()
                                        .into_values()
                                        .chain(second_chunk.clone().into_values())
                                        .collect_vec(),
                                })),
                            }))
                            .await
                            .unwrap();

                        if let Some(has_data) = notify_resp.into_inner().has_data {
                            match has_data {
                                notify_response::HasData::Data(Data { keys, vals }) => {
                                    let mut new_data = BTreeMap::<Vec<u8>, String>::from_iter(
                                        keys.into_iter().zip(vals.into_iter()),
                                    );
                                    data_write.append(&mut new_data);
                                }
                            }
                        }

                        succ_lock.push_front((pred_read.0.clone(), new_succ));
                    }
                }
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().collect_vec();
    let addr_string = args[1].clone();
    let addr = addr_string.parse()?;

    let (data, succ) = match args.get(2) {
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
            let data = Arc::new(RwLock::new(BTreeMap::<Vec<u8>, String>::from_iter(
                join_resp_data
                    .keys
                    .into_iter()
                    .zip(join_resp_data.vals.into_iter()),
            )));

            let succ = Arc::new(RwLock::new(
                VecDeque::<(Vec<u8>, NodeServiceClient<Channel>)>::from_iter(
                    &mut [(
                        join_resp_data.succ_id,
                        NodeServiceClient::connect(format!("http://{}", join_resp_data.succ_ip))
                            .await?,
                    )]
                    .into_iter(),
                ),
            ));
            (data, succ)
        }
        None => {
            let data = Arc::new(RwLock::new(BTreeMap::<Vec<u8>, String>::new()));
            let succ = Arc::new(RwLock::new(
                VecDeque::<(Vec<u8>, NodeServiceClient<Channel>)>::new(),
            ));
            (data, succ)
        }
    };

    let node = Node::new(addr_string, succ.clone(), data.clone());

    // TODO: figure out timing
    let stabilize_handle = node.start_periodics(Duration::from_millis(500));

    // TODO: also figure out the orders of these awaits
    // or if we should even do them
    Server::builder()
        .add_service(NodeServiceServer::new(node))
        .serve(addr)
        .await?;

    stabilize_handle.await?;

    Ok(())
}
