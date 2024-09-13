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
    notify_response, Data, GetRequest, GetResponse, GetStateRequest, GetStateResponse, JoinRequest,
    JoinResponse, MergeRequest, MergeResponse, NotifyRequest, NotifyResponse, PredRequest,
    PredResponse, SetRequest, SetResponse,
};

// NOTE: for the morning, the next steps are to fix the erros from adding the
// option, and then try to get a basic "add node" working in the client
// then make that guys site
#[derive(Debug)]
pub struct Node {
    addr: String,
    id: Vec<u8>,
    data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
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
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();

        // TODO: probably want to do hashing on separate worker since we will
        // do it only once for everything
        let mut hasher = Sha256::new();
        hasher.update(request_message.key.as_bytes());
        let hash = hasher.finalize().to_vec();

        let pred_read = self.pred.read().await;
        match pred_read.addr.is_empty()
            || Self::within_range(&hash, pred_read.hash.as_slice(), self.id.as_slice())
        {
            true => {
                self.data.write().await.insert(hash, request_message.val);
                Ok(Response::new(SetResponse {
                    loc: self.addr.clone(),
                }))
            }
            // PERF: i kinda feel like we're holding on to the client for a while here
            false => {
                let succ_write = &mut self.succ.write().await.client;

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
        let pred_read = self.pred.read().await;
        match pred_read.addr.is_empty()
            || Self::within_range(&hash, pred_read.hash.as_slice(), self.id.as_slice())
        {
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
                let succ_write = &mut self.succ.write().await.client;
                succ_write.get(Request::new(request_message)).await
            }
        }
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request_message = request.into_inner();

        let mut hasher = Sha256::new();
        hasher.update(request_message.ip.as_bytes());
        let hash = hasher.finalize().to_vec();

        let is_pred = {
            let pred_read = self.pred.read().await;
            pred_read.addr.is_empty()
                || Self::within_range(&hash, pred_read.hash.as_slice(), self.id.as_slice())
        };
        match is_pred {
            true => {
                let mut data_write = self.data.write().await;
                let mut pred_write = self.pred.write().await;
                let (keys, vals) = Self::shed_data(&mut data_write, &hash, &self.id, true);
                pred_write.hash = hash;
                pred_write.addr = request_message.ip;

                Ok(Response::new(JoinResponse {
                    succ_ip: self.addr.clone(),
                    succ_id: self.id.clone(),
                    keys,
                    vals,
                }))
            }
            false => {
                let succ_write = &mut self.succ.write().await.client;
                succ_write.join(Request::new(request_message)).await
            }
        }
    }

    async fn pred(&self, _request: Request<PredRequest>) -> Result<Response<PredResponse>, Status> {
        // TODO: need to make sure we're not sending empty preds here
        let pred_read = self.pred.read().await;
        Ok(Response::new(PredResponse {
            addr: pred_read.addr.clone(),
            hash: pred_read.hash.clone(),
        }))
    }

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let request_message = request.into_inner();
        let is_pred = {
            let pred_read = self.pred.read().await;
            pred_read.addr.is_empty()
                || Self::within_range(
                    request_message.hash.as_slice(),
                    pred_read.hash.as_slice(),
                    self.id.as_slice(),
                )
        };
        let response_message = match is_pred {
            true => {
                let mut pred_write = self.pred.write().await;
                let mut data_write = self.data.write().await;
                let out = match request_message.hash > self.id {
                    true => {
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
                pred_write.hash = request_message.hash;
                pred_write.addr = request_message.addr;
                out
            }
            false => NotifyResponse { has_data: None },
        };

        Ok(Response::new(response_message))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let succ_read = self.succ.read().await;
        let pred_read = self.pred.read().await;

        Ok(Response::new(GetStateResponse {
            succ: succ_read.addr.clone(),
            pred: pred_read.addr.clone(),
        }))
    }

    async fn merge(
        &self,
        request: Request<MergeRequest>,
    ) -> Result<Response<MergeResponse>, Status> {
        let request_message = request.into_inner();

        let join_data = {
            let mut temp_client =
                NodeServiceClient::connect(format!("http:/{}", request_message.join_addr))
                    .await
                    .unwrap();
            let join_resp = temp_client
                .join(Request::new(JoinRequest {
                    ip: self.addr.clone(),
                }))
                .await
                .unwrap();
            join_resp.into_inner()
        };

        let mut new_data = BTreeMap::<Vec<u8>, String>::from_iter(
            join_data.keys.into_iter().zip(join_data.vals.into_iter()),
        );
        {
            let mut data_write = self.data.write().await;
            data_write.append(&mut new_data);
        }

        let succ_client = NodeServiceClient::connect(format!("http://{}", join_data.succ_ip))
            .await
            .unwrap();
        {
            let mut succ_write = self.succ.write().await;
            succ_write.addr = join_data.succ_ip;
            succ_write.hash = join_data.succ_id;
            succ_write.client = succ_client;
        }

        Ok(Response::new(MergeResponse {}))
    }
}

impl Node {
    pub fn new(
        addr: String,
        succ: Arc<RwLock<SuccHandle>>,
        data: Arc<RwLock<BTreeMap<Vec<u8>, String>>>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(addr.as_bytes());
        let id = hasher.finalize().to_vec();

        Self {
            pred: Arc::new(RwLock::new(PredHandle {
                addr: String::new(),
                hash: vec![],
            })),
            succ,
            id,
            addr,
            data,
        }
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
                let mut succ_write = succ.write().await;
                if !succ_write.addr.is_empty() {
                    let succ_client = &mut succ_write.client;
                    let pred = succ_client
                        .pred(Request::new(PredRequest {}))
                        .await
                        .unwrap();
                    let pred_data = pred.into_inner();

                    // check if its between this node and the successor
                    let in_range = Node::within_range(
                        pred_data.hash.as_slice(),
                        succ_write.hash.as_slice(),
                        id.as_slice(),
                    );

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
                            *succ_write = SuccHandle {
                                addr: pred_data.addr,
                                hash: pred_data.hash,
                                client: new_succ,
                            };

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

                    let succ_client = &mut succ_write.client;
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
                    if !pred_read.addr.is_empty() {
                        let mut new_succ =
                            NodeServiceClient::connect(format!("http://{}", pred_read.addr))
                                .await
                                .unwrap();

                        let mut data_write = data.write().await;
                        let first_chunk = data_write.split_off(&id);
                        let keeping = data_write.split_off(&pred_read.hash);
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

                        *succ_write = SuccHandle {
                            addr: pred_read.addr.clone(),
                            hash: pred_read.hash.clone(),
                            client: new_succ,
                        };
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

            let succ = Arc::new(RwLock::new(SuccHandle {
                addr: join_resp_data.succ_ip.clone(),
                hash: join_resp_data.succ_id,
                client: NodeServiceClient::connect(format!("http://{}", join_resp_data.succ_ip))
                    .await?,
            }));
            (data, succ)
        }
        None => {
            let data = Arc::new(RwLock::new(BTreeMap::<Vec<u8>, String>::new()));
            let succ = Arc::new(RwLock::new(VecDeque::<SuccHandle>::new()));
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
