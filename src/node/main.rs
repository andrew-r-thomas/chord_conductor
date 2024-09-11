use data_manager::DataManager;
use sha2::{Digest, Sha256};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};

use std::sync::{Arc, RwLock};
use tonic::{transport::Server, Request, Response, Status};

mod node_service {
    tonic::include_proto!("node");
}
use node_service::{
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    JoinRequest, JoinResponse, SetRequest, SetResponse,
};
pub mod data_manager;

#[derive(Debug)]
pub struct Node {
    id: Vec<u8>,
    pred: Arc<RwLock<Vec<u8>>>,
    sender: Sender<Command>,
}

pub enum Command {
    Set {
        key: String,
        val: String,
        hash: Vec<u8>,
        local: bool,
        sender: oneshot::Sender<CommandResult>,
    },
    Join {
        pred: bool,
        hash: Vec<u8>,
        ip: String,
        sender: oneshot::Sender<CommandResult>,
    },
}

#[derive(Debug)]
pub enum CommandResult {
    Set,
    Join {
        ip: String,
        keys: Vec<Vec<u8>>,
        vals: Vec<String>,
    },
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();
        let mut hasher = Sha256::new();
        hasher.update(request_message.key.as_bytes());
        let hash = hasher.finalize().to_vec();

        let local = self.within_range(&hash);
        let (resp_send, resp_recv) = oneshot::channel::<CommandResult>();
        let command = Command::Set {
            key: request_message.key,
            val: request_message.val,
            hash,
            local,
            sender: resp_send,
        };

        self.sender.send(command).await.unwrap();
        let resp = resp_recv.await.unwrap();
        match resp {
            CommandResult::Set => Ok(Response::new(SetResponse {})),
            _ => todo!(),
        }
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request_message = request.into_inner();

        let mut hasher = Sha256::new();
        hasher.update(request_message.ip.as_bytes());
        let hash = hasher.finalize().to_vec();

        let (resp_send, resp_recv) = oneshot::channel::<CommandResult>();

        let command: Command;
        if self.within_range(&hash) {
            // TODO: probably move this to manager
            *self.pred.write().unwrap() = hash.clone();
            command = Command::Join {
                ip: request_message.ip,
                pred: true,
                sender: resp_send,
                hash,
            };
        } else {
            command = Command::Join {
                ip: request_message.ip,
                pred: false,
                sender: resp_send,
                hash,
            };
        }

        self.sender.send(command).await.unwrap();
        // TODO: better naming here
        let resp = resp_recv.await;
        match resp {
            Ok(r) => match r {
                CommandResult::Join { ip, keys, vals } => Ok(Response::new(JoinResponse {
                    succ_ip: ip,
                    keys,
                    vals,
                })),
                _ => todo!(),
            },
            // TODO: figure out if this is correct
            Err(e) => Err(Status::from_error(Box::new(e))),
        }
    }
}

impl Node {
    pub fn new(ip: String, sender: Sender<Command>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(ip.as_bytes());
        let id = hasher.finalize().to_vec();
        Self {
            sender,
            pred: Arc::new(RwLock::new(id.clone())),
            id,
        }
    }

    pub fn within_range(&self, key: &[u8]) -> bool {
        // PERF: there is only one node that crosses this boundary,
        // and we will be doing this check a lot
        let p = self.pred.read().unwrap();
        if p.as_slice() >= self.id.as_slice() {
            key > p.as_slice() || key <= self.id.as_slice()
        } else {
            key <= self.id.as_slice() && key > p.as_slice()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, reciever) = mpsc::channel::<Command>(32);
    // TODO: figure out ports
    let addr_string = "0.0.0.0:50051";
    let addr = addr_string.parse()?;
    let node = Node::new(addr_string.into(), sender.clone());

    let manager = tokio::spawn(async move {
        // TODO: get succ addr and initial data from cli probably
        let client = NodeServiceClient::connect(addr_string).await.unwrap();
        let mut data_manager = DataManager::new(addr_string.into(), reciever, client);
        data_manager.start().await;
    });

    Server::builder()
        .add_service(NodeServiceServer::new(node))
        .serve(addr)
        .await?;

    manager.await.unwrap();

    Ok(())
}
