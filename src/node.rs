use bytes::Bytes;
use node::{
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    SetRequest, SetResponse,
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::{self, Sender};

use core::panic;
use std::collections::HashMap;
use tonic::{transport::Server, Request, Response, Status};

pub mod node {
    tonic::include_proto!("node");
}

// TODO: use this to determine ordering of hashes
const BYTES_MAX: [u8; 256] = [u8::MAX; 256];

#[derive(Debug)]
pub struct Node {
    id: Bytes,
    succ: Bytes,
    pred: Bytes,
    sender: Sender<Command>,
}

pub enum Command {
    Set {
        key: String,
        val: String,
        local: bool,
    },
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();
        let mut hasher = Sha256::new();
        hasher.update(request_message.key.as_bytes());
        let hash = Bytes::from(hasher.finalize().to_vec());

        let command = Command::Set {
            key: request_message.key,
            val: request_message.val,
            local: self.within_range(hash),
        };

        self.sender.send(command).await.unwrap();

        Ok(Response::new(SetResponse {}))
    }
}

impl Node {
    pub fn new(id: Bytes, sender: Sender<Command>) -> Self {
        Self {
            sender,
            succ: id.clone(),
            pred: id.clone(),
            id,
        }
    }

    pub fn within_range(&self, key: Bytes) -> bool {
        // PERF: there is only one node that crosses this boundary,
        // and we will be doing this check a lot
        if self.pred >= self.id {
            key > self.pred || key <= self.id
        } else {
            key <= self.id && key > self.pred
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, mut reciever) = mpsc::channel::<Command>(32);
    // TODO: figure out ports
    let addr = "0.0.0.0:50051".parse()?;
    let node = Node::new("this is a string".into(), sender.clone());

    let manager = tokio::spawn(async move {
        let mut data = HashMap::<String, String>::new();

        while let Some(command) = reciever.recv().await {
            match command {
                Command::Set { key, val, local } => match local {
                    true => {
                        data.insert(key.clone(), val.clone());
                        println!("inserted: key: {}, val: {}, on node: {}", key, val, addr);
                    }
                    false => {
                        panic!()
                    }
                },
            }
        }
    });

    Server::builder()
        .add_service(NodeServiceServer::new(node))
        .serve(addr)
        .await?;

    manager.await.unwrap();

    Ok(())
}
