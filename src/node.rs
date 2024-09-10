use node::{
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    SetRequest, SetResponse,
};
use tokio::sync::mpsc::{self, Sender};

use std::collections::HashMap;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use sha2::{Digest, Sha256};

pub mod node {
    tonic::include_proto!("node");
}

#[derive(Debug)]
pub struct Node {
    id: String,
    sucessor: String,
    predecessor: String,
    sender: Sender<Command>,
}

pub enum Location {
    Local,
    Remote,
}
pub enum Command {
    Set {
        key: String,
        val: String,
        location: Location,
    },
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();
        let mut hasher = Sha256::new();

        Digest::update(&mut hasher, request_message.key.as_bytes());
        let hash = String::from_utf8(hasher.finalize().to_vec()).unwrap();
        let command = match hash <= self.id && hash >= self.predecessor {
            true => Command::Set {
                key: request_message.key,
                val: request_message.val,
                location: Location::Local,
            },
            false => Command::Set {
                key: request_message.key,
                val: request_message.val,
                location: Location::Remote,
            },
        };

        self.sender.send(command).await.unwrap();

        Ok(Response::new(SetResponse {}))
    }
}

impl Node {
    pub fn new(id: String, sender: Sender<Command>) -> Self {
        Self {
            sucessor: id.clone(),
            predecessor: id.clone(),
            id,
            sender,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, mut reciever) = mpsc::channel::<Command>(32);
    // TODO: figure out ports
    let addr = "[::1]:50051".parse()?;
    let node = Node::new("this is a string".into(), sender.clone());

    Server::builder()
        .add_service(NodeServiceServer::new(node))
        .serve(addr)
        .await?;

    let manager = tokio::spawn(async move {
        let mut node_client: NodeServiceClient<Channel> =
            NodeServiceClient::connect("http://[::1]:50051")
                .await
                .unwrap();
        let mut data = HashMap::<String, String>::new();

        while let Some(command) = reciever.recv().await {
            match command {
                Command::Set { key, val, location } => match location {
                    Location::Local => {
                        data.insert(key, val);
                    }
                    Location::Remote => {
                        let request = tonic::Request::new(SetRequest { key, val });
                        node_client.set(request).await.unwrap();
                    }
                },
            }
        }
    });

    manager.await.unwrap();

    Ok(())
}
