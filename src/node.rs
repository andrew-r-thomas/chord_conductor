use bytes::Bytes;
use node::{
    node_service_client::NodeServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    SetRequest, SetResponse,
};
use tokio::sync::mpsc::{self, Sender};

use std::collections::HashMap;
use tonic::{transport::Server, Request, Response, Status};

pub mod node {
    tonic::include_proto!("node");
}

#[derive(Debug)]
pub struct Node {
    id: Bytes,
    sender: Sender<Command>,
}

pub enum Command {
    Set { key: String, val: String },
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request_message = request.into_inner();
        let command = Command::Set {
            key: request_message.key,
            val: request_message.val,
        };
        self.sender.send(command).await.unwrap();

        Ok(Response::new(SetResponse {}))
    }
}

impl Node {
    pub fn new(id: Bytes, sender: Sender<Command>) -> Self {
        Self { id, sender }
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
                Command::Set { key, val } => {
                    data.insert(key.clone(), val.clone());
                    println!("inserted: key: {}, val: {}", key, val);
                }
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
