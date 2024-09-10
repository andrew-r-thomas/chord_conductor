use std::{error::Error, process::Command};

pub mod node;
use bytes::Bytes;
use node::node::{node_service_client::NodeServiceClient, SetRequest};
use sha2::{Digest, Sha256};
use tonic::Request;

#[tokio::main]
async fn main() {
    let mut client = NodeServiceClient::connect("http://0.0.0.0:50051")
        .await
        .unwrap();

    let request = Request::new(SetRequest {
        key: "this is a test".into(),
        val: "these are some bytes".into(),
    });

    client.set(request).await.unwrap();
}
