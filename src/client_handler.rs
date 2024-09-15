use std::process::Stdio;

use tokio::{io::AsyncReadExt, process::Command};
use tonic::transport::Channel;

use crate::node::node_service_client::NodeServiceClient;

pub struct ClientHandler {
    pub addr: String,
    pub client: NodeServiceClient<Channel>,
}

impl ClientHandler {
    pub async fn spawn(join_addr: Option<String>) -> Self {
        let mut node = match join_addr {
            Some(ja) => Command::new("./target/debug/node")
                .args(&[ja])
                .stdout(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
            None => Command::new("./target/debug/node")
                .stdout(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
        };
        let mut stdout = node.stdout.take().unwrap();
        let mut bytes = [0; 13];
        stdout.read(&mut bytes).await.unwrap();
        let addr = String::from_utf8(bytes.to_vec()).unwrap();
        let client = NodeServiceClient::connect(format!("http://{}", addr.clone()))
            .await
            .unwrap();
        println!("connected to client: {}", addr.clone());
        let poll_task = tokio::spawn(async move {});
        let client_sim_task = tokio::spawn(async move {});
        Self { addr, client }
    }
}
