use std::{process::Stdio, time::Duration};

use rand::seq::SliceRandom;
use tokio::{io::AsyncReadExt, process::Command, time::interval};
use tonic::{transport::Channel, Request};

use crate::{
    node::{node_service_client::NodeServiceClient, set_request::Setter},
    Haiku,
};

#[derive(Debug)]
pub struct ClientHandler {
    pub addr: String,
    pub client: NodeServiceClient<Channel>,
}

impl ClientHandler {
    pub async fn spawn(join_addr: Option<String>, data: Vec<Haiku>) -> Self {
        let mut node = match join_addr {
            Some(ja) => Command::new("./target/debug/node")
                .args(&[ja])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
            None => Command::new("./target/debug/node")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
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

        let mut task_client = client.clone();
        let task_addr = addr.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                if rand::random() {
                    let haiku = { data.choose(&mut rand::thread_rng()).unwrap() };
                    println!("trying to set on: {}", task_addr);
                    let thing = task_client
                        .set(Request::new(crate::node::SetRequest {
                            val: haiku.val.clone(),
                            setter: Some(Setter::Key(haiku.key.clone())),
                        }))
                        .await;
                    println!("got data on: {}, data: {:?}", task_addr, thing);
                }
            }
        });

        Self { addr, client }
    }
}
