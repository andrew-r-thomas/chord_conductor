use std::{process::Stdio, time::Duration};

use rand::seq::SliceRandom;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    process::Command,
    time::{interval, sleep},
};
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
        let addr = {
            let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        let mut _node = match join_addr {
            Some(ja) => Command::new("./target/debug/node")
                .args(&[addr.to_string(), ja])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
            None => Command::new("./target/debug/node")
                .args(&[addr.to_string()])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
        };
        sleep(Duration::from_millis(2000)).await;

        let client = NodeServiceClient::connect(format!("http://{}", addr.clone()))
            .await
            .unwrap();

        let mut task_client = client.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;
                let haiku = { data.choose(&mut rand::thread_rng()).unwrap() };
                let thing = task_client
                    .set(Request::new(crate::node::SetRequest {
                        val: haiku.val.clone(),
                        setter: Some(Setter::Key(haiku.key.clone())),
                    }))
                    .await;
            }
        });

        Self {
            addr: addr.to_string(),
            client,
        }
    }
}
