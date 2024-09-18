use std::process::Stdio;

use rand::seq::SliceRandom;
use sha2::{Digest, Sha256};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::mpsc::Sender,
    time::{interval, sleep, Duration, Instant},
};
use tonic::{transport::Channel, Request};

use crate::{
    node::{
        get_response::Result::Val, node_service_client::NodeServiceClient, set_request::Setter,
    },
    Haiku, WsSendMessage,
};

#[derive(Debug)]
pub struct ClientHandler {
    pub addr: String,
    pub client: NodeServiceClient<Channel>,
}

impl ClientHandler {
    pub async fn spawn(
        join_addr: Option<String>,
        data: Vec<Haiku>,
        sender: Sender<WsSendMessage>,
    ) -> (Vec<u8>, Self) {
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
        let mut hasher = Sha256::new();
        hasher.update(addr.clone());
        let hash = hasher.finalize().to_vec();

        sleep(Duration::from_millis(100)).await;
        let client = NodeServiceClient::connect(format!("http://{}", addr.clone()))
            .await
            .unwrap();

        let mut task_client = client.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                // PERF: could maybe do these concurrently
                // TODO: also change this to only get values it knows is in there, or something
                if rand::random() {
                    let haiku = { data.choose(&mut rand::thread_rng()).unwrap() };
                    let get_start = Instant::now();
                    let get_resp = task_client
                        .get(Request::new(crate::node::GetRequest {
                            getter: Some(crate::node::get_request::Getter::Key(haiku.key.clone())),
                        }))
                        .await
                        .unwrap();
                    let get_latency: Duration = Instant::now() - get_start;

                    let get_data = get_resp.into_inner();
                    if let Some(val) = get_data.result {
                        match val {
                            Val(v) => {
                                sender
                                    .send(WsSendMessage::Haiku(v, get_latency.as_millis()))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                }
                if rand::random() {
                    let haiku = { data.choose(&mut rand::thread_rng()).unwrap() };
                    task_client
                        .set(Request::new(crate::node::SetRequest {
                            val: haiku.val.clone(),
                            setter: Some(Setter::Key(haiku.key.clone())),
                        }))
                        .await
                        .unwrap();
                }
            }
        });

        (hash, Self { addr, client })
    }
}
