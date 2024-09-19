use std::{process::Stdio, sync::Arc};

use rand::{distributions::WeightedIndex, prelude::Distribution, seq::SliceRandom};
use sha2::{Digest, Sha256};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::{mpsc::UnboundedSender, oneshot, RwLock},
    time::{interval, sleep, Duration},
};
use tonic::{transport::Channel, Request};

use crate::{
    node::{node_service_client::NodeServiceClient, Quote},
    JsonQuote, PoolRequest, TrackedQuote,
};

#[derive(Debug)]
pub struct ClientHandler {
    pub addr: String,
    pub client: NodeServiceClient<Channel>,
}

impl ClientHandler {
    pub async fn spawn(
        join_addr: Option<String>,
        pool_sender: UnboundedSender<PoolRequest>,
        set_quotes: Arc<RwLock<Vec<TrackedQuote>>>,
        activity_level: u32,
        get_affinity: u32,
        stabilize_freq: u32,
        fix_fingers_freq: u32,
    ) -> (Vec<u8>, Self) {
        let mut node = match join_addr {
            Some(ja) => Command::new("./target/debug/node")
                .args(&[stabilize_freq.to_string(), fix_fingers_freq.to_string(), ja])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
            None => Command::new("./target/debug/node")
                .args(&[stabilize_freq.to_string(), fix_fingers_freq.to_string()])
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
            let mut interval = interval(Duration::from_millis(activity_level.into()));
            let weights = [get_affinity, 100 - get_affinity];
            let index = WeightedIndex::new(weights).unwrap();
            interval.tick().await;
            loop {
                interval.tick().await;
                // TODO: also change this to only get values it knows is in there, or something

                let i = { index.sample(&mut rand::thread_rng()) };
                match i {
                    0 => {
                        // get
                        let mut quote_write = set_quotes.write().await;
                        let q = { quote_write.choose_mut(&mut rand::thread_rng()) };
                        if let Some(quote) = q {
                            let get_resp = task_client
                                .get(Request::new(crate::node::GetRequest {
                                    key: quote.hash.clone(),
                                }))
                                .await
                                .unwrap();

                            let get_data = get_resp.into_inner();
                            if let Some(_) = get_data.result {
                                quote.gets += 1;
                                quote.total_get_path_len += get_data.path_len;
                            }
                        }
                    }
                    1 => {
                        let (q_send, q_recv) = oneshot::channel::<JsonQuote>();
                        pool_sender.send(PoolRequest { send: q_send }).unwrap();
                        if let Ok(quote) = q_recv.await {
                            let set_resp = task_client
                                .set(Request::new(crate::node::SetRequest {
                                    val: Some(Quote {
                                        quote: quote.quote.clone(),
                                        author: quote.author.clone(),
                                    }),
                                    setter: None,
                                }))
                                .await
                                .unwrap();

                            let set_data = set_resp.into_inner();
                            set_quotes.write().await.push(TrackedQuote {
                                quote,
                                hash: set_data.hash,
                                gets: 0,
                                total_get_path_len: 0,
                                // TODO: this number wont change as is
                                set_path_len: set_data.path_len,
                            });
                        }
                    }
                    _ => {}
                }
            }
        });

        (hash, Self { addr, client })
    }
}
