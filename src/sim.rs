use std::{collections::BTreeMap, process::Stdio, sync::Arc};

use futures::channel::oneshot;
use itertools::Itertools;
use rand::{distributions::WeightedIndex, prelude::Distribution, seq::SliceRandom};
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    process::Command,
    sync::{
        mpsc::{self, UnboundedReceiver},
        watch, RwLock,
    },
    time::{interval, sleep, Duration},
};
use tokio_stream::StreamExt;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{transport::Channel, Request};

use crate::{
    node::{node_service_client::NodeServiceClient, GetStateRequest, Quote},
    JsonQuote, NodeState, Settings, WsSendMessage,
};

pub struct Sim {
    cancel_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Sim {
    pub async fn start(settings: Settings, message_send: mpsc::Sender<WsSendMessage>) -> Self {
        let cancel_token = CancellationToken::new();

        let (pool_send, pool_recv) = mpsc::unbounded_channel::<PoolRequest>();
        let pool_task_token = cancel_token.clone();
        let mut task_tracker = TaskTracker::new();
        task_tracker.spawn(async move {
            tokio::select! {
                _ = pool_task_token.cancelled() => {
                }
            _ = Self::data_pool_task(pool_recv) => {}
            }
        });

        let tracked_quotes = Arc::new(RwLock::new(vec![]));

        let (gets_send, gets_recv) = watch::channel((0, 0));
        let (sets_send, sets_recv) = watch::channel((0, 0));

        let mut prev_addr = None;
        let nodes = Arc::new(RwLock::new(BTreeMap::new()));
        {
            let mut ns = nodes.write().await;
            for _ in 0..settings.nodes {
                let node = Self::spawn_node(
                    prev_addr,
                    pool_send.clone(),
                    tracked_quotes.clone(),
                    settings.activity_level,
                    settings.get_affinity,
                    settings.stabilize_freq,
                    settings.fix_finger_freq,
                    &mut task_tracker,
                    cancel_token.clone(),
                    gets_send.clone(),
                    sets_send.clone(),
                )
                .await;

                prev_addr = Some(node.1.addr.clone());
                ns.insert(node.0, node.1);
            }
        }

        let poll_task_token = cancel_token.clone();
        let poll_task_nodes = nodes.clone();
        let poll_task_tracked_quotes = tracked_quotes.clone();
        task_tracker.spawn(async move {
            tokio::select! {
                _ = poll_task_token.cancelled() => {
                }
                _ = Self::poll_task(
                    poll_task_nodes,
                    poll_task_tracked_quotes,
                    settings.poll_rate,
                    message_send,
                    gets_recv.clone(),
                    sets_recv.clone()
                ) => {}
            }
        });

        task_tracker.close();

        Self {
            cancel_token,
            task_tracker,
        }
    }

    pub async fn stop(&self) {
        self.cancel_token.cancel();
        self.task_tracker.wait().await;
    }

    async fn data_pool_task(mut pool_recv: UnboundedReceiver<PoolRequest>) {
        let mut rdr =
            csv_async::AsyncReader::from_reader(File::open("./quotes.csv").await.unwrap());
        let mut records = rdr.records();
        while let Some(request) = pool_recv.recv().await {
            let record = records.next().await.unwrap().unwrap();
            request
                .send
                .send(JsonQuote {
                    quote: record[0].into(),
                    author: record[1].into(),
                    gets: 0,
                })
                .unwrap();
        }
    }

    async fn poll_task(
        clients: Arc<RwLock<BTreeMap<Vec<u8>, NodeHandle>>>,
        quotes: Arc<RwLock<Vec<(JsonQuote, Vec<u8>)>>>,
        poll_rate: u64,
        message_send: mpsc::Sender<WsSendMessage>,
        gets: watch::Receiver<(u32, u32)>,
        sets: watch::Receiver<(u32, u32)>,
    ) {
        let mut ticker = interval(Duration::from_millis(poll_rate));
        loop {
            ticker.tick().await;

            let clients_read = { clients.read().await.clone() };
            let mut nodes = vec![];
            for mut c in clients_read.iter().map(|c| c.1.clone()) {
                let get_state_resp = c
                    .client
                    .get_state(Request::new(GetStateRequest {}))
                    .await
                    .unwrap();
                let get_state_data = get_state_resp.into_inner();
                nodes.push(NodeState {
                    addr: c.addr.clone(),
                    fingers: get_state_data.fingers,
                    len: get_state_data.len,
                });
            }

            let (popular_quotes, avg_get_path_len, avg_set_path_len) = {
                let mut popular_quotes = vec![];
                {
                    let quotes = quotes.read().await;
                    for (quote, _) in quotes
                        .iter()
                        .sorted_by(|a, b| b.0.gets.cmp(&a.0.gets))
                        .zip(0..3)
                    {
                        popular_quotes.push(quote.0.clone());
                    }

                    if let Some(p) = popular_quotes.last_chunk::<3>() {
                        popular_quotes = p.to_vec();
                    }
                }

                let avg_get_path_len = {
                    let gets_data = gets.borrow();
                    gets_data.0 as f32 / gets_data.1 as f32
                };
                let avg_set_path_len = {
                    let sets_data = sets.borrow();
                    sets_data.0 as f32 / sets_data.1 as f32
                };

                (popular_quotes, avg_get_path_len, avg_set_path_len)
            };

            message_send
                .send(WsSendMessage::PollData {
                    nodes,
                    popular_quotes,
                    avg_get_path_len,
                    avg_set_path_len,
                })
                .await
                .unwrap();
        }
    }

    async fn spawn_node(
        join_addr: Option<String>,
        pool_sender: mpsc::UnboundedSender<PoolRequest>,
        set_quotes: Arc<RwLock<Vec<(JsonQuote, Vec<u8>)>>>,
        activity_level: u64,
        get_affinity: u64,
        stabilize_freq: u64,
        fix_fingers_freq: u64,
        task_tracker: &mut TaskTracker,
        cancel_token: CancellationToken,
        gets_send: watch::Sender<(u32, u32)>,
        sets_send: watch::Sender<(u32, u32)>,
    ) -> (Vec<u8>, NodeHandle) {
        let mut node = match join_addr {
            Some(ja) => Command::new("./target/debug/node")
                .args(&[stabilize_freq.to_string(), fix_fingers_freq.to_string(), ja])
                .kill_on_drop(true)
                .stdout(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
            None => Command::new("./target/debug/node")
                .args(&[stabilize_freq.to_string(), fix_fingers_freq.to_string()])
                .kill_on_drop(true)
                .stdout(Stdio::piped())
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
        let client_task = async move {
            // TODO: figure out a better way to do this
            let _kill_guard = node;
            let mut ticker = interval(Duration::from_millis(activity_level.into()));
            let weights = [get_affinity, 100 - get_affinity];
            let index = WeightedIndex::new(weights).unwrap();
            ticker.tick().await;
            loop {
                ticker.tick().await;
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
                                    key: quote.1.clone(),
                                }))
                                .await
                                .unwrap();

                            let get_data = get_resp.into_inner();
                            if let Some(_) = get_data.result {
                                quote.0.gets += 1;
                                gets_send.send_modify(|(total_path_len, total_gets)| {
                                    *total_path_len += get_data.path_len;
                                    *total_gets += 1;
                                });
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
                            set_quotes.write().await.push((quote, set_data.hash));
                            sets_send.send_modify(|(total_path_len, total_sets)| {
                                *total_path_len += set_data.path_len;
                                *total_sets += 1;
                            });
                        }
                    }
                    _ => {}
                }
            }
        };

        task_tracker.spawn(async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {}
                _ = client_task => {}
            }
        });

        (hash, NodeHandle { addr, client })
    }
}

#[derive(Clone)]
struct NodeHandle {
    pub addr: String,
    pub client: NodeServiceClient<Channel>,
}

struct PoolRequest {
    send: oneshot::Sender<JsonQuote>,
}
