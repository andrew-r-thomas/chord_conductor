use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    serve, Router,
};
use axum_extra::TypedHeader;
use client_handler::ClientHandler;
use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    net::TcpListener,
    sync::{
        mpsc,
        oneshot::{self, Sender},
        RwLock,
    },
    task::JoinSet,
    time::{interval, Duration},
};
use tonic::Request;

pub mod client_handler;
mod node {
    tonic::include_proto!("node");
}
use node::GetStateRequest;

#[tokio::main]
async fn main() {
    let app = Router::new().route("/ws", get(ws_handler));
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    println!("`{user_agent}` connected.");

    ws.on_upgrade(move |socket| handle_socket(socket))
}

#[derive(Serialize, Deserialize, Debug)]
enum WsRecvMessage {
    Start {
        poll_rate: u64,
        nodes: usize,
        activity_level: u32,
        get_affinity: u32,
        stabilize_freq: u32,
        fix_finger_freq: u32,
    },
    Stop,
    Pause,
    Play,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WsSendMessage {
    PollData {
        nodes: Vec<NodeState>,
        avg_get_path_len: u32,
        avg_set_path_len: u32,
        popular_quotes: Vec<JsonQuote>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonQuote {
    quote: String,
    author: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeState {
    addr: String,
    fingers: Vec<String>,
    len: u32,
}

pub struct PoolRequest {
    send: Sender<JsonQuote>,
}

pub struct TrackedQuote {
    quote: JsonQuote,
    hash: Vec<u8>,
    total_get_path_len: u32,
    set_path_len: u32,
    gets: u32,
}

async fn handle_socket(mut socket: WebSocket) {
    socket.send(Message::Ping(vec![])).await.unwrap();
    socket.recv().await.unwrap().unwrap();

    let (mut ws_send, mut ws_recv) = socket.split();
    let (message_send, mut message_recv) = mpsc::channel::<WsSendMessage>(100);
    let (poll_send, poll_recv) = oneshot::channel::<(BTreeMap<Vec<u8>, ClientHandler>, u64)>();
    let (pool_send, mut pool_recv) = mpsc::unbounded_channel::<PoolRequest>();

    let tracked_quotes = Arc::new(RwLock::new(Vec::<TrackedQuote>::new()));

    let mut tasks = JoinSet::new();

    let recv_quotes = tracked_quotes.clone();
    tasks.spawn(async move {
        if let Some(Ok(Message::Text(msg_text))) = ws_recv.next().await {
            let ws_text_message: WsRecvMessage = serde_json::from_str(&msg_text).unwrap();
            match ws_text_message {
                WsRecvMessage::Start {
                    poll_rate,
                    nodes,
                    activity_level,
                    get_affinity,
                    stabilize_freq,
                    fix_finger_freq,
                } => {
                    let mut prev_addr = None;
                    let mut clients = BTreeMap::new();
                    for _ in 0..nodes {
                        let node = ClientHandler::spawn(
                            prev_addr,
                            pool_send.clone(),
                            recv_quotes.clone(),
                            activity_level,
                            get_affinity,
                            stabilize_freq,
                            fix_finger_freq,
                        )
                        .await;
                        println!("added node {}", node.1.addr);
                        prev_addr = Some(node.1.addr.clone());
                        clients.insert(node.0, node.1);
                    }

                    poll_send.send((clients, poll_rate)).unwrap();
                }
            }
        }
    });

    tasks.spawn(async move {
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
                })
                .unwrap();
        }
    });

    let poll_quotes = tracked_quotes.clone();
    tasks.spawn(async move {
        let (mut clients, poll_rate) = poll_recv.await.unwrap();
        let mut interval = interval(Duration::from_millis(poll_rate));

        loop {
            interval.tick().await;
            let mut nodes = vec![];

            // PERF: could maybe do this concurrently
            for c in clients.values_mut() {
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

            // PERF: yikes
            let (popular_quotes, avg_get_path_len, avg_set_path_len) = {
                let mut popular_quotes = vec![];
                let mut avg_get_path_len = 0;
                let mut avg_set_path_len = 0;
                let mut highest_pop = 0;

                let quotes = poll_quotes.read().await;
                for quote in quotes.iter() {
                    if quote.gets > highest_pop {
                        popular_quotes.push(quote.quote.clone());
                        highest_pop = quote.gets;
                    }
                    if let Some(g) = quote.total_get_path_len.checked_div(quote.gets) {
                        avg_get_path_len += g;
                    }
                    avg_set_path_len += quote.set_path_len;
                }

                if let Some(avg_set) = avg_set_path_len.checked_div(quotes.len() as u32) {
                    avg_set_path_len = avg_set;
                }
                if let Some(avg_get) = avg_get_path_len.checked_div(quotes.len() as u32) {
                    avg_get_path_len = avg_get;
                }
                if let Some(p) = popular_quotes.last_chunk::<3>() {
                    popular_quotes = p.to_vec();
                }

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
    });

    tasks.spawn(async move {
        while let Some(message) = message_recv.recv().await {
            ws_send
                .send(Message::Text(serde_json::to_string(&message).unwrap()))
                .await
                .unwrap();
        }
    });

    tasks.join_all().await;
}
