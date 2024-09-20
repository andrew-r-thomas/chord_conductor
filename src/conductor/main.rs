use axum::{
    extract::{
        ws::{Message, WebSocket},
        WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    serve, Router,
};
use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};

pub mod sim;
mod node {
    tonic::include_proto!("node");
}
use sim::Sim;

#[tokio::main]
async fn main() {
    let app = Router::new().route("/ws", get(ws_handler));
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket))
}

#[derive(Serialize, Deserialize, Debug)]
enum WsRecvMessage {
    Start(Settings),
    Stop,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Settings {
    poll_rate: u64,
    nodes: usize,
    activity_level: u64,
    get_affinity: u64,
    stabilize_freq: u64,
    fix_finger_freq: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WsSendMessage {
    PollData {
        nodes: Vec<NodeState>,
        total_get_len: u32,
        total_set_len: u32,
        total_gets: u32,
        total_sets: u32,
        popular_quotes: Vec<JsonQuote>,
    },
    Ctrl(CtrlStatus),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CtrlStatus {
    Started,
    Stopped,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonQuote {
    quote: String,
    author: String,
    gets: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeState {
    addr: String,
    fingers: Vec<String>,
    len: u32,
}

async fn handle_socket(mut socket: WebSocket) {
    socket.send(Message::Ping(vec![])).await.unwrap();
    socket.recv().await.unwrap().unwrap();

    let (mut ws_send, mut ws_recv) = socket.split();
    let (message_send, mut message_recv) = mpsc::channel::<WsSendMessage>(100);

    let mut tasks = JoinSet::new();

    let t_message_send = message_send.clone();
    tasks.spawn(async move {
        let mut active_sim = None;
        while let Some(Ok(Message::Text(msg_text))) = ws_recv.next().await {
            let ws_text_message: WsRecvMessage = serde_json::from_str(&msg_text).unwrap();
            match ws_text_message {
                WsRecvMessage::Start(settings) => {
                    if active_sim.is_none() {
                        active_sim = Some(Sim::start(settings, t_message_send.clone()).await);
                        t_message_send
                            .send(WsSendMessage::Ctrl(CtrlStatus::Started))
                            .await
                            .unwrap();
                    }
                }
                WsRecvMessage::Stop => {
                    if let Some(a_s) = &active_sim {
                        a_s.stop().await;
                        active_sim = None;
                        t_message_send
                            .send(WsSendMessage::Ctrl(CtrlStatus::Stopped))
                            .await
                            .unwrap();
                    }
                }
            }
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
