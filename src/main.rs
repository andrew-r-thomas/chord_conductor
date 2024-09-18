use std::collections::BTreeMap;

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
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    net::TcpListener,
    sync::{mpsc, oneshot},
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
    Start { nodes: usize },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WsSendMessage {
    NodeData(Vec<NodeState>),
    Haiku(String, u128),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeState {
    addr: String,
    fingers: Vec<String>,
    len: u32,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Haiku {
    key: String,
    val: String,
}

async fn handle_socket(mut socket: WebSocket) {
    socket.send(Message::Ping(vec![])).await.unwrap();
    socket.recv().await.unwrap().unwrap();

    let (mut ws_send, mut ws_recv) = socket.split();
    let (message_send, mut message_recv) = mpsc::channel::<WsSendMessage>(100);
    let (poll_send, poll_recv) = oneshot::channel::<BTreeMap<Vec<u8>, ClientHandler>>();

    let recv_message_send = message_send.clone();
    let recv_task = tokio::spawn(async move {
        if let Some(Ok(Message::Text(msg_text))) = ws_recv.next().await {
            let ws_text_message: WsRecvMessage = serde_json::from_str(&msg_text).unwrap();
            match ws_text_message {
                WsRecvMessage::Start { nodes } => {
                    let mut haikus = vec![];
                    let mut rdr = csv_async::AsyncReader::from_reader(
                        File::open("./haikus.csv").await.unwrap(),
                    );
                    let mut records = rdr.records();
                    while let Some(Ok(haiku)) = records.next().await {
                        haikus.push(Haiku {
                            val: haiku[0].into(),
                            key: haiku[1].into(),
                        });
                    }
                    let haiku_num = haikus.len();

                    let mut prev_addr = None;
                    let mut clients = BTreeMap::new();
                    for _ in 0..nodes {
                        let node = ClientHandler::spawn(
                            prev_addr,
                            haikus.drain(0..haiku_num / nodes).collect_vec(),
                            recv_message_send.clone(),
                        )
                        .await;
                        println!("added node {}", node.1.addr);
                        prev_addr = Some(node.1.addr.clone());
                        clients.insert(node.0, node.1);
                    }

                    poll_send.send(clients).unwrap();
                }
            }
        }
    });

    let poll_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(500));
        let mut clients = poll_recv.await.unwrap();

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

            message_send
                .send(WsSendMessage::NodeData(nodes.clone()))
                .await
                .unwrap();
        }
    });

    let send_task = tokio::spawn(async move {
        while let Some(message) = message_recv.recv().await {
            ws_send
                .send(Message::Text(serde_json::to_string(&message).unwrap()))
                .await
                .unwrap();
        }
    });

    poll_task.await.unwrap();
    recv_task.await.unwrap();
    send_task.await.unwrap();
}
