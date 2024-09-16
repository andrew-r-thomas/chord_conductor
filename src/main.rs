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
    io::AsyncReadExt,
    net::TcpListener,
    process::Command,
    sync::{mpsc, oneshot, RwLock},
    time::{interval, sleep, Duration},
};
use tonic::{transport::Channel, Request};

pub mod client_handler;
mod node {
    tonic::include_proto!("node");
}
use node::{node_service_client::NodeServiceClient, GetStateRequest};

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
    NodeData {
        addr: String,
        succ: Option<String>,
        data_len: u32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct NodeState {
    addr: String,
    hash: Vec<u8>,
    succ: String,
}
#[derive(Serialize, Deserialize, Debug)]
struct Nodes {
    kind: String,
    data: Vec<NodeState>,
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
    let (message_send, message_recv) = mpsc::channel::<WsSendMessage>(100);
    let (poll_send, poll_recv) = oneshot::channel::<Vec<ClientHandler>>();

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

                    let mut clients = vec![];
                    let mut prev_addr = None;
                    for _ in 0..nodes {
                        let node = ClientHandler::spawn(
                            prev_addr,
                            haikus.drain(0..haiku_num / nodes).collect_vec(),
                        )
                        .await;
                        prev_addr = Some(node.addr.clone());
                        clients.push(node);
                        sleep(Duration::from_millis(139)).await;
                    }
                    poll_send.send(clients).unwrap();
                }
            }
        }
    });

    /*
     * ok so we want to poll the clients periodically,
     * and then we want to make the nodes do stuff periodically with the clients
     */

    let send_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(500));
        let mut clients = poll_recv.await.unwrap();
        let mut nodes = vec![];

        loop {
            interval.tick().await;
            nodes.clear();
            let mut len = 0;
            for c in clients.iter_mut() {
                println!("trying to get data for: {}", c.addr);
                let get_state_resp = c
                    .client
                    .get_state(Request::new(GetStateRequest {}))
                    .await
                    .unwrap();
                println!("got data for: {}", c.addr);
                let get_state_data = get_state_resp.into_inner();
                nodes.push(NodeState {
                    addr: c.addr.clone(),
                    succ: get_state_data.succ,
                    hash: get_state_data.hash,
                });
                len += get_state_data.len;
            }
            nodes.sort_by(|a, b| a.hash.cmp(&b.hash));
            ws_send
                .send(Message::Text(
                    serde_json::to_string(&Nodes {
                        kind: "node data".into(),
                        data: nodes.clone(),
                        len,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
        }
    });

    recv_task.await.unwrap();
    send_task.await.unwrap();
}
