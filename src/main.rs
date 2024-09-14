use std::{
    process::Stdio,
    sync::{Arc, Mutex},
};

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
use futures::{channel::oneshot, stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    process::Command,
    sync::{
        mpsc::{self, UnboundedSender},
        RwLock,
    },
    task::JoinHandle,
    time::{interval, sleep, Duration},
};
use tonic::{transport::Channel, Request};

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
enum WsTextMessage {
    Start,
    AddNode,
    Other,
}

#[derive(Serialize, Deserialize, Debug)]
struct NodeState {
    addr: String,
    pred: String,
    succ: String,
}

struct Client {
    addr: String,
    client: NodeServiceClient<Channel>,
}

impl Client {
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
        Self { addr, client }
    }
}

async fn handle_socket(mut socket: WebSocket) {
    socket.send(Message::Ping(vec![])).await.unwrap();
    socket.recv().await.unwrap().unwrap();

    let (mut ws_send, mut ws_recv) = socket.split();

    // TODO: this rwlock is maybe not the right thing
    let clients = Arc::new(RwLock::new(vec![]));
    let recv_clients = clients.clone();
    let send_clients = clients.clone();
    let recv_task = tokio::spawn(async move {
        let mut started = false;

        while let Some(Ok(Message::Text(msg_text))) = ws_recv.next().await {
            let ws_text_message: WsTextMessage = serde_json::from_str(&msg_text).unwrap();
            match ws_text_message {
                WsTextMessage::Start => {
                    if started {
                        continue;
                    }

                    println!("start called");
                    started = true;

                    let client_handle = Client::spawn(None).await;
                    recv_clients.write().await.push(client_handle);
                }
                WsTextMessage::AddNode => {
                    if !started {
                        panic!();
                    }

                    let mut clients_lock = recv_clients.write().await;
                    let client_handle =
                        Client::spawn(Some(clients_lock.first().unwrap().addr.clone())).await;
                    clients_lock.push(client_handle);
                }
                _ => panic!(),
            }
        }
    });

    let send_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(500));
        let mut nodes = vec![];
        loop {
            interval.tick().await;
            let mut clients_lock = send_clients.write().await;
            nodes.clear();
            for c in clients_lock.iter_mut() {
                let get_state_resp = c
                    .client
                    .get_state(Request::new(GetStateRequest {}))
                    .await
                    .unwrap();
                let get_state_data = get_state_resp.into_inner();
                nodes.push(NodeState {
                    addr: c.addr.clone(),
                    pred: get_state_data.pred,
                    succ: get_state_data.succ,
                });
            }
            ws_send
                .send(Message::Text(serde_json::to_string(&nodes).unwrap()))
                .await
                .unwrap();
        }
    });

    recv_task.await.unwrap();
    send_task.await.unwrap();
}
