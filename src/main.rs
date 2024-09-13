use core::panic;

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
use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    process::Command,
    sync::mpsc::{self, UnboundedSender},
    task::JoinHandle,
    time::{interval, sleep, Duration},
};
use tonic::Request;

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
    join_handle: JoinHandle<()>,
}

impl Client {
    pub fn spawn(addr: String, join_addr: Option<String>, send: UnboundedSender<Message>) -> Self {
        println!("this is the addr: {}", addr);
        Self {
            addr: addr.clone(),
            join_handle: tokio::spawn(async move {
                let _node = match join_addr {
                    Some(ja) => Command::new("./target/debug/node")
                        .kill_on_drop(true)
                        .args(&[addr.clone(), ja])
                        .spawn()
                        .expect("failed to start node"),
                    None => Command::new("./target/debug/node")
                        .kill_on_drop(true)
                        .args(&[addr.clone()])
                        .spawn()
                        .expect("failed to start node"),
                };
                // TODO: maybe a mechanism for knowing when a node is ready
                sleep(Duration::from_millis(2000)).await;
                let mut client = NodeServiceClient::connect(format!("http://{}", addr.clone()))
                    .await
                    .unwrap();

                let mut interval = interval(Duration::from_millis(500));
                loop {
                    interval.tick().await;

                    let get_state_resp = client
                        .get_state(Request::new(GetStateRequest {}))
                        .await
                        .unwrap();
                    let get_state_message = get_state_resp.into_inner();
                    send.send(Message::Text(
                        serde_json::to_string(&NodeState {
                            addr: addr.clone(),
                            pred: get_state_message.pred,
                            succ: get_state_message.succ,
                        })
                        .unwrap(),
                    ))
                    .unwrap();
                }
            }),
        }
    }
}

async fn handle_socket(mut socket: WebSocket) {
    socket.send(Message::Ping(vec![])).await.unwrap();
    socket.recv().await.unwrap().unwrap();

    let (mut ws_send, mut ws_recv) = socket.split();
    let (inner_send, mut inner_recv) = mpsc::unbounded_channel::<Message>();

    let recv_task = tokio::spawn(async move {
        let mut started = false;

        let mut addr_inc = 51;
        let node_addr = "0.0.0.0:500";
        let mut clients = vec![];

        while let Some(Ok(Message::Text(msg_text))) = ws_recv.next().await {
            let ws_text_message: WsTextMessage = serde_json::from_str(&msg_text).unwrap();
            match ws_text_message {
                WsTextMessage::Start => {
                    if started {
                        continue;
                    }

                    println!("start called");
                    started = true;

                    let addr = format!("{}{}", node_addr, addr_inc);
                    addr_inc += 1;

                    let client_handle = Client::spawn(addr, None, inner_send.clone());
                    clients.push(client_handle);
                }
                WsTextMessage::AddNode => {
                    if !started {
                        panic!();
                    }

                    let addr = format!("{}{}", node_addr, addr_inc);
                    addr_inc += 1;

                    let join_addr = &clients.first().unwrap().addr;
                    let client_handle =
                        Client::spawn(addr, Some(join_addr.into()), inner_send.clone());
                    clients.push(client_handle);
                }
                _ => panic!(),
            }
        }
    });

    let send_task = tokio::spawn(async move {
        while let Some(message) = inner_recv.recv().await {
            ws_send.send(message).await.unwrap();
        }
    });

    recv_task.await.unwrap();
    send_task.await.unwrap();
}
