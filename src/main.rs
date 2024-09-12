use core::panic;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    serve, Router, ServiceExt,
};
use axum_extra::TypedHeader;
use futures::{
    channel::oneshot,
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    process::Command,
    task::JoinSet,
    time::{self, interval, sleep, Duration, Instant, Interval},
};
use tonic::{transport::Channel, Request};

mod node {
    tonic::include_proto!("node");
}
use node::{
    get_request::Getter, node_service_client::NodeServiceClient, GetRequest, GetStateRequest,
    SetRequest,
};

#[tokio::main]
async fn main() {
    // let _node_child_process_1 = Command::new("./target/debug/node")
    //     .kill_on_drop(true)
    //     .args(&["0.0.0.0:50051"])
    //     .spawn()
    //     .expect("failed to start node");
    // // TODO: maybe a mechanism for knowing when a node is ready
    // sleep(Duration::from_millis(1000)).await;
    //
    // let _node_child_process_2 = Command::new("./target/debug/node")
    //     .kill_on_drop(true)
    //     .args(&["0.0.0.0:50052", "0.0.0.0:50051"])
    //     .spawn()
    //     .expect("failed to start node");
    // sleep(Duration::from_millis(1000)).await;
    //
    // let mut client = NodeServiceClient::connect("http://0.0.0.0:50051")
    //     .await
    //     .unwrap();
    //
    // let key = "this is a test";
    // let val = "these are some bytes";
    // let request = Request::new(SetRequest {
    //     key: key.into(),
    //     val: val.into(),
    // });
    //
    // let set_resp = client.set(request).await.unwrap();
    // let set_data = set_resp.into_inner();
    // println!("key: {}, val: {} inserted on: {}", key, val, set_data.loc);
    //
    // let get_request = Request::new(GetRequest {
    //     getter: Some(Getter::Key(key.into())),
    // });
    //
    // let get_resp = client.get(get_request).await.unwrap();
    // let get_data = get_resp.into_inner();
    // println!(
    //     "value of key: {} is: {}, located on: {}",
    //     key, get_data.val, get_data.loc
    // );

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
    Other,
}

#[derive(Serialize, Deserialize, Debug)]
struct NodeState {
    addr: String,
    pred: String,
    succ: String,
}

async fn handle_socket(mut socket: WebSocket) {
    socket.send(Message::Ping(vec![])).await.unwrap();
    socket.recv().await.unwrap().unwrap();

    let (mut send, mut recv) = socket.split();

    let (poll_start_send, poll_start_recv) = oneshot::channel::<()>();
    let poll_task = tokio::spawn(async move {
        poll_start_recv.await.unwrap();

        let mut clients = BTreeMap::<String, NodeServiceClient<Channel>>::new();

        let node_addr = "0.0.0.0:50051";
        let _node = Command::new("./target/debug/node")
            .kill_on_drop(true)
            .args(&[node_addr])
            .spawn()
            .expect("failed to start node");
        // TODO: maybe a mechanism for knowing when a node is ready
        sleep(Duration::from_millis(1000)).await;
        let client = NodeServiceClient::connect(format!("http://{}", node_addr))
            .await
            .unwrap();

        clients.insert(node_addr.into(), client);

        let mut interval = interval(Duration::from_millis(500));
        loop {
            interval.tick().await;

            // PERF: make this parallel
            for client_conn in clients.iter_mut() {
                let get_state_resp = client_conn
                    .1
                    .get_state(Request::new(GetStateRequest {}))
                    .await
                    .unwrap();
                let get_state_message = get_state_resp.into_inner();
                send.send(Message::Text(
                    serde_json::to_string(&NodeState {
                        addr: client_conn.0.clone(),
                        pred: get_state_message.pred,
                        succ: get_state_message.succ,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            }
        }
    });

    let recv_task = tokio::spawn(async move {
        match recv.next().await {
            Some(Ok(Message::Text(msg_text))) => {
                let ws_text_message: WsTextMessage = serde_json::from_str(&msg_text).unwrap();
                match ws_text_message {
                    WsTextMessage::Start => {
                        println!("start called");
                        poll_start_send.send(()).unwrap();
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    });

    recv_task.await.unwrap();
    poll_task.await.unwrap();
}
