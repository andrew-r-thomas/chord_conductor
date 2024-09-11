use tokio::{
    process::Command,
    time::{sleep, Duration},
};
use tonic::Request;
mod node {
    tonic::include_proto!("node");
}

use node::{get_request::Getter, node_service_client::NodeServiceClient, GetRequest, SetRequest};

#[tokio::main]
async fn main() {
    let _node_child_process_1 = Command::new("./target/debug/node")
        .kill_on_drop(true)
        .args(&["0.0.0.0:50051"])
        .spawn()
        .expect("failed to start node");
    // TODO: maybe a mechanism for knowing when a node is ready
    sleep(Duration::from_millis(1000)).await;

    let _node_child_process_2 = Command::new("./target/debug/node")
        .kill_on_drop(true)
        .args(&["0.0.0.0:50052", "0.0.0.0:50051"])
        .spawn()
        .expect("failed to start node");
    sleep(Duration::from_millis(1000)).await;

    let mut client = NodeServiceClient::connect("http://0.0.0.0:50051")
        .await
        .unwrap();

    let key = "this is a test";
    let val = "these are some bytes";
    let request = Request::new(SetRequest {
        key: key.into(),
        val: val.into(),
    });

    let set_resp = client.set(request).await.unwrap();
    let set_data = set_resp.into_inner();
    println!("key: {}, val: {} inserted on: {}", key, val, set_data.loc);

    let get_request = Request::new(GetRequest {
        getter: Some(Getter::Key(key.into())),
    });

    let get_resp = client.get(get_request).await.unwrap();
    let get_data = get_resp.into_inner();
    println!(
        "value of key: {} is: {}, located on: {}",
        key, get_data.val, get_data.loc
    );
}
