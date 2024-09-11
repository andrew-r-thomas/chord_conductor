use std::process::Command;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::time::{sleep, Duration};
use tonic::Request;
mod node {
    tonic::include_proto!("node");
}

use node::{node_service_client::NodeServiceClient, SetRequest};

#[tokio::main]
async fn main() {
    let mut node_child_process = Command::new("./target/debug/node")
        .spawn()
        .expect("failed to start node");
    // TODO: maybe a mechanism for knowing when a node is ready
    sleep(Duration::from_millis(1000)).await;
    let mut client = NodeServiceClient::connect("http://0.0.0.0:50051")
        .await
        .unwrap();

    let request = Request::new(SetRequest {
        key: "this is a test".into(),
        val: "these are some bytes".into(),
    });

    client.set(request).await.unwrap();
    node_child_process.kill().unwrap();
}

// struct Hashed {
//     val: String,
//     hash: Bytes,
// }
//
// impl PartialEq for Hashed {
//     fn eq(&self, other: &Self) -> bool {
//         self.hash.eq(&other.hash)
//     }
// }
//
// impl PartialOrd for Hashed {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         self.hash.partial_cmp(&other.hash)
//     }
// }
//
// fn main() {
//     let mut hashes = vec![];
//     {
//         let mut hasher = Sha256::new();
//         hasher.update(BYTES_MAX);
//         hashes.push(Hashed {
//             val: "MAX".into(),
//             hash: Bytes::from(hasher.finalize().to_vec()),
//         });
//     }
//
//     for i in 1..9 {
//         let ip = format!("0.0.0.0:5005{}", i);
//         let mut hasher = Sha256::new();
//         hasher.update(ip.as_bytes());
//         let hash = Bytes::from(hasher.finalize().to_vec());
//         hashes.push(Hashed { val: ip, hash });
//     }
//
//     hashes.sort_by(|a, b| a.partial_cmp(b).unwrap());
//
//     for ip in hashes.iter().map(|h| h.val.clone()) {
//         println!("{}", ip);
//     }
// }
