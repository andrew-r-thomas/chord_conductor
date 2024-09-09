use node::{node_service_server::NodeService, SetRequest, SetResponse};

use std::{collections::HashMap, sync::Arc};
use tonic::{transport::Server, Request, Response, Status};

use sha2::{Digest, Sha256};

pub mod node {
    tonic::include_proto!("node");
}

pub struct Node {
    id: String,
    data: HashMap<String, String>,
    sucessor: Option<String>,
    predecessor: Option<String>,
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        // NOTE: good job getting back to it, do this piece next
    }
}

impl Node {
    pub fn start(id: String) -> Self {
        let data = HashMap::<String, String>::new();

        Self {
            id,
            data,
            sucessor: None,
            predecessor: None,
        }
    }
    pub fn get() {}
    pub fn set(&mut self, key: String, val: String, nodes: Arc<HashMap<String, Self>>) {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        // PERF: i dont like creating a vec every time
        // TODO: unwrap
        // TODO: figure out if strings are really what we want here
        let hash = String::from_utf8(hasher.finalize().to_vec()).unwrap();
        // TODO: unwrap
        if hash <= self.id && &hash >= self.predecessor.as_ref().unwrap() {
            self.data.insert(key, val);
        } else {
            // TODO: unwrap
            let sucessor = nodes.get_mut(self.sucessor.as_ref().unwrap()).unwrap();
            sucessor.set(key, val, nodes.clone());
        }
    }
}
