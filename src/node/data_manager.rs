use std::collections::BTreeMap;

use futures::{channel::mpsc::UnboundedReceiver, StreamExt};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::node_service::node_service_client::NodeServiceClient;

struct DataManager {
    data: BTreeMap<Vec<u8>, String>,
    pred: Option<PredHandle>,
    succ: Option<SuccHandle>,
}

enum DataManagerMessage {
    Get,
    Set,
    Stabilize,
    Join,
    Pred,
    Notify,
    GetState,
}

impl DataManager {
    pub fn start(mut recv: UnboundedReceiver<DataManagerMessage>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let data_manager = Self {
                data: BTreeMap::new(),
                pred: None,
                succ: None,
            };

            while let Some(message) = recv.next().await {}
        })
    }
}

#[derive(Debug, Clone)]
pub struct SuccHandle {
    addr: String,
    hash: Vec<u8>,
    client: NodeServiceClient<Channel>,
}
#[derive(Debug)]
pub struct PredHandle {
    addr: String,
    hash: Vec<u8>,
}
