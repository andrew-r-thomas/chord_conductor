use std::collections::HashMap;

use bytes::Bytes;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tonic::transport::Channel;

use crate::{node_service::node_service_client::NodeServiceClient, Command, CommandResult};

pub struct DataManager {
    ip: String,
    data: HashMap<Bytes, String>,
    recv: Receiver<Command>,
    pred: Option<NodeServiceClient<Channel>>,
}

impl DataManager {
    pub async fn start(&mut self) {
        while let Some(command) = self.recv.recv().await {
            match command {
                Command::Set {
                    key,
                    val,
                    local,
                    sender,
                } => match local {
                    true => {
                        // TODO: logging
                        self.data.insert(key, val);
                    }
                    false => {
                        panic!()
                    }
                },
                Command::Join {
                    ip,
                    pred,
                    sender,
                    hash,
                } => match pred {
                    true => {
                        self.pred = Some(
                            NodeServiceClient::connect(format!("http://{}", ip))
                                .await
                                .unwrap(),
                        );
                        let (keys, vals) = self.shed_data(hash);
                        sender
                            .send(CommandResult::Join {
                                ip: self.ip.clone(),
                                keys,
                                vals,
                            })
                            .unwrap();
                    }
                    false => {}
                },
            }
        }
    }

    fn shed_data(&mut self, pred_hash: Bytes) -> (Vec<Vec<u8>>, Vec<String>) {
        let keys = vec![];
        let vals = vec![];

        // TODO: this is where we use itertools sorted iterator

        (keys, vals)
    }

    pub fn new(ip: String, recv: Receiver<Command>) -> Self {
        Self {
            data: HashMap::<Bytes, String>::new(),
            recv,
            pred: None,
            ip,
        }
    }
}
