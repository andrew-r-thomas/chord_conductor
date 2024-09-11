use std::collections::BTreeMap;

use itertools::Itertools;
use tokio::sync::mpsc::Receiver;
use tonic::{transport::Channel, Request};

use crate::{
    node_service::{node_service_client::NodeServiceClient, JoinRequest, SetRequest},
    Command, CommandResult,
};

pub struct DataManager {
    ip: String,
    data: BTreeMap<Vec<u8>, String>,
    recv: Receiver<Command>,
    succ: NodeServiceClient<Channel>,
}

impl DataManager {
    pub async fn start(&mut self) {
        while let Some(command) = self.recv.recv().await {
            match command {
                Command::Set {
                    key,
                    val,
                    hash,
                    local,
                    sender,
                } => {
                    match local {
                        true => {
                            // TODO: logging
                            self.data.insert(hash, val);
                        }
                        false => {
                            // PERF: this means that every node has to rehash the key
                            self.succ
                                .set(Request::new(SetRequest { key, val }))
                                .await
                                .unwrap();
                        }
                    }
                    sender.send(CommandResult::Set).unwrap();
                }
                Command::Join {
                    ip,
                    pred,
                    sender,
                    hash,
                } => match pred {
                    true => {
                        let (keys, vals) = self.shed_data(&hash);

                        sender
                            .send(CommandResult::Join {
                                ip: self.ip.clone(),
                                keys,
                                vals,
                            })
                            .unwrap();
                    }
                    false => {
                        let resp = self
                            .succ
                            .join(Request::new(JoinRequest { ip }))
                            .await
                            .unwrap();
                        let resp_message = resp.into_inner();

                        sender
                            .send(CommandResult::Join {
                                ip: resp_message.succ_ip,
                                keys: resp_message.keys,
                                vals: resp_message.vals,
                            })
                            .unwrap();
                    }
                },
            }
        }
    }

    // PERF:
    fn shed_data(&mut self, pred_hash: &[u8]) -> (Vec<Vec<u8>>, Vec<String>) {
        let to_keep = self.data.split_off(pred_hash);
        let to_shed = self.data.clone();
        self.data = to_keep;

        (
            to_shed.keys().cloned().collect_vec(),
            to_shed.values().cloned().collect_vec(),
        )
    }

    pub fn new(ip: String, recv: Receiver<Command>, succ: NodeServiceClient<Channel>) -> Self {
        Self {
            data: BTreeMap::<Vec<u8>, String>::new(),
            recv,
            ip,
            succ,
        }
    }
}
