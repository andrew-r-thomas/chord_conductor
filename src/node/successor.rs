use ethnum::U256;
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        watch::{self, Receiver},
    },
    task::JoinSet,
};
use tonic::transport::Channel;

use crate::node_service::node_service_client::NodeServiceClient;

#[derive(Debug)]
pub struct Successor {
    pub recv: Receiver<Option<SuccessorData>>,
    pub send: UnboundedSender<SuccessorData>,
}

#[derive(Clone, Debug)]
pub struct SuccessorData {
    pub addr: String,
    pub hash: U256,
    pub client: NodeServiceClient<Channel>,
}

impl Successor {
    pub fn spawn(init: Option<SuccessorData>, join_set: &mut JoinSet<()>) -> Self {
        let (watch_send, watch_recv) = watch::channel::<Option<SuccessorData>>(init);
        let (mpsc_send, mut mpsc_recv) = mpsc::unbounded_channel::<SuccessorData>();
        join_set.spawn(async move {
            while let Some(s) = mpsc_recv.recv().await {
                watch_send.send(Some(s)).unwrap()
            }
        });

        Self {
            recv: watch_recv,
            send: mpsc_send,
        }
    }
}
