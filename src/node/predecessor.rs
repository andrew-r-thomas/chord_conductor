use ethnum::U256;
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        watch::{self, Receiver},
    },
    task::JoinSet,
};
use tracing::info;

#[derive(Debug, Clone)]
pub struct Predecessor {
    pub send: UnboundedSender<PredecessorData>,
    pub recv: Receiver<Option<PredecessorData>>,
}

#[derive(Debug, Clone)]
pub struct PredecessorData {
    pub addr: String,
    pub hash: U256,
}

impl Predecessor {
    pub fn spawn(join_set: &mut JoinSet<()>) -> Self {
        let (watch_send, watch_recv) = watch::channel::<Option<PredecessorData>>(None);
        let (mpsc_send, mut mpsc_recv) = mpsc::unbounded_channel::<PredecessorData>();

        join_set.spawn(async move {
            while let Some(p) = mpsc_recv.recv().await {
                info!("got a new pred: {}", p.addr);
                watch_send.send_modify(|current| *current = Some(p));
            }
        });

        Self {
            send: mpsc_send,
            recv: watch_recv,
        }
    }
}
