use ethnum::U256;
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        watch::{self, Receiver},
    },
    task::JoinSet,
};

#[derive(Debug, Clone)]
pub struct Predecessor {
    pub send: UnboundedSender<Option<PredecessorData>>,
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
        let (mpsc_send, mut mpsc_recv) = mpsc::unbounded_channel::<Option<PredecessorData>>();

        join_set.spawn(async move {
            while let Some(p) = mpsc_recv.recv().await {
                watch_send.send_modify(|current| *current = p);
            }
        });

        Self {
            send: mpsc_send,
            recv: watch_recv,
        }
    }
}
