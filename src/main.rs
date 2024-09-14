use core::panic;
use std::{
    collections::HashMap,
    process::Stdio,
    sync::{mpsc::Receiver, Arc},
    thread,
};

use eframe::egui;
use egui_graphs::{DefaultEdgeShape, DefaultNodeShape, Graph, GraphView};
use petgraph::{
    data::Build,
    graph::{EdgeIndex, NodeIndex},
    stable_graph::StableGraph,
    Directed,
};
use sha2::digest::consts::False;
use tokio::{
    io::AsyncReadExt,
    process::Command,
    runtime::Runtime,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot, RwLock,
    },
    time::{interval, Duration},
};
use tonic::{transport::Channel, Request};

mod node {
    tonic::include_proto!("node");
}
use node::{node_service_client::NodeServiceClient, GetStateRequest};

enum GuiMessage {
    Start(egui::Context),
    AddNode,
}

fn main() -> eframe::Result {
    let runtime = Runtime::new().unwrap();
    let _ = runtime.enter();
    let (gui_send, mut rt_recv) = mpsc::unbounded_channel::<GuiMessage>();
    let (rt_send, gui_recv) = std::sync::mpsc::channel::<StableGraph<(), ()>>();

    thread::spawn(move || {
        runtime.block_on(async {
            let clients = Arc::new(RwLock::<Vec<Client>>::new(vec![]));

            let (start_send, start_recv) = oneshot::channel::<egui::Context>();
            let poll_clients = clients.clone();
            let poll_task = tokio::spawn(async move {
                let ctx = start_recv.await.unwrap();
                let mut interval = interval(Duration::from_millis(1000));

                loop {
                    interval.tick().await;
                    let mut poll_clients_write = poll_clients.write().await;
                    // let nodes = HashMap::new();
                    let mut g: StableGraph<(), ()> = StableGraph::new();

                    // for c in poll_clients_write.iter_mut() {
                    //     let get_state_resp = c
                    //         .client
                    //         .get_state(Request::new(GetStateRequest {}))
                    //         .await
                    //         .unwrap();
                    //     let get_state_data = get_state_resp.into_inner();
                    //     let succ = match get_state_data.succ.is_empty() {
                    //         true => None,
                    //         false => Some(get_state_data.succ),
                    //     };
                    // }

                    let a = g.add_node(());
                    let b = g.add_node(());
                    g.add_edge(a, b, ());

                    rt_send.send(g).unwrap();
                    ctx.request_repaint();
                }
            });

            if let Some(GuiMessage::Start(ctx)) = rt_recv.recv().await {
                let mut clients_write = clients.write().await;
                let new_client = Client::spawn(None).await;
                clients_write.push(new_client);
                start_send.send(ctx).unwrap();
            }

            while let Some(gui_message) = rt_recv.recv().await {
                match gui_message {
                    GuiMessage::AddNode => {
                        let mut client_write = clients.write().await;
                        let new_client =
                            Client::spawn(Some(client_write.first().unwrap().addr.clone())).await;
                        client_write.push(new_client);
                    }
                    _ => {
                        println!("ahhhh!!!");
                        panic!();
                    }
                }
            }
            poll_task.await.unwrap();
        });
    });

    eframe::run_native(
        "Conductor",
        eframe::NativeOptions {
            vsync: false,
            ..Default::default()
        },
        Box::new(|_cc| Ok(Box::new(Gui::new(gui_send, gui_recv)))),
    )
}

struct Gui {
    send: UnboundedSender<GuiMessage>,
    recv: Receiver<StableGraph<(), ()>>,
    graph: Graph<(), (), Directed>,
    nodes: HashMap<String, (NodeIndex, Option<EdgeIndex>)>,
}

impl Gui {
    pub fn new(send: UnboundedSender<GuiMessage>, recv: Receiver<StableGraph<(), ()>>) -> Self {
        let mut g: StableGraph<(), ()> = StableGraph::new();
        g.add_node(());
        let graph = Graph::from(&g);
        Self {
            send,
            recv,
            graph,
            nodes: HashMap::new(),
        }
    }
}

impl eframe::App for Gui {
    fn update(&mut self, ctx: &eframe::egui::Context, frame: &mut eframe::Frame) {
        if let Ok(g) = self.recv.try_recv() {
            self.graph = Graph::from(&g);
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Conductor");
            if ui.button("start").clicked() {
                self.send.send(GuiMessage::Start(ctx.clone())).unwrap()
            }
            if ui.button("add node").clicked() {
                self.send.send(GuiMessage::AddNode).unwrap();
            }
            ui.add(&mut GraphView::<
                _,
                _,
                _,
                _,
                DefaultNodeShape,
                DefaultEdgeShape,
            >::new(&mut self.graph));
        });
    }
}

struct Client {
    addr: String,
    succ: Option<String>,
    client: NodeServiceClient<Channel>,
}

impl Client {
    pub async fn spawn(join_addr: Option<String>) -> Self {
        let mut node = match join_addr {
            Some(ja) => Command::new("./target/debug/node")
                .args(&[ja])
                .stdout(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
            None => Command::new("./target/debug/node")
                .stdout(Stdio::piped())
                .spawn()
                .expect("failed to start node"),
        };
        let mut stdout = node.stdout.take().unwrap();
        let mut bytes = [0; 13];
        stdout.read(&mut bytes).await.unwrap();
        let addr = String::from_utf8(bytes.to_vec()).unwrap();
        let client = NodeServiceClient::connect(format!("http://{}", addr.clone()))
            .await
            .unwrap();
        println!("connected to client: {}", addr.clone());
        Self {
            addr,
            client,
            succ: None,
        }
    }
}
