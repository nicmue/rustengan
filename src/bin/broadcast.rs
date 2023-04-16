use rustengan::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    sync::mpsc::Sender,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    node: String,
    msg_id: usize,
    messages: HashSet<usize>,
    knows: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(
        _state: (),
        msg_id: usize,
        init: Init,
        _tx: Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            node: init.node_id,
            msg_id,
            knows: HashMap::new(),
            messages: HashSet::new(),
            neighborhood: Vec::new(),
        })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(_) => {}
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    Payload::Broadcast { message } => {
                        if self.messages.insert(message) {
                            // if new to us we broadcast to our neighbours
                            for n in self.neighborhood.iter().filter(|n| *n != &reply.dst) {
                                Message {
                                    src: self.node.clone(),
                                    dst: n.clone(),
                                    body: Body {
                                        id: None,
                                        in_reply_to: None,
                                        payload: Payload::Broadcast { message },
                                    },
                                }
                                .send(&mut *output)
                                .with_context(|| format!("broadacst to neighbour {n}"))?;
                            }
                        }
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(&mut *output).context("reply to broadcast")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(&mut *output).context("reply to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("no topology given for node {}", self.node));
                        self.knows = self
                            .neighborhood
                            .iter()
                            .cloned()
                            .map(|n| (n, HashSet::new()))
                            .collect();
                        reply.body.payload = Payload::TopologyOk;
                        reply.send(&mut *output).context("reply to topology")?;
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
