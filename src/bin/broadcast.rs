use rustengan::*;

use anyhow::{Context, Ok};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    io::StdoutLock,
    sync::mpsc::Sender,
    time::Duration,
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
    Gossip {
        messages: HashSet<usize>,
    },
    GossipOk {
        messages: HashSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node: String,
    msg_id: usize,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    gossip_in_flight: HashMap<String, (usize, HashSet<usize>)>,
    neighborhood: Vec<String>,
}

impl BroadcastNode {
    fn gossip_to_all(&mut self, output: &mut StdoutLock) {
        let neighbourhood: Vec<String> = self.neighborhood.iter().cloned().collect();
        for n in neighbourhood {
            self.gossip_to(n, output);
        }
    }

    fn gossip_to(&mut self, neighbour: String, output: &mut StdoutLock) {
        let known_to_n = &self.known[&neighbour];
        let messages: HashSet<_> = self
            .messages
            .iter()
            .copied()
            .filter(|m| !known_to_n.contains(m))
            .collect();
        if messages.is_empty() {
            return;
        }

        // allocate message id
        let msg_id = self.msg_id;
        self.msg_id += 1;

        // upsert in_flight_gossip
        let entry = self.gossip_in_flight.entry(neighbour.clone()).or_default();
        entry.0 = msg_id;
        entry.1 = messages.clone();

        // we dont error on sending here because we anyways
        // retry if we dont receive an ok. with this we make
        // sure we try to gossip to everyone
        let _ = Message {
            src: self.node.clone(),
            dst: neighbour.clone(),
            body: Body {
                id: Some(msg_id),
                in_reply_to: None,
                payload: Payload::Gossip { messages },
            },
        }
        .send(output);
    }
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        msg_id: usize,
        init: Init,
        tx: Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(100));
                if let Err(_) = tx.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        Ok(Self {
            node: init.node_id,
            msg_id,
            known: HashMap::new(),
            messages: HashSet::new(),
            gossip_in_flight: HashMap::new(),
            neighborhood: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(InjectedPayload::Gossip) => {
                self.gossip_to_all(&mut *output);
            }
            Event::Message(input) => {
                let (input, payload) = input.split();
                match payload {
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        input
                            .into_reply(Some(&mut self.msg_id), Payload::BroadcastOk)
                            .send(&mut *output)
                            .context("reply to broadcast")?;
                    }
                    Payload::Read => {
                        input
                            .into_reply(
                                Some(&mut self.msg_id),
                                Payload::ReadOk {
                                    messages: self.messages.clone(),
                                },
                            )
                            .send(&mut *output)
                            .context("reply to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("no topology given for node {}", self.node));
                        self.known = self
                            .neighborhood
                            .iter()
                            .cloned()
                            .map(|n| (n, HashSet::new()))
                            .collect();
                        input
                            .into_reply(Some(&mut self.msg_id), Payload::TopologyOk)
                            .send(&mut *output)
                            .context("reply to topology")?;
                    }
                    Payload::Gossip { messages } => {
                        self.known
                            .get_mut(&input.src)
                            .expect("got gossip from unknown node")
                            .extend(messages.iter().copied());
                        self.messages.extend(messages);

                        let known_to_n = &self.known[&input.src];
                        let messages: HashSet<_> = self
                            .messages
                            .iter()
                            .copied()
                            .filter(|m| !known_to_n.contains(m))
                            .collect();

                        input
                            .into_reply(Some(&mut self.msg_id), Payload::GossipOk { messages })
                            .send(&mut *output)
                            .context("reply to gossip")?;
                    }
                    Payload::GossipOk { messages } => {
                        self.known
                            .get_mut(&input.src)
                            .expect("got gossip_ok from unknown node")
                            .extend(messages.iter().copied());
                        self.messages.extend(messages);

                        let Entry::Occupied(entry) = self.gossip_in_flight.entry(input.src.clone()) else {
                            return Ok(());
                        };

                        if entry.get().0 == input.body.in_reply_to.unwrap() {
                            let (_, messages) = entry.remove();
                            self.known
                                .get_mut(&input.src)
                                .expect("got gossip_ok  from unknown node")
                                .extend(messages.iter().copied());
                        }
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
