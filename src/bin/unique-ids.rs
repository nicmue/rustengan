use anyhow::Context;
use rustengan::*;

use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    node: String,
    msg_id: usize,
}

impl Node<(), Payload> for UniqueNode {
    fn from_init(
        _state: (),
        msg_id: usize,
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(UniqueNode {
            node: init.node_id,
            msg_id,
        })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        let (input, payload) = input.split();
        match payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.msg_id);
                input
                    .into_reply(Some(&mut self.msg_id), Payload::GenerateOk { guid })
                    .send(&mut *output)
                    .context("reply to generate")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _, _>(())
}
