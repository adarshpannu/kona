// pop_hashjoin

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, POPContext, POP},
    stage::Stage,
};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {}

impl HashJoin {}

/***************************************************************************************************/
pub struct HashJoinContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    built_hashtable: bool,
}

impl POPContext for HashJoinContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Chunk<Box<dyn Array>>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        if let POP::HashJoin(_) = pop {
            // Drain build-side (i.e. right child), then drain probe-side (left child)
            loop {
                let child = if self.built_hashtable { &mut self.children[0] } else { &mut self.children[1] };

                let chunk = child.next(flow, stage)?;
                if chunk.len() == 0 {
                    if !self.built_hashtable {
                        self.built_hashtable = true
                    } else {
                        break;
                    }
                } else {
                    let side = if !self.built_hashtable { "build " } else { "probe " };
                    debug!(
                        "HashJoinContext {:?} partition = {}, side = {}::\n{}",
                        self.pop_key,
                        self.partition_id,
                        side,
                        chunk_to_string(&chunk)
                    );
                }
            }
        } else {
            panic!("ugh")
        }

        Ok(Chunk::new(vec![]))
    }
}

impl HashJoinContext {
    pub fn new(pop_key: POPKey, _: &HashJoin, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        Ok(Box::new(HashJoinContext {
            pop_key,
            children,
            partition_id,
            built_hashtable: false,
        }))
    }
}
