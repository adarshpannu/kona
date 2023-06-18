// task

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{POPContext, POP},
    pop_csv::CSVContext,
    pop_hashjoin::HashJoinContext,
    pop_repartition::{RepartitionReadContext, RepartitionWriteContext},
    stage::Stage,
};

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub partition_id: PartitionId,

    #[serde(skip)]
    pub contexts: Vec<Box<dyn POPContext>>,
}

// Tasks write to flow-id / top-id / dest-part-id / source-part-id
impl Task {
    pub fn new(partition_id: PartitionId) -> Task {
        Task {
            partition_id,
            contexts: vec![],
        }
    }

    pub fn run(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        /*
        debug!(
            "Running task: stage = {:?}, partition = {}/{}",
            stage.root_pop_key, self.partition_id, stage.npartitions_producer
        );
        */
        let root_pop_key = stage.root_pop_key.unwrap();

        let mut root_context = self.init_context(flow, stage, root_pop_key)?;
        loop {
            let chunk = root_context.next(flow, stage)?;
            if chunk.len() == 0 {
                break;
            }
        }

        Ok(())
    }

    pub fn init_context(&self, flow: &Flow, stage: &Stage, popkey: POPKey) -> Result<Box<dyn POPContext>, String> {
        let (pop, _, children) = stage.pop_graph.get3(popkey);
        let child_contexts = if let Some(children) = children {
            let children = children
                .iter()
                .map(|&child_popkey| self.init_context(flow, stage, child_popkey).unwrap())
                .collect::<Vec<_>>();
            Some(children)
        } else {
            None
        };

        let ctxt = match &pop {
            POP::CSV(csv) => CSVContext::new(popkey, csv, self.partition_id)?,
            POP::RepartitionWrite(rpw) => RepartitionWriteContext::new(popkey, &rpw, child_contexts.unwrap(), self.partition_id)?,

            // FIXME: Since POP graphs are local to stages, repartition directories may not be unique. Use LOPKey?
            POP::RepartitionRead(rpr) => RepartitionReadContext::new(flow.id, popkey, &rpr, self.partition_id)?,
            POP::HashJoin(hj) => HashJoinContext::new(popkey, &hj, child_contexts.unwrap(), self.partition_id)?,
            _ => unimplemented!(),
        };
        Ok(ctxt)
    }
}
