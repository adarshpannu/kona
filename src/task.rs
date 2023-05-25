// task

use crate::{
    flow::Flow,
    includes::*,
    pop::{POPContext, POP},
    pop_csv::CSVPartitionIter,
    stage::Stage,
};

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub partition_id: PartitionId,

    #[serde(skip)]
    pub contexts: Vec<POPContext>,
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
        self.init_contexts(flow, stage);

        loop {
            let output_chunk = stage.root_pop_key.unwrap().next(flow, stage, self)?;
            if output_chunk.len() == 0 {
                break;
            }
        }
        Ok(())
    }

    pub fn init_contexts(&mut self, flow: &Flow, stage: &Stage) {
        let root_pop_key = stage.root_pop_key.unwrap();

        for _ in 0..stage.pop_count {
            self.contexts.push(POPContext::UninitializedContext)
        }

        /*
        let stop_search = |popkey| {
            let pop = &flow.pop_graph.get(popkey).value;
            matches!(pop, POP::Repartition(_))
        };
        */

        let mut iter = flow.pop_graph.iter(root_pop_key);
        while let Some(popkey) = iter.next(&flow.pop_graph) {
            let (pop, props, ..) = flow.pop_graph.get3(popkey);
            let ix = props.index_in_stage;
            let ctx = match &pop {
                POP::CSV(csv) => {
                    let iter = CSVPartitionIter::new(csv, self.partition_id).unwrap();
                    POPContext::CSVContext { iter }
                }
                _ => unimplemented!(),
            };
            self.contexts[ix] = ctx;
        }
    }
}
