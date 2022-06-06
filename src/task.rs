// task

use std::collections::HashMap;
use crate::includes::*;
use crate::pop::*;

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub partition_id: PartitionId,

    #[serde(skip)]
    pub contexts: HashMap<POPKey, NodeRuntime>,

    #[serde(skip)]
    pub task_row: Row,
}

// Tasks write to flow-id / top-id / dest-part-id / source-part-id
impl Task {
    pub fn new(partition_id: PartitionId) -> Task {
        Task {
            partition_id,
            contexts: HashMap::new(),
            task_row: Row::from(vec![]),
        }
    }

    pub fn run(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        /*
        debug!(
            "Running task: stage = {:?}, partition = {}/{}",
            stage.root_pop_key, self.partition_id, stage.npartitions_producer
        );
        */
        self.task_row = Row::from(vec![Datum::NULL; 32]); // FIXME
        loop {
            let retval = stage.root_pop_key.unwrap().next(flow, stage, self, true)?;
            if !retval {
                break;
            }
        }
        Ok(())
    }
}
