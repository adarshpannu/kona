// stage

use std::collections::HashMap;

use crate::{
    graph::{LOPKey, POPKey},
    includes::*,
    pop::POPGraph,
    scheduler::SchedulerMessage,
    task::Task,
    Flow,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Stage {
    pub stage_id: StageId,
    pub parent_stage_id: Option<StageId>, // 0 == no stage depends on this
    pub root_lop_key: LOPKey,
    pub root_pop_key: Option<POPKey>,
    pub orig_child_count: usize,
    pub npartitions: usize,
    pub pop_count: usize, // # of POPs in this stage
}

#[derive(Debug)]
pub struct StageStatus {
    // Runtime details
    pub completed_child_count: usize,
    pub completed_npartitions: usize,
}

impl StageStatus {
    pub fn new() -> Self {
        StageStatus {
            completed_child_count: 0,
            completed_npartitions: 0,
        }
    }
}

/***************************************************************************************************/
impl Stage {
    pub fn new(stage_id: usize, parent_stage_id: Option<usize>, root_lop_key: LOPKey) -> Self {
        debug!("New stage with root_lop_key: {:?}", root_lop_key);

        Stage {
            stage_id,
            parent_stage_id,
            root_lop_key,
            root_pop_key: None,
            orig_child_count: 0,
            npartitions: 0,
            pop_count: 0,
        }
    }

    pub fn run(&self, env: &Env, flow: &Flow) {
        let (_, props, ..) = flow.pop_graph.get3(self.root_pop_key.unwrap());
        let npartitions = props.npartitions;
        for partition_id in 0..npartitions {
            let task = Task::new(partition_id);
            //task.run(flow, self);

            let thread_id = partition_id % (env.scheduler.size());

            let task_triplet = &(flow, self, task);
            let task_serialized: Vec<u8> = bincode::serialize(&task_triplet).unwrap();

            env.scheduler.s2t_channels_sx[thread_id]
                .send(SchedulerMessage::RunTask(task_serialized))
                .unwrap();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StageGraph {
    pub stages: Vec<Stage>, // Stage hierarchy encoded in this array
}

impl StageGraph {
    pub fn new() -> Self {
        let status_vec = vec![]; // zeroth entry is not used
        StageGraph { stages: status_vec }
    }

    pub fn add_stage(&mut self, root_lop_key: LOPKey, parent_stage_id: Option<usize>) -> usize {
        // `parent_stage_id` must exist, and it will become dependent on newly created stage
        // newly created stage goes at the end of the vector, and its `id` is essentially its index
        let new_id = self.stages.len();

        let new_ss = Stage::new(new_id, parent_stage_id, root_lop_key);
        self.stages.push(new_ss);

        if let Some(parent_stage_id) = parent_stage_id {
            assert!(parent_stage_id <= self.stages.len());

            let parent_stage = &mut self.stages[parent_stage_id];
            parent_stage.orig_child_count = parent_stage.orig_child_count + 1;
        }

        debug!("Added new stage: {:?}", new_id);
        new_id
    }

    pub fn set_pop_key(&mut self, pop_graph: &POPGraph, stage_id: StageId, pop_key: POPKey) {
        let stage = &mut self.stages[stage_id];
        let props = &pop_graph.get(pop_key).properties;
        stage.npartitions = props.npartitions;
        stage.root_pop_key = Some(pop_key)
    }

    pub fn increment_pop(&mut self, stage_id: StageId) -> usize {
        let stage = &mut self.stages[stage_id];
        stage.pop_count += 1;
        return stage.pop_count;
    }

    pub fn print(&self) {
        for stage in self.stages.iter() {
            debug!("--- Stage: {:?}", stage)
        }
    }

    pub fn get_pop_to_stage_map(&self) -> HashMap<POPKey, &Stage> {
        self.stages.iter().map(|s| (s.root_pop_key.unwrap(), s)).collect()
    }
}
