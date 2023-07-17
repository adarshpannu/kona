// stage

use std::collections::HashMap;

use crate::{
    graph::{Graph, LOPKey, POPKey},
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
    pub parent_pop_key: Option<POPKey>,   // POPKey that 'reads' this stage
    pub root_lop_key: LOPKey,
    pub root_pop_key: Option<POPKey>,
    pub nchildren: usize, // # of stages this stage depends on
    pub npartitions: usize,
    pub pop_graph: POPGraph,
}

#[derive(Debug, Default)]
pub struct StageContext {
    // Runtime details
    pub nchildren_completed: usize,
    pub npartitions_completed: usize,
}

/***************************************************************************************************/
impl Stage {
    pub fn new(stage_id: usize, parent_stage_id: Option<usize>, root_lop_key: LOPKey) -> Self {
        debug!("New stage with root_lop_key: {:?}", root_lop_key);
        let pop_graph = Graph::default();

        Stage {
            stage_id,
            parent_stage_id,
            parent_pop_key: None,
            root_lop_key,
            root_pop_key: None,
            nchildren: 0,
            npartitions: 0,
            pop_graph,
        }
    }

    pub fn schedule(&self, env: &Env, flow: &Flow) {
        debug!("Schedule stage: {:?}", self.root_pop_key);

        let (_, props, ..) = self.pop_graph.get3(self.root_pop_key.unwrap());
        let npartitions = props.npartitions;
        for partition_id in 0..npartitions {
            let task = Task::new(partition_id);
            //task.run(flow, self);

            let thread_id = partition_id % (env.scheduler.nthreads());

            let task_triplet = &(flow, self, task);
            let task_serialized: Vec<u8> = bincode::serialize(&task_triplet).unwrap();

            env.scheduler.s2t_channels_sx[thread_id]
                .send(SchedulerMessage::ScheduleTask(task_serialized))
                .unwrap();
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
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
            parent_stage.nchildren += 1;
        }

        debug!("Added new stage: {:?}", new_id);
        new_id
    }

    pub fn set_root_pop_key(&mut self, stage_id: StageId, pop_key: POPKey) {
        let stage = &mut self.stages[stage_id];
        let props = &stage.pop_graph.get(pop_key).properties;
        stage.npartitions = props.npartitions;
        stage.root_pop_key = Some(pop_key)
    }

    pub fn set_parent_pop_key(&mut self, stage_id: StageId, pop_key: POPKey) {
        let stage = &mut self.stages[stage_id];
        stage.parent_pop_key = Some(pop_key)
    }

    pub fn print(&self) {
        debug!("Stage graph");
        for stage in self.stages.iter() {
            debug!("    Stage: {:?}", stage)
        }
    }

    pub fn get_pop_to_stage_map(&self) -> HashMap<POPKey, &Stage> {
        self.stages.iter().map(|s| (s.root_pop_key.unwrap(), s)).collect()
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct StageLink(pub StageId, pub StageId); // (from, to)
