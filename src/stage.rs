// stage

use crate::includes::*;
use crate::pop::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Stage {
    stage_id: StageId,
    parent_stage_id: Option<StageId>, // 0 == no stage depends on this
    orig_child_count: usize,
    active_child_count: usize,
    root_lop_key: LOPKey,
    pub root_pop_key: Option<POPKey>,
    pub register_allocator: RegisterAllocator,
}

/***************************************************************************************************/
impl Stage {
    pub fn new(stage_id: usize, parent_stage_id: Option<usize>, root_lop_key: LOPKey) -> Self {
        debug!("New stage with root_lop_key: {:?}", root_lop_key);
        Stage {
            stage_id,
            parent_stage_id,
            orig_child_count: 0,
            active_child_count: 0,
            root_lop_key,
            root_pop_key: None,
            register_allocator: RegisterAllocator::new(),
        }
    }

    pub fn run(&self, env: &Env, flow: &Flow) {
        let (_, props, ..) = flow.pop_graph.get3(self.root_pop_key.unwrap());
        let npartitions = props.npartitions;
        for partition_id in 0..npartitions {
            let task = Task::new(partition_id);
            //task.run(flow, self);

            let thread_id = partition_id % (env.thread_pool.size());

            //let t2sa = Task2SendAcross { flow: flow.clone() };
            let t2sa = &(flow, self, task);
            let encoded: Vec<u8> = bincode::serialize(&t2sa).unwrap();
            debug!("Serialized task len = {}", encoded.len());

            let _decoded: (Flow, Stage, Task) = bincode::deserialize(&encoded[..]).unwrap();

            //dbg!(&decoded.0);

            env.thread_pool.s2t_channels_sx[thread_id].send(ThreadPoolMessage::RunTask(encoded)).unwrap();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StageManager {
    status_vec: Vec<Stage>, // Stage hierarchy encoded in this array
}

impl StageManager {
    pub fn new() -> Self {
        let status_vec = vec![]; // zeroth entry is not used
        StageManager { status_vec }
    }

    pub fn add_stage(&mut self, root_lop_key: LOPKey, parent_stage_id: Option<usize>) -> usize {
        // `parent_stage_id` must exist, and it will become dependent on newly created stage
        // newly created stage goes at the end of the vector, and its `id` is essentially its index
        let new_id = self.status_vec.len();

        let new_ss = Stage::new(new_id, parent_stage_id, root_lop_key);
        self.status_vec.push(new_ss);

        if let Some(parent_stage_id) = parent_stage_id {
            assert!(parent_stage_id <= self.status_vec.len());

            let parent_stage = &mut self.status_vec[parent_stage_id];
            parent_stage.orig_child_count = parent_stage.orig_child_count + 1;
            parent_stage.active_child_count = parent_stage.active_child_count + 1;
        }

        debug!("Added new stage: {:?}", new_id);
        new_id
    }

    pub fn get_register_allocator(&mut self, stage_id: StageId) -> &mut RegisterAllocator {
        let stage = &mut self.status_vec[stage_id];
        &mut stage.register_allocator
    }

    pub fn set_pop_key(&mut self, stage_id: StageId, pop_key: POPKey) {
        let stage = &mut self.status_vec[stage_id];
        stage.root_pop_key = Some(pop_key)
    }

    pub fn print(&self) {
        for stage in self.status_vec.iter() {
            debug!("--- Stage: {:?}", stage)
        }
    }
}
