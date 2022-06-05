// flow

use crate::{graph::*, includes::*, pop::*, stage::*};

#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub pop_graph: POPGraph,
    pub stage_mgr: StageManager,
}

impl Flow {
    pub fn get_node(&self, pop_key: POPKey) -> &POP {
        &self.pop_graph.get(pop_key).value
    }

    /*
    pub fn make_stages(&self) -> Vec<Stage> {
        let stages: Vec<_> = self
            .pop_graph
            .iter(self.root_pop_key)
            .filter(|&pop_key| if pop_key == self.root_pop_key { true } else { false })
            .map(|pop_key| {
                //let npartitions = props.npartitions;
                Stage::new(self, pop_key)
            })
            .collect();
        debug!("Stages: {:?}", stages);
        stages
    }
    */

    pub fn run(&self, env: &Env) {
        /* 
        let stages = self.make_stages();
        for stage in stages {
            stage.run(env, self);
        }
        */
    }
}

