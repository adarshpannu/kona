// pop_repartition

pub use crate::includes::*;
use crate::{pcode::PCode, task::Task};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Repartition {
    pub output_map: Option<Vec<RegisterId>>,
    pub repart_key: Vec<PCode>,
}

impl Repartition {
    pub fn next(&self, _task: &mut Task) -> Result<ChunkBox, String> {
        debug!("Repartition:next():");

        todo!()
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct RepartitionRead {}

impl RepartitionRead {
    pub fn next(&self, _task: &mut Task) -> Result<ChunkBox, String> {
        debug!("RepartitionRead:next():");

        todo!()
    }
}
