// pop_hashjoin

use crate::{includes::*, task::Task};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {}

impl HashJoin {
    pub fn next(&self, _: &mut Task) -> Result<ChunkBox, String> {
        todo!()
    }
}
