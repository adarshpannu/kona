// pop_hashjoin

use crate::{includes::*, task::Task, pop::POPProps};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {}

impl HashJoin {
    pub fn next(&self, _: &mut Task, _props: &POPProps) -> Result<ChunkBox, String> {
        todo!()
    }
}
