// pop_hashjoin

pub use crate::{includes::*, pop::*};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {}

impl HashJoin {
    pub fn next(&self, _: &mut Task) -> Result<ChunkBox, String> {
        todo!()
    }
}
