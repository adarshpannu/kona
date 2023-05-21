// pop_aggregation

use crate::{includes::*, task::Task};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {}

impl Aggregation {
    pub fn next(&self, _: &mut Task) -> Result<ChunkBox, String> {
        todo!()
    }
}
