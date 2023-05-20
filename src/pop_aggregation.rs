// pop_aggregation

pub use crate::{includes::*, pop::*};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {}

impl Aggregation {
    pub fn next(&self, _: &mut Task) -> Result<ChunkBox, String> {
        todo!()
    }
}
