// pop_aggregation

use crate::{includes::*, task::Task, pop::POPProps};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {}

impl Aggregation {
    pub fn next(&self, _: &mut Task, _props: &POPProps) -> Result<ChunkBox, String> {
        todo!()
    }
}
