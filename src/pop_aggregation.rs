// pop_aggregation

use crate::{includes::*, pop::Agg};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub aggs: Vec<(Agg, ColId)>
}

impl Aggregation {}
