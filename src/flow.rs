// flow

use crate::{includes::*, stage::StageGraph};

#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub id: usize,

    #[serde(skip)]
    pub stage_graph: StageGraph,

    pub schema: Schema,
}
