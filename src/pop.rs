// pop: Physical operators

use std::collections::HashMap;

use crate::{
    graph::{Graph, ExprKey, POPKey},
    includes::*,
    pcode::PCode,
    pop_aggregation::Aggregation,
    pop_csv::{CSVDir, CSVDirIter, CSVPartitionIter, CSV},
    pop_hashjoin::HashJoin,
    pop_repartition::{Repartition, RepartitionRead},
};

pub type POPGraph = Graph<POPKey, POP, POPProps>;

/***************************************************************************************************/
pub enum POPContext {
    UninitializedContext,
    CSVContext { iter: CSVPartitionIter },
    CSVDirContext { iter: CSVDirIter },
}

impl std::default::Default for POPContext {
    fn default() -> Self {
        POPContext::UninitializedContext
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Projection {
    QunCol(QunCol),
    VirtCol(ExprKey)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectionMap {
    pub hashmap: HashMap<Projection, ColId>,
}

impl std::default::Default for ProjectionMap {
    fn default() -> Self {
        ProjectionMap::new()
    }
}

impl ProjectionMap {
    pub fn new() -> ProjectionMap {
        ProjectionMap { hashmap: HashMap::new() }
    }

    pub fn set(&mut self, prj: Projection, colid: ColId) {
        self.hashmap.insert(prj, colid);
    }

    pub fn get(&self, prj: Projection) -> Option<ColId> {
        self.hashmap.get(&prj).cloned()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct POPProps {
    pub predicates: Option<Vec<PCode>>,
    pub cols: Option<Vec<ColId>>,
    pub virtcols: Option<Vec<PCode>>,
    pub npartitions: usize,
    pub index_in_stage: usize,
}

impl POPProps {
    pub fn new(predicates: Option<Vec<PCode>>, cols: Option<Vec<ColId>>, virtcols: Option<Vec<PCode>>, npartitions: usize) -> POPProps {
        POPProps {
            predicates,
            cols,
            virtcols,
            npartitions,
            index_in_stage: 0,
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum POP {
    CSV(CSV),
    CSVDir(CSVDir),
    HashJoin(HashJoin),
    Repartition(Repartition),
    RepartitionRead(RepartitionRead),
    Aggregation(Aggregation),
}

impl POP {
    pub fn is_stage_root(&self) -> bool {
        matches!(self, POP::Repartition { .. })
    }
}
