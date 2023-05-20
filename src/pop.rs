// pop: Physical operators

use std::collections::HashMap;

pub use crate::{pop_csv::*, pop_repartition::*, pop_hashjoin::*, pop_aggregation::*};
pub use crate::{bitset::*, expr::*, flow::*, graph::*, includes::*, lop::*, metadata::*, pcode::*, pcode::*, pop_csv::*, qgm::*, row::*, stage::*, task::*};

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

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ColumnPosition {
    pub column_position: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnPositionTable {
    pub hashmap: HashMap<QunCol, ColumnPosition>,
}

impl std::default::Default for ColumnPositionTable {
    fn default() -> Self {
        ColumnPositionTable::new()
    }
}

impl ColumnPositionTable {
    pub fn new() -> ColumnPositionTable {
        ColumnPositionTable { hashmap: HashMap::new() }
    }

    pub fn set(&mut self, quncol: QunCol, cp: ColumnPosition) {
        self.hashmap.insert(quncol, cp);
    }

    pub fn get(&self, quncol: QunCol) -> ColumnPosition {
        *self.hashmap.get(&quncol).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct POPProps {
    pub predicates: Option<Vec<PCode>>,
    pub emitcols: Option<Vec<PCode>>,
    pub npartitions: usize,
    pub index_in_stage: usize,
}

impl POPProps {
    pub fn new(predicates: Option<Vec<PCode>>, emitcols: Option<Vec<PCode>>, npartitions: usize) -> POPProps {
        POPProps {
            predicates,
            emitcols,
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
