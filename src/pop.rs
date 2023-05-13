// pop: Physical operators

#![allow(unused_variables)]

use std::collections::HashMap;
use std::fmt;

pub use crate::{bitset::*, csv::*, expr::*, flow::*, graph::*, includes::*, lop::*, metadata::*, pcode::*, pcode::*, qgm::*, row::*, stage::*, task::*};

pub type POPGraph = Graph<POPKey, POP, POPProps>;
use arrow2::array::BooleanArray;
use arrow2::compute::filter::filter_chunk;

/***************************************************************************************************/
pub enum NodeRuntime {
    Uninitialized,
    CSVRuntime { iter: CSVPartitionIter },
    CSVDirRuntime { iter: CSVDirIter },
}

impl std::default::Default for NodeRuntime {
    fn default() -> Self {
        NodeRuntime::Uninitialized
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
    Aggregation(Aggregation),
}

impl POP {
    pub fn is_stage_root(&self) -> bool {
        matches!(self, POP::Repartition { .. })
    }
}

impl POPKey {
    pub fn next(&self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Result<ChunkBox, String> {
        let (pop, props, ..) = flow.pop_graph.get3(*self);

        loop {
            let mut chunk = match pop {
                POP::CSV(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
                POP::CSVDir(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
                POP::Repartition(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
                POP::HashJoin(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
                POP::Aggregation(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
            };

            debug!("Before preds: {:?}", &chunk);

            if chunk.len() > 0 {
                // Run predicates and emits, if any
                chunk = Self::eval_predicates(props, chunk);
                debug!("After preds: {:?}", &chunk);

                //chunk = Self::eval_emitcols(props, out_chunk);
            }
            return Ok(chunk);
        }
    }

    pub fn eval_predicates(props: &POPProps, input: ChunkBox) -> ChunkBox {
        let mut filtered_chunk = input;
        if let Some(preds) = props.predicates.as_ref() {
            for pred in preds.iter() {
                let bool_chunk = pred.eval(&filtered_chunk);
                let bool_array = bool_chunk[0].as_any().downcast_ref::<BooleanArray>().unwrap();

                filtered_chunk = filter_chunk(&filtered_chunk, &bool_array).unwrap();
            }
        }
        return filtered_chunk;
    }

    /*
    pub fn eval_emitcols(props: &POPProps, registers: &Row) -> Option<Row> {
        if let Some(emitcols) = props.emitcols.as_ref() {
            let emit_output = emitcols
                .iter()
                .map(|emit| {
                    let result = emit.eval(&registers);
                    result
                })
                .collect::<Vec<_>>();
            Some(Row::from(emit_output))
        } else {
            None
        }
    }
    */
}
/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Repartition {
    pub output_map: Option<Vec<RegisterId>>,
    pub repart_key: Vec<PCode>,
}

impl Repartition {
    fn next(&self, pop_key: POPKey, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Result<ChunkBox, String> {
        debug!("Repartition:next(): {:?}, is_head: {}", pop_key, is_head);

        todo!()
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {}

impl HashJoin {
    fn next(&self, pop_key: POPKey, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Result<ChunkBox, String> {
        todo!()
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {}

impl Aggregation {
    fn next(&self, pop_key: POPKey, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Result<ChunkBox, String> {
        todo!()
    }
}

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct CSV {
    pub pathname: String,
    pub fields: Vec<Field>,
    pub header: bool,
    pub separator: char,
    pub partitions: Vec<TextFilePartition>,
    pub projection: Vec<ColId>,
}

impl CSV {
    pub fn new(pathname: String, fields: Vec<Field>, header: bool, separator: char, npartitions: usize, projection: Vec<ColId>) -> CSV {
        let partitions = compute_partitions(&pathname, npartitions as u64).unwrap();

        CSV {
            pathname,
            fields,
            header,
            separator,
            partitions,
            projection,
        }
    }

    pub fn next(&self, pop_key: POPKey, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Result<Chunk<Box<dyn Array>>, String> {
        let partition_id = task.partition_id;
        let context = &mut task.contexts[0];

        let pop = &flow.pop_graph.get(pop_key).value;

        if let NodeRuntime::CSVRuntime { ref mut iter } = context {
            return iter.next().ok_or("CSV::next() failed!".to_string());
        }
        panic!("Cannot get NodeRuntime::CSV")
    }
}

impl fmt::Debug for CSV {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let pathname = self.pathname.split("/").last().unwrap();
        fmt.debug_struct("").field("file", &pathname).finish()
    }
}

/***************************************************************************************************/

#[derive(Serialize, Deserialize)]
pub struct CSVDir {
    pub dirname_prefix: String, // E.g.: $TEMPDIR/flow-99/stage  i.e. everything except the "-{partition#}"
    pub fields: Vec<Field>,
    pub header: bool,
    pub separator: char,
    pub npartitions: usize,
    pub projection: Vec<ColId>,
}

impl fmt::Debug for CSVDir {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let dirname = self.dirname_prefix.split("/").last().unwrap();
        fmt.debug_struct("").field("dir", &dirname).finish()
    }
}

impl CSVDir {
    pub fn new(dirname_prefix: String, fields: Vec<Field>, header: bool, separator: char, npartitions: usize, projection: Vec<ColId>) -> Self {
        CSVDir {
            dirname_prefix,
            fields,
            header,
            separator,
            npartitions,
            projection,
        }
    }

    pub fn next(&self, pop_key: POPKey, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Result<ChunkBox, String> {
        todo!()
        /*
        let partition_id = task.partition_id;
        let runtime = task.contexts.entry(pop_key).or_insert_with(|| {
            let full_dirname = format!("{}-{}", self.dirname_prefix, partition_id);
            let iter = CSVDirIter::new(&full_dirname).unwrap();
            NodeRuntime::CSVDir { iter }
        });

        if let NodeRuntime::CSVDir { iter } = runtime {
            if let Some(line) = iter.next() {
                // debug!("line = :{}:", &line.trim_end());
                line.trim_end()
                    .split(self.separator)
                    .enumerate()
                    .filter(|(ix, col)| self.projection.get(ix).is_some())
                    .for_each(|(ix, col)| {
                        let ttuple_ix = *self.projection.get(&ix).unwrap();
                        let datum = match self.coltypes[ix] {
                            DataType::Int64 => {
                                let ival = col.parse::<isize>();
                                if ival.is_err() {
                                    panic!("{} is not an INT", &col);
                                } else {
                                    Datum::INT(ival.unwrap())
                                }
                            }
                            DataType::Utf8 => Datum::STR(Rc::new(col.to_owned())),
                            _ => todo!(),
                        };
                        task.task_row.set_column(ttuple_ix, &datum);
                    });
                return Ok(true);
            } else {
                return Ok(false);
            }
        }
        panic!("Cannot get NodeRuntime::CSV")
        */
    }
}
