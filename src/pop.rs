// LOP: Physical operators
#![allow(unused_variables)]

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::process::Command;
use std::rc::Rc;

pub use crate::{bitset::*, csv::*, expr::*, flow::*, graph::*, includes::*, lop::*, metadata::*, pcode::*, pcode::*, qgm::*, row::*, task::*};

pub type POPGraph = Graph<POPKey, POP, POPProps>;

#[derive(Debug)]
pub struct Stage {
    root_lop_key: LOPKey,
    register_allocator: RegisterAllocator,
}

impl Stage {
    pub fn new(root_lop_key: LOPKey) -> Stage {
        debug!("New stage with root_lop_key: {:?}", root_lop_key);

        Stage {
            root_lop_key,
            register_allocator: RegisterAllocator::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct POPProps {
    pub predicates: Option<Vec<PCode>>,
    pub emit_exprs: Option<Vec<PCode>>,
    pub npartitions: usize,
}

impl POPProps {
    pub fn new(predicates: Option<Vec<PCode>>, emit_exprs: Option<Vec<PCode>>, npartitions: usize) -> POPProps {
        POPProps {
            predicates,
            emit_exprs,
            npartitions,
        }
    }
}

impl POPKey {
    pub fn printable_key(&self) -> String {
        format!("{:?}", *self).replace("(", "").replace(")", "")
    }

    pub fn printable(&self, pop_graph: &POPGraph) -> String {
        let pop = &pop_graph.get(*self).value;
        format!("{:?}-{:?}", *pop, *self)
    }
}
/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum POP {
    CSV(CSV),
    CSVDir(CSVDir),
    HashJoin,
    Repartition { output_map: Option<Vec<RegisterId>> },
    Aggregation,
}

impl POP {
    pub fn is_stage_root(&self) -> bool {
        matches!(self, POP::Repartition { .. })
    }
}

impl POPKey {
    pub fn next(&self, flow: &Flow, stage: &OldStage, task: &mut Task, is_head: bool) -> Result<bool, String> {
        let (pop, props, ..) = flow.pop_graph.get3(*self);

        loop {
            let got_row = match pop {
                POP::CSV(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
                POP::CSVDir(inner_node) => inner_node.next(*self, flow, stage, task, is_head)?,
                _ => unimplemented!(),
            };

            // Run predicates and emits, if any
            if got_row {
                let row_passed = Self::eval_predicates(props, &task.task_row);
                if row_passed {
                    Self::eval_emit_exprs(props, &task.task_row);
                }
                return Ok(true);
            } else {
                // No more rows to drain
                return Ok(false);
            }
        }
    }

    pub fn eval_predicates(props: &POPProps, registers: &Row) -> bool {
        if let Some(preds) = props.predicates.as_ref() {
            for pred in preds.iter() {
                let result = pred.eval(&registers);
                if let Datum::BOOL(b) = result {
                    if !b {
                        return false; // short circuit
                    }
                } else {
                    panic!("No bool?")
                }
            }
        }
        return true;
    }

    pub fn eval_emit_exprs(props: &POPProps, registers: &Row) {
        if let Some(emit_exprs) = props.emit_exprs.as_ref() {
            let emit_output = emit_exprs
                .iter()
                .map(|emit| {
                    let result = emit.eval(&registers);
                    result
                })
                .collect::<Vec<_>>();
            debug!("Emitted: {:?}", emit_output);
        }
    }
}

/***************************************************************************************************/

#[derive(Serialize, Deserialize)]
pub struct CSV {
    filename: String,
    coltypes: Vec<DataType>,
    header: bool,
    separator: char,
    partitions: Vec<TextFilePartition>,
    input_map: HashMap<ColId, RegisterId>,
}

impl CSV {
    fn new(filename: String, coltypes: Vec<DataType>, header: bool, separator: char, npartitions: usize, input_map: HashMap<ColId, RegisterId>) -> CSV {
        let partitions = compute_partitions(&filename, npartitions as u64).unwrap();

        CSV {
            filename,
            coltypes,
            header,
            separator,
            partitions,
            input_map,
        }
    }

    fn next(&self, pop_key: POPKey, flow: &Flow, stage: &OldStage, task: &mut Task, is_head: bool) -> Result<bool, String> {
        let partition_id = task.partition_id;
        let runtime = task.contexts.entry(pop_key).or_insert_with(|| {
            let partition = &self.partitions[partition_id];
            let mut iter = CSVPartitionIter::new(&self.filename, partition).unwrap();
            if partition_id == 0 {
                iter.next(); // Consume the header row (fix: check if header exists though)
            }
            NodeRuntime::CSV { iter }
        });

        if let NodeRuntime::CSV { iter } = runtime {
            if let Some(line) = iter.next() {
                // debug!("line = :{}:", &line.trim_end());
                line.trim_end()
                    .split(self.separator)
                    .enumerate()
                    .filter(|(ix, col)| self.input_map.get(ix).is_some())
                    .for_each(|(ix, col)| {
                        let ttuple_ix = *self.input_map.get(&ix).unwrap();
                        let datum = match self.coltypes[ix] {
                            DataType::INT => {
                                let ival = col.parse::<isize>();
                                if ival.is_err() {
                                    panic!("{} is not an INT", &col);
                                } else {
                                    Datum::INT(ival.unwrap())
                                }
                            }
                            DataType::STR => Datum::STR(Rc::new(col.to_owned())),
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
    }
}

impl fmt::Debug for CSV {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let filename = self.filename.split("/").last().unwrap();
        //fmt.debug_struct("").field("file", &filename).field("input_map", &self.input_map).finish()
        fmt.debug_struct("").field("file", &filename).finish()
    }
}

/***************************************************************************************************/

#[derive(Serialize, Deserialize)]
pub struct CSVDir {
    dirname_prefix: String, // E.g.: $TEMPDIR/flow-99/stage  i.e. everything except the "-{partition#}"
    coltypes: Vec<DataType>,
    header: bool,
    separator: char,
    npartitions: usize,
    input_map: HashMap<ColId, RegisterId>,
}

impl fmt::Debug for CSVDir {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let dirname = self.dirname_prefix.split("/").last().unwrap();
        //fmt.debug_struct("").field("file", &filename).field("input_map", &self.input_map).finish()
        fmt.debug_struct("").field("dir", &dirname).finish()
    }
}

impl CSVDir {
    fn new(dirname_prefix: String, coltypes: Vec<DataType>, header: bool, separator: char, npartitions: usize, input_map: HashMap<ColId, RegisterId>) -> Self {
        CSVDir {
            dirname_prefix,
            coltypes,
            header,
            separator,
            npartitions,
            input_map,
        }
    }

    fn next(&self, pop_key: POPKey, flow: &Flow, stage: &OldStage, task: &mut Task, is_head: bool) -> Result<bool, String> {
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
                    .filter(|(ix, col)| self.input_map.get(ix).is_some())
                    .for_each(|(ix, col)| {
                        let ttuple_ix = *self.input_map.get(&ix).unwrap();
                        let datum = match self.coltypes[ix] {
                            DataType::INT => {
                                let ival = col.parse::<isize>();
                                if ival.is_err() {
                                    panic!("{} is not an INT", &col);
                                } else {
                                    Datum::INT(ival.unwrap())
                                }
                            }
                            DataType::STR => Datum::STR(Rc::new(col.to_owned())),
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
    }
}

/***************************************************************************************************/
pub enum NodeRuntime {
    Unused,
    CSV { iter: CSVPartitionIter },
    CSVDir { iter: CSVDirIter },
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoinPOP {}

impl Flow {
    pub fn compile(env: &Env, qgm: &mut QGM) -> Result<Flow, String> {
        let (lop_graph, lop_key) = qgm.build_logical_plan(env)?;
        let mut pop_graph: POPGraph = Graph::new();

        let mut root_stage = Stage::new(lop_key);

        let root_pop_key = Self::compile_lop(qgm, &lop_graph, lop_key, &mut pop_graph, &mut root_stage)?;

        debug!("Root stage {:?}", root_stage);

        let plan_filename = format!("{}/{}", env.output_dir, "pop.dot");
        QGM::write_physical_plan_to_graphviz(qgm, &pop_graph, root_pop_key, &plan_filename)?;

        let flow = Flow { pop_graph, root_pop_key };
        return Ok(flow);
    }

    pub fn compile_lop(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, stage: &mut Stage) -> Result<POPKey, String> {
        let (lop, lopprops, lop_children) = lop_graph.get3(lop_key);

        let child_stage;
        let mut child_stage = if matches!(lop, LOP::Repartition { .. }) {
            child_stage = Stage::new(lop_key);
            Some(child_stage)
        } else {
            None
        };

        // Compile children first
        let mut pop_children = vec![];
        if let Some(lop_children) = lop_children {
            for lop_child_key in lop_children {
                let pop_key = Self::compile_lop(qgm, lop_graph, *lop_child_key, pop_graph, child_stage.as_mut().unwrap_or(stage))?;
                pop_children.push(pop_key);
            }
        }

        if let Some(child_stage) = child_stage.as_ref() {
            debug!("Stage {:?}", child_stage);
        }

        let npartitions = lopprops.partdesc.npartitions;

        let pop_key = match lop {
            LOP::TableScan { input_cols } => Self::compile_scan(qgm, lop_graph, lop_key, pop_graph, stage)?,
            LOP::HashJoin { equi_join_preds } => Self::compile_join(qgm, lop_graph, lop_key, pop_graph, pop_children, stage)?,
            LOP::Repartition { cpartitions } => {
                Self::compile_repartition(qgm, lop_graph, lop_key, pop_graph, pop_children, stage, child_stage.as_mut().unwrap())?
            }
            LOP::Aggregation { .. } => {
                let props = POPProps::new(None, None, npartitions);
                pop_graph.add_node_with_props(POP::Aggregation, props, Some(pop_children))
                //Self::compile_aggregation(qgm, lop_graph, lop_key, pop_graph, pop_children, stage)?
            }
        };
        Ok(pop_key)
    }

    pub fn compile_repartition(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>, stage: &mut Stage, child_stage: &mut Stage,
    ) -> Result<POPKey, String> {
        // Repartition split into Repartition + CSVDirScan
        let (lop, lopprops, ..) = lop_graph.get3(lop_key);

        // We shouldn't have any predicates
        let predicates = None;
        assert!(lopprops.preds.len() == 0);

        // Compile cols or emit_exprs. We will have one or the other
        let emit_exprs = Self::compile_emit_exprs(qgm, lopprops.emit_exprs.as_ref(), &mut child_stage.register_allocator);

        let output_map: Option<Vec<RegisterId>> = if emit_exprs.is_none() {
            let output_map = lopprops
                .cols
                .elements()
                .iter()
                .map(|&quncol| {
                    let regid = stage.register_allocator.get_id(quncol);
                    regid
                })
                .collect();
            Some(output_map)
        } else {
            None
        };

        let props = POPProps::new(predicates, emit_exprs, lopprops.partdesc.npartitions);

        let pop_key = pop_graph.add_node_with_props(POP::Repartition { output_map }, props, Some(pop_children));

        Ok(pop_key)
    }

    pub fn compile_join(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>, stage: &mut Stage,
    ) -> Result<POPKey, String> {
        let (lop, lopprops, ..) = lop_graph.get3(lop_key);

        // Compile predicates
        //debug!("Compile predicate for lopkey: {:?}", lop_key);
        let predicates = Self::compile_predicates(qgm, &lopprops.preds, &mut stage.register_allocator);

        // Compile emit_exprs
        //debug!("Compile emits for lopkey: {:?}", lop_key);
        let emit_exprs = Self::compile_emit_exprs(qgm, lopprops.emit_exprs.as_ref(), &mut stage.register_allocator);

        let props = POPProps::new(predicates, emit_exprs, lopprops.partdesc.npartitions);

        let pop_key = pop_graph.add_node_with_props(POP::HashJoin, props, Some(pop_children));

        Ok(pop_key)
    }

    pub fn compile_aggregation(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>, stage: &mut Stage,
    ) -> Result<POPKey, String> {
        let (lop, lopprops, ..) = lop_graph.get3(lop_key);

        // Compile predicates
        debug!("Compile predicate for lopkey: {:?}", lop_key);
        let predicates = Self::compile_predicates(qgm, &lopprops.preds, &mut stage.register_allocator);

        // Compile emit_exprs
        debug!("Compile emits for lopkey: {:?}", lop_key);
        let emit_exprs = Self::compile_emit_exprs(qgm, lopprops.emit_exprs.as_ref(), &mut stage.register_allocator);

        let props = POPProps::new(predicates, emit_exprs, lopprops.partdesc.npartitions);

        let pop_key = pop_graph.add_node_with_props(POP::Aggregation, props, Some(pop_children));

        Ok(pop_key)
    }

    pub fn compile_scan(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, stage: &mut Stage) -> Result<POPKey, String> {
        let (lop, lopprops, ..) = lop_graph.get3(lop_key);

        let qunid = lopprops.quns.elements()[0];
        let tbldesc = qgm.metadata.get_tabledesc(qunid).unwrap();
        let columns = tbldesc.columns();

        let coltypes = columns.iter().map(|col| col.datatype).collect();

        // Build input map
        let input_map: HashMap<ColId, RegisterId> = if let LOP::TableScan { input_cols } = lop {
            input_cols
                .elements()
                .iter()
                .map(|&quncol| {
                    let regid = stage.register_allocator.get_id(quncol);
                    (quncol.1, regid)
                })
                .collect()
        } else {
            return Err(format!("Internal error: compile_scan() received a POP that isn't a TableScan"));
        };

        // Compile predicates
        //debug!("Compile predicate for lopkey: {:?}", lop_key);

        let predicates = Self::compile_predicates(qgm, &lopprops.preds, &mut stage.register_allocator);

        let pop = match tbldesc.get_type() {
            TableType::CSV => {
                let inner = CSV::new(
                    tbldesc.pathname().clone(),
                    coltypes,
                    tbldesc.header(),
                    tbldesc.separator(),
                    lopprops.partdesc.npartitions,
                    input_map,
                );
                POP::CSV(inner)
            }
            TableType::CSVDIR => {
                let inner = CSVDir::new(
                    tbldesc.pathname().clone(),
                    coltypes,
                    tbldesc.header(),
                    tbldesc.separator(),
                    lopprops.partdesc.npartitions,
                    input_map,
                );
                POP::CSVDir(inner)
            }
        };

        // Compile emit_exprs
        //debug!("Compile emits for lopkey: {:?}", lop_key);
        let emit_exprs = Self::compile_emit_exprs(qgm, lopprops.emit_exprs.as_ref(), &mut stage.register_allocator);

        let props = POPProps::new(predicates, emit_exprs, lopprops.partdesc.npartitions);

        let pop_key = pop_graph.add_node_with_props(pop, props, None);
        Ok(pop_key)
    }

    pub fn compile_predicates(qgm: &QGM, preds: &Bitset<ExprKey>, register_allocator: &mut RegisterAllocator) -> Option<Vec<PCode>> {
        let mut pcodevec = vec![];
        if preds.len() > 0 {
            for pred_key in preds.elements().iter() {
                let mut pcode = PCode::new();
                pred_key.compile(&qgm.expr_graph, &mut pcode, register_allocator);
                pcodevec.push(pcode);
            }
            Some(pcodevec)
        } else {
            None
        }
    }

    pub fn compile_emit_exprs(qgm: &QGM, emit_exprs: Option<&Vec<EmitExpr>>, register_allocator: &mut RegisterAllocator) -> Option<Vec<PCode>> {
        if let Some(emit_exprs) = emit_exprs {
            let pcode = PCode::new();
            let pcodevec = emit_exprs
                .iter()
                .map(|ne| {
                    let mut pcode = PCode::new();
                    ne.expr_key.compile(&qgm.expr_graph, &mut pcode, register_allocator);
                    pcode
                })
                .collect::<Vec<_>>();
            Some(pcodevec)
        } else {
            None
        }
    }
}

impl QGM {
    pub fn write_physical_plan_to_graphviz(self: &QGM, pop_graph: &POPGraph, pop_key: POPKey, filename: &str) -> Result<(), String> {
        let mut file = std::fs::File::create(filename).map_err(|err| f!("{:?}: {}", err, filename))?;

        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        self.write_pop_to_graphviz(pop_graph, pop_key, &mut file)?;

        fprint!(file, "}}\n");

        drop(file);

        let ofilename = format!("{}.jpg", filename);
        let oflag = format!("-o{}.jpg", filename);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(filename)
            .status()
            .expect("failed to execute process");

        Ok(())
    }

    pub fn write_pop_to_graphviz(self: &QGM, pop_graph: &POPGraph, pop_key: POPKey, file: &mut File) -> Result<(), String> {
        let id = pop_key.printable_key();
        let (pop, props, children) = pop_graph.get3(pop_key);

        if let Some(children) = children {
            for &child_key in children.iter() {
                let child_name = child_key.printable_key();
                fprint!(file, "    popkey{} -> popkey{};\n", child_name, id);
                self.write_pop_to_graphviz(pop_graph, child_key, file)?;
            }
        }

        let (label, extrastr) = match &pop {
            POP::CSV(csv) => {
                let filename = csv.filename.split("/").last().unwrap_or(&csv.filename);
                let mut input_map = csv.input_map.iter().collect::<Vec<_>>();
                input_map.sort_by(|a, b| a.cmp(b));
                let extrastr = format!("file: {}, map: {:?}", filename, input_map).replace("{", "(").replace("}", ")");
                (String::from("CSV"), extrastr)
            }
            POP::CSVDir(csvdir) => {
                let dirname = csvdir.dirname_prefix.split("/").last().unwrap_or(&csvdir.dirname_prefix);
                let mut input_map = csvdir.input_map.iter().collect::<Vec<_>>();
                input_map.sort_by(|a, b| a.cmp(b));
                let extrastr = format!("file: {}, map: {:?}", dirname, input_map).replace("{", "(").replace("}", ")");
                (String::from("CSVDir"), extrastr)
            }
            POP::HashJoin { .. } => {
                let extrastr = format!("");
                (String::from("HashJoin"), extrastr)
            }
            POP::Repartition { output_map } => {
                let extrastr = format!("output_map = {:?}", output_map);
                (String::from("Repartition"), extrastr)
            }
            POP::Aggregation => {
                let extrastr = format!("");
                (String::from("Aggregation"), extrastr)
            }
        };

        let label = label.replace("\"", "").replace("{", "").replace("}", "");
        fprint!(file, "    popkey{}[label=\"{}|p = {}|{}\"];\n", id, label, props.npartitions, extrastr);

        Ok(())
    }
}

use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterAllocator {
    pub hashmap: HashMap<QunCol, RegisterId>,
    next_id: RegisterId,
}

impl RegisterAllocator {
    pub fn new() -> RegisterAllocator {
        RegisterAllocator {
            hashmap: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn get_id(&mut self, quncol: QunCol) -> RegisterId {
        let next_id = self.next_id;
        let e = self.hashmap.entry(quncol).or_insert(next_id);
        if *e == next_id {
            self.next_id = next_id + 1;
        }
        //debug!("Assigned {:?} -> {}", &quncol, *e);
        *e
    }
}
