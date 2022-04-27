// LOP: Physical operators
#![allow(warnings)]

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::process::Command;

pub use crate::{bitset::*, expr::*, flow::*, graph::*, includes::*, lop::*, pcode::*, pcode::*, qgm::*, row::*};

pub type POPGraph = Graph<POPKey, POP, POPProps>;

#[derive(Debug)]
pub struct Stage {
    root_lop_key: LOPKey,
    register_allocator: RegisterAllocator,
}

impl Stage {
    pub fn new(root_lop_key: LOPKey) -> Stage {
        Stage {
            root_lop_key,
            register_allocator: RegisterAllocator::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct POPProps {
    predicates: Option<PCode>,
}

impl POPProps {
    pub fn new(predicates: Option<PCode>) -> POPProps {
        POPProps { predicates }
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
    HashJoin,
    Repartition,
    Aggregation,
}

impl POP {
    pub fn is_stage_root(&self) -> bool {
        match *self {
            POP::Repartition => true,
            _ => false,
        }
    }
}

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct CSV {
    filename: String,
    header: bool,
    separator: char,
    partitions: Vec<TextFilePartition>,
    input_map: HashMap<ColId, RegisterId>,
}

impl fmt::Debug for CSV {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let filename = self.filename.split("/").last().unwrap();
        //fmt.debug_struct("").field("file", &filename).field("input_map", &self.input_map).finish()
        fmt.debug_struct("").field("file", &filename).finish()

    }
}

/***************************************************************************************************/
pub enum NodeRuntime {
    Unused,
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

        let plan_filename = format!("{}/{}", env.output_dir, "pop.dot");
        QGM::write_physical_plan_to_graphviz(qgm, &pop_graph, root_pop_key, &plan_filename)?;

        let flow = Flow { pop_graph, root_pop_key };
        return Ok(flow);
    }

    pub fn compile_lop(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, stage: &mut Stage) -> Result<POPKey, String> {
        let (lop, lopprops, lop_children) = lop_graph.get3(lop_key);

        // Compile children first
        let mut pop_children = vec![];
        if let Some(lop_children) = lop_children {
            for lop_child_key in lop_children {
                let pop_key = Self::compile_lop(qgm, lop_graph, *lop_child_key, pop_graph, stage)?;
                pop_children.push(pop_key);
            }
        }

        let pop_key = match lop {
            LOP::TableScan { input_cols } => Self::compile_scan(qgm, lop_graph, lop_key, pop_graph, stage)?,
            LOP::HashJoin { equi_join_preds } => {
                let props = POPProps::new(None);
                pop_graph.add_node_with_props(POP::HashJoin, props, Some(pop_children))
            }
            LOP::Repartition { cpartitions } => {
                let props = POPProps::new(None);
                pop_graph.add_node_with_props(POP::Repartition, props, Some(pop_children))
            }
            LOP::Aggregation { .. } => {
                let props = POPProps::new(None);
                pop_graph.add_node_with_props(POP::Aggregation, props, Some(pop_children))
            }
        };
        Ok(pop_key)
    }

    pub fn compile_scan(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, stage: &mut Stage) -> Result<POPKey, String> {
        let (lop, lopprops, ..) = lop_graph.get3(lop_key);

        let qunid = lopprops.quns.elements()[0];
        let qun = qgm.metadata.get_tabledesc(qunid).unwrap();

        // Build input map
        let cols = &lopprops.cols;
        let input_map: HashMap<ColId, RegisterId> = cols
            .elements()
            .iter()
            .map(|&quncol| {
                let regid = stage.register_allocator.get_id(quncol);
                (quncol.1, regid)
            })
            .collect();

        // Compile predicates
        let pcode = Self::compile_predicates(qgm, &lopprops.preds, &mut stage.register_allocator);

        let pop = CSV {
            filename: qun.filename().clone(),
            header: qun.header(),
            separator: qun.separator(),
            partitions: vec![],
            input_map,
        };
        let props = POPProps { predicates: pcode };
        let pop_key = pop_graph.add_node_with_props(POP::CSV(pop), props, None);
        Ok(pop_key)
    }

    pub fn compile_predicates(qgm: &QGM, preds: &Bitset<ExprKey>, register_allocator: &mut RegisterAllocator) -> Option<PCode> {
        if preds.len() > 0 {
            let mut pcode = PCode::new();
            for (ix, pred_key) in preds.elements().iter().enumerate() {
                pred_key.compile(&qgm.expr_graph, &mut pcode, register_allocator);
                if ix > 0 {
                    pcode.push(PInstruction::ControlOp(ControlOp::ReturnIfFalse));
                }
            }
            Some(pcode)
        } else {
            None
        }
    }
}

#[macro_use]
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
        let label = format!("{:?}", &pop);
        let label = label.replace("\"", "").replace("{", "").replace("}", "");
        fprint!(file, "    popkey{}[label=\"{}\"];\n", id, label);

        Ok(())
    }
}

use std::collections::HashMap;

type RegisterId = usize;

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
