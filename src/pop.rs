use crate::{csv::*, expr::*, graph::*, includes::*, lop::*, flow::*, qgm::*, row::*, task::*};
use std::fs::File;
use std::io::Write;
use std::process::Command;
use std::rc::Rc;

pub type POPGraph = Graph<POPKey, POP, POPProps>;

#[derive(Debug, Serialize, Deserialize)]
pub struct POPProps {
    npartitions: usize,
}

impl std::default::Default for POPProps {
    fn default() -> Self {
        POPProps {
            npartitions: 4, // todo
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CSVPOP {
    filename: String,
    header: bool,
    separator: char,
    partitions: Vec<TextFilePartition>,
}

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
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
pub enum NodeRuntime {
    Unused,
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoinPOP {}

#[derive(Debug, Serialize, Deserialize)]
pub enum POP {
    CSVPOP(CSVPOP),
    HashJoinPOP,
}


impl POP {
    fn is_stage_boundary(&self) -> bool {
        false
    }
}

impl Flow {
    pub fn compile(env: &Env, qgm: &mut QGM) -> Result<Flow, String> {
        if let Ok((lop_graph, lop_key)) = qgm.build_logical_plan() {
            let mut pop_graph: POPGraph = Graph::new();

            debug!("COMPILER!");
            let root_pop_key = Self::compile_lop(qgm, &lop_graph, lop_key, &mut pop_graph)?;
            let plan_filename = format!("{}/{}", GRAPHVIZDIR, "pop.dot");
            QGM::write_physical_plan_to_graphviz(qgm, &pop_graph, root_pop_key, &plan_filename);

            let flow = Flow {
                pop_graph,
                root_pop_key,
            };
            return Ok(flow);
        } else {
            todo!()
        }
    }

    pub fn compile_lop(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph,
    ) -> Result<POPKey, String> {
        let (lop, lopprops, lop_children) = lop_graph.get3(lop_key);

        // Compile children first
        let mut pop_children = vec![];
        if let Some(lop_children) = lop_children {
            for lop_child_key in lop_children {
                let pop_key = Self::compile_lop(qgm, lop_graph, *lop_child_key, pop_graph)?;
                pop_children.push(pop_key);
            }
        }

        let pop_key = match lop {
            LOP::TableScan { input_cols } => {
                let qunid = lopprops.quns.elements()[0];
                let qun = qgm.metadata.get_tabledesc(qunid).unwrap();
                let pop = CSVPOP {
                    filename: qun.filename().clone(),
                    header: qun.header(),
                    separator: qun.separator(),
                    partitions: vec![],
                };
                pop_graph.add_node(POP::CSVPOP(pop), None)
            }
            LOP::HashJoin { join_pred } => pop_graph.add_node(POP::HashJoinPOP, Some(pop_children)),
            _ => todo!(),
        };
        Ok(pop_key)
    }
}

impl QGM {
    pub fn write_physical_plan_to_graphviz(
        self: &QGM, pop_graph: &POPGraph, pop_key: POPKey, filename: &str,
    ) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        self.write_pop_to_graphviz(pop_graph, pop_key, &mut file);

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

    pub fn write_pop_to_graphviz(
        self: &QGM, pop_graph: &POPGraph, pop_key: POPKey, file: &mut File,
    ) -> std::io::Result<()> {
        let id = pop_key.printable_key();
        let (pop, props, children) = pop_graph.get3(pop_key);

        if let Some(children) = children {
            for &child_key in children.iter().rev() {
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
