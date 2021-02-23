#![allow(warnings)]

use std::cell::UnsafeCell;

use crate::expr::{Expr::*, *};
use crate::row::*;

use crate::consts::*;
use typed_arena::Arena;

type node_id = usize;
type col_id = usize;

type NodeArena = Arena<Box<dyn Node>>;

trait Node {
    fn next(&mut self) -> Option<Row> {
        None
    }

    fn project(
        &self, arena: &NodeArena, colids: Vec<col_id>,
    ) -> Box<dyn Node> {
        let base = NodeBase::new1(&arena, self.id());
        Box::new(ProjectNode { base, colids })
    }

    fn union(
        &self, arena: &NodeArena, other_sources: Vec<&Box<dyn Node>>,
    ) -> Box<dyn Node> {
        let base = NodeBase::new(&arena, self.id(), other_sources);
        Box::new(UnionNode { base })
    }

    fn agg(
        &self, arena: &NodeArena, keycolids: Vec<col_id>,
        aggcolids: Vec<(AggType, col_id)>,
    ) -> Box<dyn Node> {
        let base = NodeBase::new1(&arena, self.id());
        Box::new(AggNode {
            base,
            keycolids,
            aggcolids,
        })
    }

    fn base(&self) -> &NodeBase;

    fn id(&self) -> node_id {
        self.base().id
    }

    fn name(&self) -> String {
        "name tbd".to_string()
    }

    fn sources(&self) -> &Vec<node_id> {
        &self.base().sources
    }
}

#[derive(Debug)]
struct NodeBase {
    id: node_id,
    sources: Vec<node_id>,
}

impl NodeBase {
    fn new0(arena: &NodeArena) -> NodeBase {
        NodeBase {
            id: arena.len(),
            sources: vec![],
        }
    }

    fn new1(arena: &NodeArena, source_id: node_id) -> NodeBase {
        NodeBase {
            id: arena.len(),
            sources: vec![source_id],
        }
    }

    fn new(
        arena: &NodeArena, source_id: node_id,
        other_sources: Vec<&Box<dyn Node>>,
    ) -> NodeBase {
        let node_id = arena.len();
        let mut sources: Vec<_> =
            other_sources.iter().map(|e| e.id()).collect();
        sources.push(source_id);
        NodeBase {
            id: arena.len(),
            sources,
        }
    }
}

struct CSVNode {
    base: NodeBase,
    filename: String,
}

impl CSVNode {
    fn new(arena: &NodeArena, filename: String) -> Box<dyn Node> {
        let base = NodeBase {
            id: arena.len(),
            sources: vec![],
        };
        Box::new(CSVNode { base, filename })
    }
}

impl Node for CSVNode {
    fn base(&self) -> &NodeBase {
        &self.base
    }
}

#[derive(Debug)]
struct ProjectNode {
    base: NodeBase,
    colids: Vec<col_id>,
}

impl Node for ProjectNode {
    fn base(&self) -> &NodeBase {
        &self.base
    }
}

impl ProjectNode {}

struct UnionNode {
    base: NodeBase,
}

impl Node for UnionNode {
    fn base(&self) -> &NodeBase {
        &self.base
    }
}

impl UnionNode {}

#[derive(Debug)]
struct AggNode {
    base: NodeBase,
    keycolids: Vec<col_id>,
    aggcolids: Vec<(AggType, col_id)>,
}

impl Node for AggNode {
    fn base(&self) -> &NodeBase {
        &self.base
    }
}

impl AggNode {}

#[derive(Debug, Clone, Copy)]
enum AggType {
    COUNT,
    MIN,
    MAX,
    //AVG,
    //SUM,
}

#[test]
fn test() {
    let arena: NodeArena = Arena::new();
    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string())
        .project(&arena, vec![0, 1, 2]);
    let c = ab.project(&arena, vec![0]);
    let d = ab.project(&arena, vec![1]);
    let e = c.union(&arena, vec![&d]).agg(
        &arena,
        vec![0],
        vec![(AggType::COUNT, 1)],
    );
}

struct Flow {
    nodes: Vec<Box<dyn Node>>,
}

impl Flow {
    fn get_node(&self, node_id: node_id) -> &Box<dyn Node> {
        &self.nodes[node_id]
    }
}

fn make_mvp_flow() -> Flow {
    let arena: Arena<_> = Arena::new();

    /*
        CSV -> Project -> Agg
    */
    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string())
        .project(&arena, vec![0, 1, 2])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 1)]);
    
    Flow {
        nodes: arena.into_vec(),
    }
}

use std::io::Write;
use std::process::Command;

fn write_flow_to_graphviz(
    flow: &Flow, filename: &str, open_jpg: bool,
) -> std::io::Result<()> {
    let mut file = std::fs::File::create(filename)?;
    file.write_all("digraph example1 {\n".as_bytes())?;
    file.write_all("    node [shape=record];\n".as_bytes())?;
    file.write_all("    rankdir=LR;\n".as_bytes())?; // direction of DAG
    file.write_all("    splines=polyline;\n".as_bytes())?;
    file.write_all("    nodesep=0.5;\n".as_bytes())?;

    for node in flow.nodes.iter() {
        let nodestr =
            format!("    Node{}[label=\"{}\"];\n", node.id(), node.name());
        file.write_all(nodestr.as_bytes())?;

        for source in node.sources().iter() {
            let edge = format!("    Node{} -> Node{};\n", source, node.id());
            file.write_all(edge.as_bytes())?;
        }
    }
    file.write_all("}\n".as_bytes())?;
    drop(file);

    let ofilename = format!("{}.jpg", filename);
    let oflag = format!("-o{}.jpg", filename);

    // dot -Tjpg -oex.jpg exampl1.dot
    let cmd = Command::new("dot")
        .arg("-Tjpg")
        .arg(oflag)
        .arg(filename)
        .status()
        .expect("failed to execute process");

    if open_jpg {
        let cmd = Command::new("open")
            .arg(ofilename)
            .status()
            .expect("failed to execute process");
    }

    Ok(())
}

#[test]
fn test2() {
    let flow = make_mvp_flow();

    let gvfilename = format!("{}/{}", DATADIR, "flow.dot");

    write_flow_to_graphviz(&flow, &gvfilename, true)
        .expect("Cannot write to .dot file.");
}
