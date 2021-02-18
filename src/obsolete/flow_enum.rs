#![allow(warnings)]

use crate::consts::*;
use typed_arena::Arena;

type node_id = usize;
type col_id = usize;

#[derive(Debug)]
struct Flow {
    nodes: Vec<Node>,
}

impl Flow {
    fn get_node(&self, node_id: node_id) -> &Node {
        &self.nodes[node_id]
    }
}

#[derive(Debug)]
struct ProjectNode {
    cols: Vec<col_id>,
}

#[derive(Debug)]
struct UnionNode {}

#[derive(Debug)]
struct CSVNode {
    name: String,
}

/***************************************************************************************************/
#[derive(Debug, Clone, Copy)]
enum AggType {
    COUNT,
    MIN,
    MAX,
    //AVG,
    //SUM,
}

#[derive(Debug)]
struct AggNode {
    keycolids: Vec<col_id>,
    aggcolids: Vec<(AggType, col_id)>,
}

impl CSVNode {
    fn new(arena: &Arena<Node>, name: String) -> &Node {
        let base = NodeBase::CSVNode(CSVNode { name });
        let node_id = arena.len();

        arena.alloc(Node {
            node_id,
            base,
            source: vec![],
        })
    }
}

#[derive(Debug)]
enum NodeBase {
    CSVNode(CSVNode),
    ProjectNode(ProjectNode),
    UnionNode(UnionNode),
    AggNode(AggNode),
}

impl NodeBase {
    fn open(&mut self) {
        
    }
}

#[derive(Debug)]
struct Node {
    node_id: node_id,
    base: NodeBase,
    source: Vec<node_id>,
}

impl Node {
    fn project<'a>(
        &self, arena: &'a Arena<Node>, cols: Vec<col_id>,
    ) -> &'a Node {
        let base = NodeBase::ProjectNode(ProjectNode { cols });
        let node_id = arena.len();

        arena.alloc(Node {
            node_id,
            base,
            source: vec![self.node_id],
        })
    }

    fn agg<'a>(
        &self, arena: &'a Arena<Node>, keycolids: Vec<col_id>,
        aggcolids: Vec<(AggType, col_id)>,
    ) -> &'a Node {
        let base = NodeBase::AggNode(AggNode {
            keycolids,
            aggcolids,
        });
        let node_id = arena.len();

        arena.alloc(Node {
            node_id,
            base,
            source: vec![self.node_id],
        })
    }

    fn union<'a>(
        &self, arena: &'a Arena<Node>, sources: Vec<&Node>,
    ) -> &'a Node {
        let base = NodeBase::UnionNode(UnionNode {});
        let node_id = arena.len();
        let mut source: Vec<_> = sources.iter().map(|e| e.node_id).collect();
        source.push(self.node_id);

        arena.alloc(Node {
            node_id,
            base,
            source,
        })
    }

    fn name(&self) -> String {
        let nodetype = match &self.base {
            NodeBase::CSVNode(_) => "CSVNode",
            NodeBase::ProjectNode(_) => "ProjectNode",
            NodeBase::UnionNode(_) => "UnionNode",
            NodeBase::AggNode(_) => "AggNode",
        };
        format!("{}-{}|{}", nodetype, self.node_id, self.node_id)
    }
}

fn make_test_flow() -> Flow {
    let arena: Arena<_> = Arena::new();

    /*
                       C ->
                    /      \
        A -> B ->              -> E
                            /
                    \  D ->
    */
    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string())
        .project(&arena, vec![0, 1, 2]);
    let c = ab.project(&arena, vec![0]);
    let d = ab.project(&arena, vec![1]);
    let e = c.union(&arena, vec![d]).agg(
        &arena,
        vec![0],
        vec![(AggType::COUNT, 1)],
    );

    Flow {
        nodes: arena.into_vec(),
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
            format!("    Node{}[label=\"{}\"];\n", node.node_id, node.name());
        file.write_all(nodestr.as_bytes())?;

        for source in node.source.iter() {
            let edge = format!("    Node{} -> Node{};\n", source, node.node_id);
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
fn test() {
    let flow = make_mvp_flow();

    let gvfilename = format!("{}/{}", DATADIR, "flow.dot");

    write_flow_to_graphviz(&flow, &gvfilename, true)
        .expect("Cannot write to .dot file.");
}

fn run(flow: Flow, top_node: node_id) {
    let node = flow.get_node(top_node);
}
