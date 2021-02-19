#![allow(warnings)]

use std::cell::UnsafeCell;

use crate::expr::{Expr::*, *};
use crate::row::*;

use crate::consts::*;
use typed_arena::Arena;

type node_id = usize;
type col_id = usize;

trait Node {
    fn next(&mut self) -> Option<Row> {
        None
    }

    fn project(
        &self, arena: &Arena<Box<dyn Node>>, colids: Vec<col_id>,
    ) -> Box<dyn Node> {
        let base = NodeBase {
            id: arena.len(),
            sources: vec![self.id()],
        };
        Box::new(ProjectNode { base, colids })
    }

    fn union(
        &self, arena: &Arena<Box<dyn Node>>, other_sources: Vec<&Box<dyn Node>>,
    ) -> Box<dyn Node> {
        let node_id = arena.len();
        let mut source: Vec<_> = other_sources.iter().map(|e| e.id()).collect();
        source.push(self.id());
        let base = NodeBase {
            id: arena.len(),
            sources: vec![self.id()],
        };
        Box::new(UnionNode { base })
    }

    fn agg(
        &self, arena: &Arena<Box<dyn Node>>, keycolids: Vec<col_id>,
        aggcolids: Vec<(AggType, col_id)>,
    ) -> Box<dyn Node> {
        let base = NodeBase {
            id: arena.len(),
            sources: vec![self.id()],
        };
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
}

#[derive(Debug)]
struct NodeBase {
    id: node_id,
    sources: Vec<node_id>,
}

struct CSVNode {
    base: NodeBase,
    filename: String,
}

impl CSVNode {
    fn new(arena: &Arena<Box<dyn Node>>, filename: String) -> Box<dyn Node> {
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
    let arena: Arena<Box<dyn Node>> = Arena::new();
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
