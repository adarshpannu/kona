use crate::includes::*;
use slotmap::{SlotMap, new_key_type};
use crate::row::DataType;

new_key_type! { pub struct NodeId; }

#[derive(Debug, Serialize, Deserialize)]
pub struct Node<T> {
    pub inner: T,
    pub children: Option<Vec<NodeId>>,
    pub datatype: DataType
}

impl<T> Node<T> {
    pub fn new(t: T) -> Self {
        Node { inner: t, children: None, datatype: DataType::UNKNOWN }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph<T> {
    pub sm: SlotMap<NodeId, Node<T>>,
    next_id: usize,

}

impl<T> Graph<T> {
    pub fn new() -> Self {
        Graph { sm: SlotMap::with_key(), next_id: 0 }
    }

    pub fn next_id(&mut self) -> usize {
        let retval = self.next_id;
        self.next_id += 1;
        retval
    }

    pub fn add_node(&mut self, t: T, children: Option<Vec<NodeId>>) -> NodeId {
        let mut node = Node::new(t);
        node.children = children;
        self.sm.insert(node)
    }

    pub fn len(&self) -> usize {
        self.sm.len()
    }

    pub fn get_children(&self, ix: NodeId) -> Option<Vec<NodeId>> {
        let expr = self.sm.get(ix).unwrap();
        if let Some(children) = &expr.children {
            Some(children.clone())
        } else {
            None
        }
    }

    pub fn get_node(&self, ix: NodeId) -> &Node<T> {
        let node = self.sm.get(ix).unwrap();
        node
    }

    pub fn get_node_mut(&mut self, ix: NodeId) -> &mut Node<T> {
        let mut node = self.sm.get_mut(ix).unwrap();
        node
    }

    pub fn get_node_with_children(&self, ix: NodeId) -> (&T, Option<&Vec<NodeId>>) {
        let node = self.sm.get(ix).unwrap();
        (&node.inner, node.children.as_ref())
    }

    pub fn replace(&mut self, ix: NodeId, t: T) {
        let mut node = self.sm.get_mut(ix).unwrap();
        let mut new_node = Node::new(t);
        *node = new_node;
    }

    pub fn replace_many(&mut self, parent_ix: NodeId, ix: NodeId, mut children: Vec<NodeId>) {
        // Node#ix is deleted
        // children already present in graph although not connected
        let mut node = self.sm.get_mut(parent_ix).unwrap();
        let mut new_children: Vec<NodeId> = vec![];
        let seen_old_child = false;

        for &child_ix in node.children.as_ref().unwrap() {
            if child_ix == ix {
                new_children.append(&mut children);
            } else {
                new_children.push(child_ix)
            }
        }
        node.children = Some(new_children);
    }
}

#[derive(Debug)]
enum TestRelOpType {
    Lt, Le, Gt, Ge, Ne, Eq
}

#[derive(Debug)]
enum TestExpr {
    Column(String),
    CID(usize),
    Integer(isize),
    Boolean(bool),
    Star,
    Select,
    Cast,
    Relop(TestRelOpType)
}

#[test]
pub fn test_graph() {

    // select (111 = 222), *
    let mut qgm: Graph<TestExpr> = Graph::new();

    let lhs = qgm.add_node(TestExpr::Integer(111), None);
    let rhs = qgm.add_node(TestExpr::Integer(222), None);
    let relop = qgm.add_node(TestExpr::Relop(TestRelOpType::Eq), Some(vec![lhs, rhs]));

    //let mut e = qgm.get_node_mut(e);
    //*e = Expr::Boolean(false);
    //qgm.replace_node(2, Expr::Boolean(false));

    let star = qgm.add_node(TestExpr::Star, None);

    let select = qgm.add_node(TestExpr::Select, Some(vec![relop, star]));

    let cols: Vec<NodeId> = vec!["c1", "c2"].into_iter().map(|col| {
        let expr = TestExpr::Column(col.to_string());
        qgm.add_node(expr, None)
    }).collect();

    qgm.replace_many(select, star, cols);
    dbg!(&qgm);
}

fn qst(g: &mut Graph<TestExpr>, ix: NodeId) {
    let expr = &g.get_node(ix).inner;
    match expr {
        TestExpr::Relop(_) => {

        },
        TestExpr::Column(_) => {
            g.replace(ix, TestExpr::CID(99));
        }
        _ => {}
    }
}

