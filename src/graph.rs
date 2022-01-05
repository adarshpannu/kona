
type NodeId = usize;

#[derive(Debug)]
struct Node<T> {
    inner: T,
    children: Option<Vec<NodeId>>
}

impl<T> Node<T> {
    pub fn new(t: T) -> Self {
        Node { inner: t, children: None }
    }
}

#[derive(Debug)]
struct Graph<T> {
    nodes: Vec<Node<T>>
}

impl<T> Graph<T> {
    pub fn new() -> Self {
        Graph { nodes: vec![] }
    }

    pub fn add_node(&mut self, t: T, children: Option<Vec<NodeId>>) -> NodeId {
        let mut node = Node::new(t);
        node.children = children;
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    pub fn get_node(&mut self, ix: NodeId) -> &T {
        &self.nodes[ix].inner
    }

    pub fn get_node2(&mut self, ix: NodeId) -> (&T, NodeId, NodeId) {
        let children = self.nodes[ix].children.as_ref();
        (&self.nodes[ix].inner, 0, 0)
    }

    pub fn get_node_mut(&mut self, ix: NodeId) -> &mut T {
        &mut self.nodes[ix].inner
    }

    pub fn replace(&mut self, ix: NodeId, t: T) {
        let mut node = Node::new(t);
        self.nodes[ix] = node;
    }

    pub fn replace_many(&mut self, parent_ix: NodeId, ix: NodeId, mut children: Vec<NodeId>) {
        // Node#ix is deleted
        // children already present in graph although not connected
        let mut node = &mut self.nodes[parent_ix];
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
enum RelOpType {
    Lt, Le, Gt, Ge, Ne, Eq
}

#[derive(Debug)]
enum Expr {
    Column(String),
    CID(usize),
    Integer(isize),
    Boolean(bool),
    Star,
    Select,
    Cast,
    Relop(RelOpType)
}

#[test]
pub fn test_graph() {
    let mut qgm: Graph<Expr> = Graph::new();

    let lhs = qgm.add_node(Expr::Integer(111), None);
    let rhs = qgm.add_node(Expr::Integer(222), None);
    let e = qgm.add_node(Expr::Relop(RelOpType::Eq), Some(vec![lhs, rhs]));

    let mut e = qgm.get_node_mut(2);

    *e = Expr::Boolean(false);

    //qgm.replace_node(2, Expr::Boolean(false));

    let cols: Vec<NodeId> = vec!["c1", "c2"].into_iter().map(|col| {
        let expr = Expr::Column(col.to_string());
        qgm.add_node(expr, None)
    }).collect();

    qgm.replace_many(2, 0, cols);
    dbg!(&qgm);
}

fn qst(g: &mut Graph<Expr>, ix: NodeId) {
    let expr = g.get_node(ix);
    match expr {
        Expr::Relop(_) => {

        },
        Expr::Column(_) => {
            g.replace(ix, Expr::CID(99));
        }
        _ => {}
    }
}

