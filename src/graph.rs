use crate::row::DataType;
use crate::{ast::ExprProp, includes::*};
use slotmap::{new_key_type, SlotMap};

new_key_type! { pub struct NodeId; }

#[derive(Debug, Serialize, Deserialize)]
pub struct Node<T, P>
where
    P: std::default::Default,
{
    pub inner: T,
    pub properties: P,
    pub children: Option<Vec<NodeId>>,
}

impl<T, P> Node<T, P>
where
    P: std::default::Default,
{
    pub fn new(t: T, properties: P) -> Self {
        Node {
            inner: t,
            properties,
            children: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph<T, P>
where
    P: std::default::Default,
{
    pub sm: SlotMap<NodeId, Node<T, P>>,
    next_id: usize,
}

impl<T, P> Graph<T, P>
where
    P: std::default::Default,
{
    pub fn new() -> Self {
        Graph {
            sm: SlotMap::with_key(),
            next_id: 0,
        }
    }

    pub fn next_id(&mut self) -> usize {
        let retval = self.next_id;
        self.next_id += 1;
        retval
    }

    pub fn add_node(&mut self, t: T, children: Option<Vec<NodeId>>) -> NodeId {
        let properties = P::default();
        let mut node = Node::new(t, properties);
        node.children = children;
        self.sm.insert(node)
    }

    pub fn add_node_with_props(&mut self, t: T, properties: P, children: Option<Vec<NodeId>>) -> NodeId {
        let mut node = Node::new(t, properties);
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

    pub fn get_node(&self, ix: NodeId) -> &Node<T, P> {
        let node = self.sm.get(ix).unwrap();
        node
    }

    pub fn get_node_mut(&mut self, ix: NodeId) -> &mut Node<T, P> {
        let mut node = self.sm.get_mut(ix).unwrap();
        node
    }

    pub fn get_node_with_children(&self, ix: NodeId) -> (&T, Option<&Vec<NodeId>>) {
        let node = self.sm.get(ix).unwrap();
        (&node.inner, node.children.as_ref())
    }

    pub fn get_node_with_children_mut(&mut self, ix: NodeId) -> (&mut T, Option<&mut Vec<NodeId>>) {
        let mut node = self.sm.get_mut(ix).unwrap();
        (&mut node.inner, node.children.as_mut())
    }

    pub fn replace(&mut self, ix: NodeId, t: T, p: P) {
        let mut node = self.sm.get_mut(ix).unwrap();
        let mut new_node = Node::new(t, p);
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

    pub fn iter<'a>(&'a self, root: NodeId) -> GraphIterator<'a, T, P> {
        GraphIterator {
            graph: self,
            queue: vec![root],
        }
    }
}

pub struct GraphIterator<'a, T, P>
where
    P: std::default::Default,
{
    graph: &'a Graph<T, P>,
    queue: Vec<NodeId>,
}

// Breadth-first iterator
impl<'a, T, P> Iterator for GraphIterator<'a, T, P>
where
    P: std::default::Default,
{
    type Item = NodeId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.queue.len() == 0 {
            return None;
        }
        let cur_id = self.queue.pop();
        if let Some(cur_id) = cur_id {
            if let Some(children) = self.graph.get_children(cur_id) {
                self.queue.extend(children.iter());
            }
        }
        cur_id
    }
}

impl NodeId {}
