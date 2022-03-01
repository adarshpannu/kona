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

    pub fn get_node_with_children_mut(&mut self, ix: NodeId) -> (&mut T, Option<&mut Vec<NodeId>>) {
        let mut node = self.sm.get_mut(ix).unwrap();
        (&mut node.inner, node.children.as_mut())
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
