use crate::includes::*;
use slotmap::{new_key_type, SlotMap};

new_key_type! { pub struct ExprKey; }
new_key_type! { pub struct LOPKey; }
new_key_type! { pub struct POPKey; }
new_key_type! { pub struct QueryBlockKey; }

#[derive(Debug, Serialize, Deserialize)]
pub struct Node<K, V, P> {
    pub value: V,
    pub properties: P,
    pub children: Option<Vec<K>>,
}

impl<K, V, P> Node<K, V, P> {
    pub fn new(v: V, properties: P) -> Self {
        Node {
            value: v,
            properties,
            children: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph<K, V, P>
where
    K: slotmap::Key,
{
    pub sm: SlotMap<K, Node<K, V, P>>,
    next_id: usize,
}

impl<K, V, P> Graph<K, V, P>
where
    P: std::default::Default,
    K: slotmap::Key,
{
    pub fn add_node(&mut self, v: V, children: Option<Vec<K>>) -> K {
        let properties = P::default();
        let mut node = Node::new(v, properties);
        node.children = children;
        self.sm.insert(node)
    }
}

impl<K, V, P> Graph<K, V, P>
where
    K: slotmap::Key,
{
    pub fn new() -> Self {
        Graph {
            sm: SlotMap::with_key(),
            next_id: 1,   // Starts at 1. There's code out these that handles zero-valued things differently. So keep this at 1.
        }
    }

    pub fn next_id(&mut self) -> usize {
        let retval = self.next_id;
        self.next_id += 1;
        retval
    }

    pub fn add_node_with_props(&mut self, v: V, properties: P, children: Option<Vec<K>>) -> K {
        let mut node = Node::new(v, properties);
        node.children = children;
        self.sm.insert(node)
    }

    pub fn len(&self) -> usize {
        self.sm.len()
    }

    pub fn get3(&self, key: K) -> (&V, &P, Option<&Vec<K>>) {
        let node = self.sm.get(key).unwrap();
        (&node.value, &node.properties, node.children.as_ref())
    }

    pub fn get(&self, key: K) -> &Node<K, V, P> {
        let node = self.sm.get(key).unwrap();
        node
    }

    pub fn get_mut(&mut self, key: K) -> &mut Node<K, V, P> {
        let node = self.sm.get_mut(key).unwrap();
        node
    }

    pub fn get_disjoint_mut(&mut self, keys: [K; 2]) -> [&mut Node<K, V, P>; 2] {
        self.sm.get_disjoint_mut(keys).unwrap()
    }

    pub fn replace(&mut self, key: K, v: V, p: P) {
        let node = self.sm.get_mut(key).unwrap();
        let new_node = Node::new(v, p);
        *node = new_node;
    }

    pub fn replace_many(&mut self, parent_key: K, key: K, mut children: Vec<K>) {
        // Node#key is deleted
        // children already present in graph although not connected
        let mut node = self.sm.get_mut(parent_key).unwrap();
        let mut new_children: Vec<K> = vec![];

        for &child_key in node.children.as_ref().unwrap() {
            if child_key == key {
                new_children.append(&mut children);
            } else {
                new_children.push(child_key)
            }
        }
        node.children = Some(new_children);
    }

    pub fn iter<'a>(&'a self, root: K) -> GraphIterator<'a, K, V, P> {
        GraphIterator {
            graph: self,
            queue: vec![root],
        }
    }
}

pub struct GraphIterator<'a, K, V, P>
where
    K: slotmap::Key,
{
    graph: &'a Graph<K, V, P>,
    queue: Vec<K>,
}

// Breadth-first iterator
impl<'a, K, V, P> Iterator for GraphIterator<'a, K, V, P>
where
    K: slotmap::Key,
    V: std::fmt::Debug
{
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        if self.queue.len() == 0 {
            return None;
        }
        let cur_key = self.queue.pop();
        if let Some(cur_key) = cur_key {
            let children = self.graph.get(cur_key).children.as_ref();
            if let Some(children) = children {
                self.queue.extend(children.iter());
            }
        }
        cur_key
    }
}
