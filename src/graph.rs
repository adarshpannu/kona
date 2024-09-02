// graph

use std::fmt;

use regex::Regex;
use slotmap::{new_key_type, SlotMap};

use crate::includes::*;

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
        Node { value: v, properties, children: None }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph<K, V, P>
where
    K: slotmap::Key, {
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

impl<K, V, P> Default for Graph<K, V, P>
where
    K: slotmap::Key,
{
    fn default() -> Self {
        Graph {
            sm: SlotMap::with_key(),
            next_id: 1, // Starts at 1. There's code out these that handles zero-valued things differently. So keep this at 1.
        }
    }
}

impl<K, V, P> Graph<K, V, P>
where
    K: slotmap::Key,
{
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

    pub fn get3(&self, key: K) -> (&V, &P, Option<&Vec<K>>) {
        let node = self.sm.get(key);
        if node.is_none() {
            panic!("hello")
        }
        let node = node.unwrap();
        (&node.value, &node.properties, node.children.as_ref())
    }

    pub fn get(&self, key: K) -> &Node<K, V, P> {
        let node = self.sm.get(key);
        if node.is_none() {
            panic!("hello")
        }
        node.unwrap()
    }

    pub fn get_value(&self, key: K) -> &V {
        &self.sm.get(key).unwrap().value
    }

    pub fn get_properties(&self, key: K) -> &P {
        &self.sm.get(key).unwrap().properties
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
        let node = self.sm.get_mut(parent_key).unwrap();
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

    pub fn iter(&self, root: K) -> GraphIterator<K> {
        GraphIterator { queue: vec![root], stop_depth_traversal: None }
    }

    pub fn iter_cond<F>(&self, root: K, cond: Option<Box<dyn Fn(K) -> bool>>) -> GraphIterator<K> {
        GraphIterator { queue: vec![root], stop_depth_traversal: cond }
    }

    pub fn true_iter(&self, root: K) -> TrueGraphIterator<'_, K, V, P> {
        let graph_iter = GraphIterator { queue: vec![root], stop_depth_traversal: None };
        TrueGraphIterator { graph_iter, graph: self }
    }
}

pub struct GraphIterator<K>
where
    K: slotmap::Key, {
    queue: Vec<K>,
    stop_depth_traversal: Option<Box<dyn Fn(K) -> bool>>, // If true, don't explore children of a given node
}

pub struct TrueGraphIterator<'a, K, V, P>
where
    K: slotmap::Key, {
    graph_iter: GraphIterator<K>,
    graph: &'a Graph<K, V, P>,
}

// Breadth-first iterator
impl<K> GraphIterator<K>
where
    K: slotmap::Key,
{
    pub fn next<V, P>(&mut self, graph: &Graph<K, V, P>) -> Option<K> {
        if self.queue.is_empty() {
            return None;
        }
        let cur_key_option = self.queue.pop();
        if let Some(cur_key) = cur_key_option {
            if let Some(stop_depth_traversal) = &self.stop_depth_traversal {
                if !stop_depth_traversal(cur_key) {
                    return cur_key_option;
                }
            }
            let children = graph.get(cur_key).children.as_ref();
            if let Some(children) = children {
                self.queue.extend(children.iter());
            }
        }
        cur_key_option
    }
}

impl<'a, K, V, P> Iterator for TrueGraphIterator<'a, K, V, P>
where
    K: slotmap::Key,
    V: std::fmt::Debug,
{
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        self.graph_iter.next(self.graph)
    }
}

pub fn key_to_id(keystr: &str) -> String {
    let re1 = Regex::new(r"^.*\(").unwrap();
    let re2 = Regex::new(r"\).*$").unwrap();
    //let re3 = Regex::new(r"v1$").unwrap();

    let id = keystr;
    let id = re1.replace_all(&id, "");
    let id = re2.replace_all(&id, "");
    //let id = re3.replace_all(&id, "");
    id.to_string()
}

impl fmt::Display for QueryBlockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for ExprKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id())
    }
}

impl fmt::Display for LOPKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id())
    }
}

impl fmt::Display for POPKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id())
    }
}

pub trait KeyId
where
    Self: fmt::Debug, {
    fn id(&self) -> String {
        let keystr = format!("{:?}", self);
        key_to_id(&keystr)
    }
}

impl KeyId for POPKey {}
impl KeyId for LOPKey {}
impl KeyId for ExprKey {}

