use crate::includes::*;
use slotmap::{new_key_type, SlotMap};

new_key_type! { pub struct ExprKey; }
new_key_type! { pub struct POPKey; }
new_key_type! { pub struct QueryBlockKey; }

#[derive(Debug, Serialize, Deserialize)]
pub struct Node<K, T, P>
where
    P: std::default::Default,
{
    pub contents: T,
    pub properties: P,
    pub children: Option<Vec<K>>,
}

impl<K, T, P> Node<K, T, P>
where
    P: std::default::Default,
{
    pub fn new(t: T, properties: P) -> Self {
        Node {
            contents: t,
            properties,
            children: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph<K, T, P>
where
    P: std::default::Default,
    K: slotmap::Key
{
    pub sm: SlotMap<K, Node<K, T, P>>,
    next_id: usize,
}

impl<K, T, P> Graph<K, T, P>
where
    P: std::default::Default,
    K: slotmap::Key
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

    pub fn add_node(&mut self, t: T, children: Option<Vec<K>>) -> K {
        let properties = P::default();
        let mut node = Node::new(t, properties);
        node.children = children;
        self.sm.insert(node)
    }

    pub fn add_node_with_props(&mut self, t: T, properties: P, children: Option<Vec<K>>) -> K {
        let mut node = Node::new(t, properties);
        node.children = children;
        self.sm.insert(node)
    }

    pub fn len(&self) -> usize {
        self.sm.len()
    }

    pub fn get_children(&self, key: K) -> Option<Vec<K>> {
        let expr = self.sm.get(key).unwrap();
        if let Some(children) = &expr.children {
            Some(children.clone()) // fixme
        } else {
            None
        }
    }
    
    pub fn get1(&self, key: K) -> &T {
        let node = self.sm.get(key).unwrap();
        &node.contents
    }

    pub fn get1_mut(&mut self, key: K) -> &mut T {
        let node = self.sm.get_mut(key).unwrap();
        &mut node.contents
    }

    pub fn get1_disjoint_mut(&mut self, keys: [K; 2]) -> [&mut Node<K, T, P>; 2] {
        self.sm.get_disjoint_mut(keys).unwrap()
    }

    pub fn get3(&self, key: K) -> (&T, &P, Option<&Vec<K>>) {
        let node = self.sm.get(key).unwrap();
        (&node.contents, &node.properties, node.children.as_ref())
    }

    pub fn get3_mut(&mut self, key: K) -> (&mut T, &mut P, Option<&mut Vec<K>>) {
        let mut node = self.sm.get_mut(key).unwrap();
        (&mut node.contents, &mut node.properties, node.children.as_mut())
    }

    pub fn get(&self, key: K) -> &Node<K, T, P> {
        let node = self.sm.get(key).unwrap();
        node
    }

    pub fn get_mut(&mut self, key: K) -> &mut Node<K, T, P> {
        let mut node = self.sm.get_mut(key).unwrap();
        node
    }

    pub fn replace(&mut self, key: K, t: T, p: P) {
        let mut node = self.sm.get_mut(key).unwrap();
        let mut new_node = Node::new(t, p);
        *node = new_node;
    }

    pub fn replace_many(&mut self, parent_key: K, key: K, mut children: Vec<K>) {
        // Node#key is deleted
        // children already present in graph although not connected
        let mut node = self.sm.get_mut(parent_key).unwrap();
        let mut new_children: Vec<K> = vec![];
        let seen_old_child = false;

        for &child_key in node.children.as_ref().unwrap() {
            if child_key == key {
                new_children.append(&mut children);
            } else {
                new_children.push(child_key)
            }
        }
        node.children = Some(new_children);
    }

    pub fn iter<'a>(&'a self, root: K) -> GraphIterator<'a, K, T, P> {
        GraphIterator {
            graph: self,
            queue: vec![root],
        }
    }
}

pub struct GraphIterator<'a, K, T, P>
where
    P: std::default::Default,
    K: slotmap::Key
{
    graph: &'a Graph<K, T, P>,
    queue: Vec<K>,
}

// Breadth-first iterator
impl<'a, K, T, P> Iterator for GraphIterator<'a, K, T, P>
where
    P: std::default::Default,
    K: slotmap::Key
{
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        if self.queue.len() == 0 {
            return None;
        }
        let cur_id = self.queue.pop();
        if let Some(cur_id) = cur_id {
            let (_, _, children) = self.graph.get3(cur_id);
            if let Some(children) = children {
                self.queue.extend(children.iter());
            }
        }
        cur_id
    }
}
