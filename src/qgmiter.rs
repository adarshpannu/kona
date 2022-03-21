// QGM Iterators

use crate::graph::*;
use crate::includes::*;
use crate::qgm::*;
use crate::expr::{*, Expr::*};

pub struct QueryBlockIter<'a> {
    qblock_graph: &'a QueryBlockGraph,
    queue: Vec<QueryBlockKey>,
}

impl<'a> Iterator for QueryBlockIter<'a> {
    type Item = QueryBlockKey;
    fn next(&mut self) -> Option<Self::Item> {
        while self.queue.len() > 0 {
            let qbkey = self.queue.pop().unwrap();
            let (qblocknode, _, children) = self.qblock_graph.get3(qbkey);
            /*
            if let Some(children) = children {
                // UIE set operators have legs; make sure we traverse them
                self.queue.append(&mut children.clone());
            }
            */
            let children: Vec<QueryBlockKey> = qblocknode.quns.iter().filter_map(|qun| qun.qblock).collect();
            self.queue.append(&mut children.clone());
            return Some(qbkey);
        }
        None
    }
}

impl QGM {
    pub fn iter_qblock(&self) -> QueryBlockIter {
        let mut queue = vec![self.main_qblock_key];
        queue.append(&mut self.cte_list.clone());
        QueryBlockIter {
            qblock_graph: &self.qblock_graph,
            queue,
        }
    }
}

impl QGM {
    pub fn scratch(&self) {
        let qblock_iter = self.iter_qblock();
        for qbkey in qblock_iter {
            debug!("qblock_iter: {:?}", qbkey);
        }
    }
}

impl ExprKey {
    pub fn iter_quncols<'g>(&self, graph: &'g ExprGraph) -> Box<dyn Iterator<Item = QunCol> + 'g> {
        let it = graph
            .iter(*self)
            .filter_map(move |nodeid| match &graph.get(nodeid).value {
                Column { qunid, colid, .. } => Some(QunCol(*qunid, *colid)),
                CID(qunid, cid) => Some(QunCol(*qunid, *cid)),
                _ => None,
            });
        Box::new(it)
    }

    pub fn iter_quns<'g>(&self, graph: &'g ExprGraph) -> Box<dyn Iterator<Item = QunId> + 'g> {
        Box::new(self.iter_quncols(graph).map(|quncol| quncol.0))
    }
}