// qgmiter: Various iterators over QGM

use crate::{
    expr::{
        Expr::{Column, CID},
        ExprGraph,
    },
    graph::{ExprKey, QueryBlockKey},
    includes::*,
    qgm::{QueryBlockGraph, QGM},
};

pub struct QueryBlockIter<'a> {
    qblock_graph: &'a QueryBlockGraph,
    queue: Vec<QueryBlockKey>,
}

impl<'a> Iterator for QueryBlockIter<'a> {
    type Item = QueryBlockKey;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.queue.is_empty() {
            let qbkey = self.queue.pop().unwrap();
            let qblocknode = &self.qblock_graph.get(qbkey).value;
            /*
            if let Some(children) = children {
                // UIE set operators have legs; make sure we traverse them
                self.queue.append(&mut children.clone());
            }
            */
            let mut children: Vec<QueryBlockKey> = qblocknode.quns.iter().filter_map(|qun| qun.get_qblock()).collect();
            self.queue.append(&mut children);
            return Some(qbkey);
        }
        None
    }
}

impl QGM {
    pub fn iter_qblocks0(&self) -> QueryBlockIter<'_> {
        let mut queue = vec![self.main_qblock_key];
        queue.append(&mut self.cte_list.clone());
        QueryBlockIter { qblock_graph: &self.qblock_graph, queue }
    }

    pub fn iter_qblocks(&self) -> Box<dyn Iterator<Item = QueryBlockKey> + '_> {
        Box::new(self.qblock_graph.sm.keys())
    }

    pub fn iter_quncols(&self) -> Box<dyn Iterator<Item = QunCol> + '_> {
        let qblock_iter = self.iter_qblocks();
        let iter = qblock_iter.flat_map(move |qblock_key| qblock_key.iter_quncols(&self.qblock_graph, &self.expr_graph));
        Box::new(iter)
    }

    pub fn iter_toplevel_exprs(&self) -> Box<dyn Iterator<Item = ExprKey> + '_> {
        let qblock_iter = self.iter_qblocks();
        let iter = qblock_iter.flat_map(move |qblock_key| qblock_key.iter_toplevel_exprs(&self.qblock_graph));
        Box::new(iter)
    }
}

impl ExprKey {
    pub fn iter_quncols<'g>(&self, expr_graph: &'g ExprGraph) -> Box<dyn Iterator<Item = QunCol> + 'g> {
        let it = expr_graph.true_iter(*self).filter_map(move |nodeid| {
            let expr = &expr_graph.get(nodeid).value;
            match expr {
                Column { qunid, colid, .. } => Some(QunCol(*qunid, *colid)),
                CID(qunid, cid) => Some(QunCol(*qunid, *cid)),
                _ => None,
            }
        });
        Box::new(it)
    }

    pub fn iter_quns<'g>(&self, expr_graph: &'g ExprGraph) -> Box<dyn Iterator<Item = QunId> + 'g> {
        Box::new(self.iter_quncols(expr_graph).map(|quncol| quncol.0))
    }
}

impl QueryBlockKey {
    pub fn iter_toplevel_exprs<'g>(&self, qblock_graph: &'g QueryBlockGraph) -> Box<dyn Iterator<Item = ExprKey> + 'g> {
        let qblock = &qblock_graph.get(*self).value;

        // Append select_list expressions
        let mut iter: Box<dyn Iterator<Item = ExprKey>> = Box::new(qblock.select_list.iter().map(|ne| ne.expr_key));

        // Append pred_list, group_by and having_clause expressions
        // todo: order_by
        for &expr_list in &[&qblock.pred_list, &qblock.group_by, &qblock.having_clause] {
            if let Some(expr_list) = expr_list {
                iter = Box::new(iter.chain(expr_list.iter().copied()));
            }
        }
        iter
    }

    pub fn iter_quncols<'g>(&self, qblock_graph: &'g QueryBlockGraph, expr_graph: &'g ExprGraph) -> Box<dyn Iterator<Item = QunCol> + 'g> {
        let iter = self.iter_toplevel_exprs(qblock_graph);
        let iter = iter.flat_map(move |expr_key| expr_key.iter_quncols(expr_graph));
        Box::new(iter)
    }
}
