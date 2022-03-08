use crate::ast::{Expr::*, *};
use crate::flow::*;
use crate::graph::*;
use crate::includes::*;
use crate::row::*;
use crate::task::*;

pub struct APS;

enum POP {
    TableScan { colids: Vec<ColId> },
    HashJoin,
    NLJoin,
    Sort,
    GroupBy,
}

struct POPProps {
    output: Vec<NodeId>,
}

impl POPProps {
    fn new(output: Vec<NodeId>) -> Self {
        POPProps { output }
    }
}

impl std::default::Default for POPProps {
    fn default() -> Self {
        POPProps { output: vec![] }
    }
}

impl APS {
    pub fn find_best_plan(env: &Env, qgm: &mut QGM) -> Result<(), String> {
        let graph = replace(&mut qgm.graph, Graph::new());
        let mut pop_graph: Graph<POP, POPProps> = Graph::new();
        let worklist: Vec<NodeId> = vec![];
        let topqblock = &qgm.qblock;

        assert!(qgm.cte_list.len() == 0);

        // Extract cols from select-list into vector of (qun_id, col_id) pairs
        let select_list_cols = topqblock
            .select_list
            .iter()
            .flat_map(|ne| {
                let cols = graph
                    .iter(ne.expr_id)
                    .filter_map(|nodeid| {
                        if let Column {
                            prefix,
                            colname,
                            qunid,
                            colid,
                        } = &graph.get_node(nodeid).inner
                        {
                            debug!("COL: {:?}.{:?}", prefix, colname);
                            Some((*qunid, *colid))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                cols
            })
            .collect::<Vec<_>>();

        // Classify boolean factors 
        // todo

        // Build tablescan POPs first
        for qun in topqblock.quns.iter() {
            let colids: Vec<ColId> = qun.get_column_map().keys().map(|e| *e).collect();
            let props = POPProps::new(vec![]);
            let popnode = pop_graph.add_node_with_props(POP::TableScan { colids }, props, None);
        }

        Ok(())
    }

}
