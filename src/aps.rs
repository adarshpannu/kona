use crate::expr::{Expr::*, *};
use crate::graph::*;
use crate::graph::*;

use crate::includes::*;
use crate::qgm::*;
use std::collections::{HashMap, HashSet};

pub struct APS;

enum POP {
    TableScan { qunid: QunId, colids: Vec<ColId> },
    HashJoin,
    NLJoin,
    Sort,
    GroupBy,
}

struct POPProps {
    quns: HashSet<QunId>,
    preds: HashSet<ExprId>,
    projections: Vec<ColId>,
}

impl POPProps {
    fn new(quns: HashSet<QunId>, preds: HashSet<ExprId>, projections: Vec<ColId>) -> Self {
        POPProps {
            quns,
            preds,
            projections,
        }
    }
}

impl std::default::Default for POPProps {
    fn default() -> Self {
        POPProps {
            quns: HashSet::new(),
            preds: HashSet::new(),
            projections: vec![],
        }
    }
}

#[derive(Debug)]
enum PredicateType {
    Constant, // 1 = 2
    Local,    // t.col1 = t.col2 + 20
    EquiJoin, // r.col1 + 10 = s.col2 + 20
    Other,    // r.col1 > s.col1
}

impl APS {
    pub fn find_best_plan(env: &Env, qgm: &mut QGM) -> Result<(), String> {
        let graph = replace(&mut qgm.graph, Graph::new());
        let mut pop_graph: Graph<POPId, POP, POPProps> = Graph::new();
        let topqblock = &qgm.qblock;
        let mut worklist: Vec<POPId> = vec![];

        assert!(qgm.cte_list.len() == 0);

        // Extract cols from select-list into vector of (qun_id, col_id) pairs
        let select_list_cols = topqblock
            .select_list
            .iter()
            .flat_map(|ne| {
                Self::get_unique_cols(&graph, ne.expr_id)
                    .into_iter()
                    .collect::<Vec<QunCol>>()
            })
            .collect::<Vec<_>>();

        // Process predicates:
        // 1. Classify predicates: pred -> type
        // 2. Collect quns + cols for each predicates: pred -> set(quns)
        let mut pred_to_quns_map: HashMap<ExprId, HashSet<QunId>> = HashMap::new();
        let mut pred_to_type_map: HashMap<ExprId, PredicateType> = HashMap::new();
        let mut pred_to_quncols_map: HashMap<ExprId, HashSet<QunCol>> = HashMap::new();

        if let Some(pred_list) = topqblock.pred_list.as_ref() {
            for &pred_id in pred_list.iter() {
                let quns = Self::get_unique_quns(&graph, pred_id);
                let pred_type = if quns.len() == 0 {
                    // Constant
                    PredicateType::Constant
                } else if quns.len() == 1 {
                    PredicateType::Local
                } else {
                    // Join
                    let expr = graph.get_node(pred_id);
                    if let RelExpr(RelOp::Eq) = expr.inner {
                        let children = expr.children.as_ref().unwrap();
                        let (left_child_id, right_child_id) = (children[0], children[1]);
                        let left_quns = Self::get_unique_quns(&graph, left_child_id);
                        let right_quns = Self::get_unique_quns(&graph, right_child_id);
                        if left_quns.intersection(&right_quns).collect::<HashSet<_>>().len() == 0 {
                            PredicateType::EquiJoin
                        } else {
                            PredicateType::Other
                        }
                    } else {
                        PredicateType::Other
                    }
                };
                debug!(
                    "Predicate: {}, quns={:?}, type={:?}",
                    Expr::to_string(pred_id, &graph),
                    &quns,
                    pred_type
                );
                pred_to_type_map.insert(pred_id, pred_type);
                pred_to_quns_map.insert(pred_id, quns.clone());
            }
        }

        // Build tablescan POPs first
        for qun in topqblock.quns.iter() {
            let colids: Vec<ColId> = qun.get_column_map().keys().map(|e| *e).collect();
            let mut quns = HashSet::new();
            quns.insert(qun.id);
            let props = POPProps::new(quns, HashSet::new(), vec![]);
            let popnode = pop_graph.add_node_with_props(POP::TableScan { qunid: qun.id, colids }, props, None);
            worklist.push(popnode);
        }

        let n = topqblock.quns.len();
        dbg!(&n);
        for ix in (2..=n).rev() {
            // Make pairs of all plans in work-list
            for &plan1_id in worklist.iter() {
                for &plan2_id in worklist.iter() {
                    let plan1 = pop_graph.get_node(plan1_id);
                    let plan2 = pop_graph.get_node(plan2_id);
                }
            }
        }

        Ok(())
    }

    pub fn get_unique_cols(graph: &Graph<ExprId, Expr, ExprProp>, root_node_id: ExprId) -> HashSet<QunCol> {
        let qun_col_pairs: HashSet<QunCol> = graph
            .iter(root_node_id)
            .filter_map(|nodeid| {
                if let Column { qunid, colid, .. } = &graph.get_node(nodeid).inner {
                    Some(QunCol(*qunid, *colid))
                } else {
                    None
                }
            })
            .collect();
        qun_col_pairs
    }

    pub fn get_unique_quns(graph: &Graph<ExprId, Expr, ExprProp>, root_node_id: ExprId) -> HashSet<QunId> {
        let qun_col_pairs = Self::get_unique_cols(graph, root_node_id);
        qun_col_pairs.iter().map(|quncol| quncol.0).collect()
    }
}
