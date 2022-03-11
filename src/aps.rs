use crate::expr::{Expr::*, *};
use crate::graph::*;
use crate::graph::*;

use crate::includes::*;
use crate::qgm::*;
use std::collections::{HashMap, HashSet};

pub struct APS;

enum POP {
    TableScan,
    HashJoin,
    NLJoin,
    Sort,
    GroupBy,
}

#[derive(Debug)]
struct POPProps {
    quns: HashSet<QunId>,
    cols: HashSet<ColId>,
    preds: HashSet<ExprId>,
}

impl POPProps {
    fn new(quns: HashSet<QunId>, cols: HashSet<ColId>, preds: HashSet<ExprId>, ) -> Self {
        POPProps {
            quns,
            cols,
            preds,
        }
    }
}

impl std::default::Default for POPProps {
    fn default() -> Self {
        POPProps {
            quns: HashSet::new(),
            preds: HashSet::new(),
            cols: HashSet::new(),
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
        let mainqblock = &qgm.qblock;
        let mut worklist: Vec<POPId> = vec![];

        assert!(qgm.cte_list.len() == 0);

        // Process select-list: Collect all QunCols
        let mut select_list_cols: Vec<QunCol> = mainqblock
            .select_list
            .iter()
            .flat_map(|ne| {
                Self::get_unique_quncols(&graph, ne.expr_id)
                    .into_iter()
                    .collect::<Vec<QunCol>>()
            })
            .collect();
        select_list_cols.sort();
        select_list_cols.dedup();

        // Process predicates:
        // 1. Classify predicates: pred -> type
        // 2. Collect QunCols for each predicates: pred -> set(QunCol)
        // 3. Collect quns for each predicates: pred -> set(Qun)
        let mut pred_to_type_map: HashMap<ExprId, PredicateType> = HashMap::new();
        let mut pred_to_quns_map: HashMap<ExprId, HashSet<QunId>> = HashMap::new();
        let mut pred_to_quncols_map: HashMap<ExprId, HashSet<QunCol>> = HashMap::new();

        if let Some(pred_list) = mainqblock.pred_list.as_ref() {
            for &pred_id in pred_list.iter() {
                let quncols = Self::get_unique_quncols(&graph, pred_id);
                let quns = Self::get_unique_quns(&graph, pred_id);

                let pred_type = if quns.len() == 0 {
                    // Constant
                    PredicateType::Constant
                } else if quns.len() == 1 {
                    PredicateType::Local
                } else {
                    // Join
                    let expr = graph.get_node(pred_id);
                    if let RelExpr(RelOp::Eq) = expr.contents {
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
                pred_to_quns_map.insert(pred_id, quns);
            }
        }

        // Build tablescan POPs first
        for qun in mainqblock.quns.iter() {
            // Set quns
            let mut quns = HashSet::new();
            quns.insert(qun.id);

            // Set cols: find all column references for this qun
            let cols: HashSet<ColId> = qun.get_column_map().keys().map(|e| *e).collect();

            // Set preds: find preds that refer to this qun
            let preds: HashSet<ExprId> = pred_to_quns_map
                .iter()
                .filter_map(|(pred_id, quns)| {
                    if quns.len() == 1 && quns.contains(&qun.id) {
                        Some(*pred_id)
                    } else {
                        None
                    }
                })
                .collect();

            // Set props
            let props = POPProps::new(quns, cols, preds);
            debug!("POPNODE props: {:?}", &props);

            let popnode = pop_graph.add_node_with_props(POP::TableScan, props, None);

            worklist.push(popnode);
        }

        let n = mainqblock.quns.len();
        dbg!(&n);
        for ix in (2..=n).rev() {
            // Make pairs of all plans in work-list
            for &plan1_id in worklist.iter() {
                for &plan2_id in worklist.iter() {
                    let plan1 = pop_graph.get_node(plan1_id);
                    let plan2 = pop_graph.get_node(plan2_id);

                    // Is there a join between two subplans?
                    let plan1_quns = &plan1.properties.quns;
                    let plan2_quns = &plan1.properties.quns;

                }
            }
        }

        Ok(())
    }

    pub fn get_unique_quncols(graph: &Graph<ExprId, Expr, ExprProp>, root_node_id: ExprId) -> HashSet<QunCol> {
        let qun_col_pairs: HashSet<QunCol> = graph
            .iter(root_node_id)
            .filter_map(|nodeid| {
                if let Column { qunid, colid, .. } = &graph.get_node(nodeid).contents {
                    Some(QunCol(*qunid, *colid))
                } else {
                    None
                }
            })
            .collect();
        qun_col_pairs
    }

    pub fn get_unique_quns(graph: &Graph<ExprId, Expr, ExprProp>, root_node_id: ExprId) -> HashSet<QunId> {
        let qun_col_pairs = Self::get_unique_quncols(graph, root_node_id);
        qun_col_pairs.iter().map(|quncol| quncol.0).collect()
    }
}
