// APS: Access Path Selection

use bitmaps::Bitmap;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;

use crate::includes::*;
use crate::bitset::*;
use crate::graph::*;
use crate::expr::{Expr::*, *};
use crate::qgm::*;
use std::process::Command;

type POPGraph = Graph<POPId, POP, POPProps>;

pub struct APS;

const BITMAPLEN: usize = 256;

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

#[derive(Debug)]
pub enum POP {
    TableScan,
    HashJoin,
    NLJoin,
    Sort,
    GroupBy,
}

#[derive(Debug)]
pub struct POPProps {
    quns: Bitset<QunId>,
    cols: Bitset<QunCol>,
    preds: Bitset<ExprId>,
}

impl POPProps {
    fn new(quns: Bitset<QunId>, cols: Bitset<QunCol>, preds: Bitset<ExprId>) -> Self {
        POPProps { quns, cols, preds }
    }

    fn union(&self, other: &Self) -> Self {
        let mut quns = self.quns.clone();
        quns.bitmap = quns.bitmap | other.quns.bitmap;

        let mut cols = self.cols.clone();
        cols.bitmap = cols.bitmap | other.cols.bitmap;

        let mut preds = self.preds.clone();
        preds.bitmap = preds.bitmap | other.preds.bitmap;

        Self::new(quns, cols, preds)
    }
}

impl std::default::Default for POPProps {
    fn default() -> Self {
        POPProps {
            quns: Bitset::new(),
            preds: Bitset::new(),
            cols: Bitset::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum PredicateType {
    Constant, // 1 = 2
    Local,    // t.col1 = t.col2 + 20
    EquiJoin, // r.col1 + 10 = s.col2 + 20
    Other,    // r.col1 > s.col1
}

impl APS {
    pub fn find_best_plan(env: &Env, qgm: &mut QGM) -> Result<(), String> {
        let graph = replace(&mut qgm.graph, Graph::new());
        let mut pop_graph: POPGraph = Graph::new();

        let mainqblock = &qgm.qblock;
        let mut worklist: Vec<POPId> = vec![];

        assert!(qgm.cte_list.len() == 0);

        let mut all_quncols_bitset: Bitset<QunCol> = Bitset::new();
        let mut all_quns_bitset: Bitset<QunId> = Bitset::new();
        let mut all_preds_bitset: Bitset<ExprId> = Bitset::new();

        // Process select-list: Collect all QunCols
        mainqblock
            .select_list
            .iter()
            .flat_map(|ne| {
                Self::get_expr_quncols(&graph, ne.expr_id)
                    .into_iter()
                    .collect::<Vec<QunCol>>()
            })
            .for_each(|quncol| all_quncols_bitset.set(quncol));
        let select_list_quncol_bitset = all_quncols_bitset.clone(); // shallow copy

        // Process predicates:
        // 1. Classify predicates: pred -> type
        // 2. Collect QunCols for each predicates: pred -> set(QunCol)
        // 3. Collect quns for each predicates: pred -> set(Qun)
        let mut pred_map: HashMap<ExprId, (PredicateType, Bitset<QunCol>, Bitset<QunId>)> = HashMap::new();

        if let Some(pred_list) = mainqblock.pred_list.as_ref() {
            for (pred_ix, &pred_id) in pred_list.iter().enumerate() {
                let mut quncols_bitset = all_quncols_bitset.clone().clear();
                let quncols = Self::get_expr_quncols(&graph, pred_id);
                quncols.iter().for_each(|&quncol| {
                    quncols_bitset.set(quncol);
                    all_quncols_bitset.set(quncol);
                });

                let mut quns_bitset = all_quns_bitset.clone().clear();
                let quns: HashSet<QunId> = quncols.iter().map(|quncol| quncol.0).collect();
                quns.iter().for_each(|&qun| quns_bitset.set(qun));

                let mut pred_type;
                if quns.len() == 0 {
                    // Constant
                    pred_type = PredicateType::Constant
                } else if quns.len() == 1 {
                    pred_type = PredicateType::Local
                } else {
                    // Join
                    let expr = graph.get(pred_id);
                    if let RelExpr(RelOp::Eq) = expr.contents {
                        let children = expr.children.as_ref().unwrap();
                        let (left_child_id, right_child_id) = (children[0], children[1]);
                        let left_quns = Self::get_expr_quns(&graph, left_child_id);
                        let right_quns = Self::get_expr_quns(&graph, right_child_id);
                        if left_quns.intersection(&right_quns).collect::<HashSet<_>>().len() == 0 {
                            pred_type = PredicateType::EquiJoin
                        } else {
                            pred_type = PredicateType::Other
                        }
                    } else {
                        pred_type = PredicateType::Other
                    }
                };
                debug!(
                    "Predicate {:?}: {}, quns={:?}, type={:?}",
                    pred_id,
                    Expr::to_string(pred_id, &graph),
                    &quns,
                    pred_type
                );
                pred_map.insert(pred_id, (pred_type, quncols_bitset, quns_bitset));
            }
        }

        // Build tablescan POPs first
        for qun in mainqblock.quns.iter() {
            debug!("Build TableScan: {:?} id={}", qun.display(), qun.id);

            // Set quns
            let mut quns_bitset = all_quns_bitset.clone().clear();
            quns_bitset.set(qun.id);

            // Set cols: find all column references for this qun
            let mut quncols_bitset = all_quncols_bitset.clone().clear();
            qun.get_column_map()
                .keys()
                .for_each(|&colid| quncols_bitset.set(QunCol(qun.id, colid)));

            // Set preds: find preds that refer to this qun
            let mut preds_bitset = all_preds_bitset.clone().clear();
            pred_map
                .iter()
                .for_each(|(&pred_id, (pred_type, quncols_bitset, quns_bitset))| {
                    if quns_bitset.bitmap.len() == 1 && quns_bitset.get(qun.id) {
                        preds_bitset.set(pred_id);
                    };
                });

            for pred_id in preds_bitset.elements().iter() {
                pred_map.remove_entry(pred_id);
            }

            // Set props
            let props = POPProps::new(quns_bitset, quncols_bitset, preds_bitset);
            debug!(
                "POPNODE quns={:?}, cols={:?}, preds={:?}",
                &props.quns.elements(),
                &props.cols.elements(),
                &props.preds.elements()
            );

            let popnode = pop_graph.add_node_with_props(POP::TableScan, props, None);

            worklist.push(popnode);
        }

        let n = mainqblock.quns.len();
        dbg!(&n);
        for ix in (2..=n).rev() {
            let mut join_status = None;

            // Make pairs of all plans in work-list
            'outer: for (ix1, &plan1_id) in worklist.iter().enumerate() {
                for (ix2, &plan2_id) in worklist.iter().enumerate() {
                    if plan1_id == plan2_id {
                        continue;
                    }

                    //debug!("Evaluate join between {:?} and {:?}", plan1_id, plan2_id);

                    let (plan1, props1, _) = pop_graph.get3(plan1_id);
                    let (plan2, props2, _) = pop_graph.get3(plan2_id);

                    let join_quns_bitmap = props1.quns.bitmap | props2.quns.bitmap;

                    // Is there a join predicate between two subplans?
                    // P1.quns should be superset of LHS quns
                    // P2.quns should be superset of RHS quns
                    let mut found_equijoin = false;
                    let join_preds: Vec<(ExprId, PredicateType)> = pred_map
                        .iter()
                        .filter_map(|(pred_id, (pred_type, quncols_bitset, quns_bitset))| {
                            let pred_quns_bitmap = quns_bitset.bitmap;
                            let is_subset = (pred_quns_bitmap & join_quns_bitmap) == pred_quns_bitmap;
                            if is_subset {
                                if *pred_type == PredicateType::EquiJoin {
                                    found_equijoin = true
                                }
                                Some((*pred_id, *pred_type))
                            } else {
                                None
                            }
                        })
                        .collect();

                    if join_preds.len() > 0 && found_equijoin {
                        debug!(
                            "Equijoin between {:?} and {:?}, preds: {:?}",
                            props1.quns.elements(),
                            props2.quns.elements(),
                            &join_preds
                        );

                        let mut join_bitset = all_preds_bitset.clone().clear();
                        for (pred_id, pred_type) in join_preds {
                            join_bitset.set(pred_id);
                            pred_map.remove_entry(&pred_id);
                        }

                        let mut props = props1.union(&props2);
                        props.preds.bitmap = props.preds.bitmap | join_bitset.bitmap;

                        let join_node =
                            pop_graph.add_node_with_props(POP::HashJoin, props, Some(vec![plan1_id, plan2_id]));
                        join_status = Some((ix1, ix2, join_node));

                        break 'outer;
                    }
                }
            }

            if let Some((ix1, ix2, join_node)) = join_status {
                worklist.remove(ix1);
                worklist.remove(ix2 - 1);
                worklist.push(join_node);
            } else {
                panic!("No join found!!!")
            }
        }

        assert!(worklist.len() == 1);
        let root_pop_id = worklist[0];

        let plan_filename = format!("{}/{}", GRAPHVIZDIR, "plan.dot");
        APS::write_plan_to_graphviz(&pop_graph, root_pop_id, &plan_filename);
        Ok(())
    }

    pub fn get_expr_quncols(graph: &ExprGraph, root_node_id: ExprId) -> HashSet<QunCol> {
        let qun_col_pairs: HashSet<QunCol> = graph
            .iter(root_node_id)
            .filter_map(|nodeid| {
                if let Column { qunid, colid, .. } = &graph.get(nodeid).contents {
                    Some(QunCol(*qunid, *colid))
                } else {
                    None
                }
            })
            .collect();
        qun_col_pairs
    }

    pub fn get_expr_quns(graph: &ExprGraph, root_node_id: ExprId) -> HashSet<QunId> {
        let qun_col_pairs = Self::get_expr_quncols(graph, root_node_id);
        qun_col_pairs.iter().map(|quncol| quncol.0).collect()
    }
}

impl APS {
    pub(crate) fn write_plan_to_graphviz(pop_graph: &POPGraph, pop_id: POPId, filename: &str) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        Self::write_pop_to_graphviz(pop_graph, pop_id, &mut file);

        fprint!(file, "}}\n");

        drop(file);

        let ofilename = format!("{}.jpg", filename);
        let oflag = format!("-o{}.jpg", filename);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(filename)
            .status()
            .expect("failed to execute process");

        Ok(())
    }

    fn nodeid_to_str(nodeid: &POPId) -> String {
        format!("{:?}", nodeid).replace("(", "").replace(")", "")
    }

    pub(crate) fn write_pop_to_graphviz(pop_graph: &POPGraph, pop_id: POPId, file: &mut File) -> std::io::Result<()> {
        let id = Self::nodeid_to_str(&pop_id);
        let (pop, props, children) = pop_graph.get3(pop_id);

        if let Some(children) = children {
            for &childid in children.iter().rev() {
                let childid_name = Self::nodeid_to_str(&childid);
                fprint!(file, "    popnode{} -> popnode{};\n", childid_name, id);
                Self::write_pop_to_graphviz(pop_graph, childid, file)?;
            }
        }
        fprint!(file, "    popnode{}[label=\"{:?}|{:?}\"];\n", id, &pop, props.quns.elements());

        Ok(())
    }
}
