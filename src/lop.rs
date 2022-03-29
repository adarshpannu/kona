// APS: Access Path Selection

use bitmaps::Bitmap;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::process::Command;

use crate::bitset::*;
use crate::expr::{Expr::*, *};
use crate::graph::*;
use crate::includes::*;
use crate::qgm::*;

pub type LOPGraph = Graph<LOPKey, LOP, LOPProps>;

const BITMAPLEN: usize = 256;

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

impl LOPKey {
    pub fn printable_key(&self) -> String {
        format!("{:?}", *self).replace("(", "").replace(")", "")
    }

    pub fn printable(&self, lop_graph: &LOPGraph) -> String {
        let lop = &lop_graph.get(*self).value;
        format!("{:?}-{:?}", *lop, *self)
    }
}

#[derive(Debug)]
pub enum LOP {
    TableScan { input_cols: Bitset<QunCol> },
    HashJoin { join_pred: ExprKey },
    NLJoin,
    Sort,
    GroupBy,
}

#[derive(Debug)]
pub struct LOPProps {
    pub quns: Bitset<QunId>,
    pub cols: Bitset<QunCol>, // output cols
    pub preds: Bitset<ExprKey>,
}

impl LOPProps {
    fn new(quns: Bitset<QunId>, cols: Bitset<QunCol>, preds: Bitset<ExprKey>) -> Self {
        LOPProps { quns, cols, preds }
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

impl std::default::Default for LOPProps {
    fn default() -> Self {
        LOPProps {
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

pub struct APSContext {
    all_quncols_bitset: Bitset<QunCol>,
    all_quns_bitset: Bitset<QunId>,
    all_preds_bitset: Bitset<ExprKey>,
    all_quncols: HashSet<QunCol>,
}

impl APSContext {
    fn new(qgm: &QGM) -> Self {
        let mut all_quncols_bitset = Bitset::new();
        let mut all_quns_bitset = Bitset::new();
        let mut all_preds_bitset = Bitset::new();
        let mut all_quncols = HashSet::new();

        for quncol in qgm.iter_quncols() {
            all_quncols_bitset.set(quncol);
            all_quns_bitset.set(quncol.0);
            all_quncols.insert(quncol);
        }
        for expr_key in qgm.iter_preds() {
            all_preds_bitset.set(expr_key);
        }

        APSContext {
            all_quncols_bitset,
            all_quns_bitset,
            all_preds_bitset,
            all_quncols,
        }
    }
}

impl QGM {
    pub fn build_logical_plan(self: &mut QGM) -> Result<(LOPGraph, LOPKey), String> {
        // Construct bitmaps
        let mut aps_context = APSContext::new(self);
        let mut lop_graph: LOPGraph = Graph::new();

        let lop_key = self.build_qblock_logical_plan(self.main_qblock_key, &aps_context, &mut lop_graph);
        if let Ok(lop_key) = lop_key {
            Ok((lop_graph, lop_key))
        } else {
            Err("Something bad happened lol".to_string())
        }
    }

    pub fn build_qblock_logical_plan(
        self: &mut QGM, qblock_key: QueryBlockKey, aps_context: &APSContext, lop_graph: &mut LOPGraph,
    ) -> Result<LOPKey, String> {
        //let graph = replace(&mut qgm.graph, Graph::new());
        let graph = &self.expr_graph;

        let qblock = &self.qblock_graph.get(qblock_key).value;
        let mut worklist: Vec<LOPKey> = vec![];

        assert!(self.cte_list.len() == 0);

        let all_quncols_bitset = &aps_context.all_quncols_bitset;
        let all_quns_bitset = &aps_context.all_quns_bitset;
        let all_preds_bitset = &aps_context.all_preds_bitset;

        let mut select_list_quncol_bitset = all_quncols_bitset.clone().clear(); // shallow copy

        // Process select-list: Collect all QunCols
        qblock
            .select_list
            .iter()
            .flat_map(|ne| ne.expr_key.iter_quncols(&graph))
            .for_each(|quncol| select_list_quncol_bitset.set(quncol));

        //debug!("select_list_quncols: {:?}", select_list_quncol_bitset.printable(self));

        // Process predicates:
        // 1. Classify predicates: pred -> type
        // 2. Collect QunCols for each predicates: pred -> set(QunCol)
        // 3. Collect quns for each predicates: pred -> set(Qun)
        let mut pred_map: HashMap<ExprKey, (PredicateType, Bitset<QunCol>, Bitset<QunId>)> = HashMap::new();

        if let Some(pred_list) = qblock.pred_list.as_ref() {
            for &pred_key in pred_list.iter() {
                let mut quncols_bitset = all_quncols_bitset.clone().clear();
                let mut quns_bitset = all_quns_bitset.clone().clear();

                for quncol in pred_key.iter_quncols(&graph) {
                    quncols_bitset.set(quncol);
                    quns_bitset.set(quncol.0);
                }

                let mut pred_type;
                if quns_bitset.len() == 0 {
                    // Constant
                    pred_type = PredicateType::Constant
                } else if quns_bitset.len() == 1 {
                    pred_type = PredicateType::Local
                } else {
                    // Join
                    let expr = graph.get(pred_key);
                    if let RelExpr(RelOp::Eq) = expr.value {
                        let children = expr.children.as_ref().unwrap();
                        let (left_child_key, right_child_key) = (children[0], children[1]);
                        let left_quns: HashSet<QunId> = left_child_key.iter_quns(&graph).collect();
                        let right_quns: HashSet<QunId> = right_child_key.iter_quns(&graph).collect();
                        if left_quns.intersection(&right_quns).collect::<HashSet<_>>().len() == 0 {
                            pred_type = PredicateType::EquiJoin
                        } else {
                            pred_type = PredicateType::Other
                        }
                    } else {
                        pred_type = PredicateType::Other
                    }
                };
                /*
                debug!(
                    "Predicate {:?}: {}, quns={:?}, type={:?}",
                    pred_key,
                    pred_key.printable(&graph, false),
                    &quns_bitset,
                    pred_type
                );
                */
                pred_map.insert(pred_key, (pred_type, quncols_bitset, quns_bitset));
            }
        }

        // Build unary POPs first
        for qun in qblock.quns.iter() {
            //debug!("Build TableScan: {:?} id={}", qun.display(), qun.id);

            // Set quns
            let mut quns_bitset = all_quns_bitset.clone().clear();
            quns_bitset.set(qun.id);

            // Set input cols: find all column references for this qun
            let mut input_quncols_bitset = all_quncols_bitset.clone().clear();
            aps_context
                .all_quncols
                .iter()
                .filter(|&quncol| quncol.0 == qun.id)
                .for_each(|&quncol| input_quncols_bitset.set(quncol));

            /*
            debug!(
                "input_quncols_bitset for qun = {}: {:?}",
                qun.id,
                input_quncols_bitset.printable(self)
            );
            */

            // Set output cols + preds
            let mut unbound_quncols_bitset = select_list_quncol_bitset.clone();

            let mut preds_bitset = all_preds_bitset.clone().clear();
            pred_map
                .iter()
                .for_each(|(&pred_key, (pred_type, quncols_bitset, quns_bitset))| {
                    if quns_bitset.get(qun.id) {
                        if quns_bitset.bitmap.len() == 1 {
                            // Set preds: find local preds that refer to this qun
                            preds_bitset.set(pred_key);
                        } else {
                            // Set output columns: Only project cols in the select-list + unbound join preds
                            unbound_quncols_bitset.bitmap = unbound_quncols_bitset.bitmap | quncols_bitset.bitmap;
                        }
                    }
                });

            let mut output_quncols_bitset = select_list_quncol_bitset.clone();
            output_quncols_bitset.bitmap = unbound_quncols_bitset.bitmap & input_quncols_bitset.bitmap;

            for pred_key in preds_bitset.elements().iter() {
                pred_map.remove_entry(pred_key);
            }

            // Set props
            let props = LOPProps::new(quns_bitset, output_quncols_bitset, preds_bitset);
            /*
            debug!(
                "LOP quns={:?}, cols={:?}, preds={:?}",
                &props.quns.elements(),
                &props.cols.elements(),
                &props.preds.elements()
            );
            */

            let lopkey = lop_graph.add_node_with_props(
                LOP::TableScan {
                    input_cols: input_quncols_bitset,
                },
                props,
                None,
            );

            worklist.push(lopkey);
        }

        // Run greedy join enumeration
        let n = qblock.quns.len();
        for ix in (2..=n).rev() {
            let mut join_status = None;

            // Make pairs of all plans in work-list
            'outer: for (ix1, &plan1_key) in worklist.iter().enumerate() {
                for (ix2, &plan2_key) in worklist.iter().enumerate() {
                    if plan1_key == plan2_key {
                        continue;
                    }

                    let (plan1, props1, _) = lop_graph.get3(plan1_key);
                    let (plan2, props2, _) = lop_graph.get3(plan2_key);

                    let join_quns_bitmap = props1.quns.bitmap | props2.quns.bitmap;

                    // Is there a join predicate between two subplans?
                    // P1.quns should be superset of LHS quns
                    // P2.quns should be superset of RHS quns
                    let mut found_equijoin = false;
                    let join_preds: Vec<(ExprKey, PredicateType)> = pred_map
                        .iter()
                        .filter_map(|(pred_key, (pred_type, quncols_bitset, quns_bitset))| {
                            let pred_quns_bitmap = quns_bitset.bitmap;
                            let is_subset = (pred_quns_bitmap & join_quns_bitmap) == pred_quns_bitmap;
                            if is_subset {
                                if *pred_type == PredicateType::EquiJoin {
                                    found_equijoin = true
                                }
                                Some((*pred_key, *pred_type))
                            } else {
                                None
                            }
                        })
                        .collect();

                    if join_preds.len() > 0 && found_equijoin {
                        let mut join_bitset = all_preds_bitset.clone().clear();

                        // Pick any one of the join predicates to use for the actual join
                        let mut join_pred = join_preds
                            .iter()
                            .filter_map(|(pred_key, pred_type)| {
                                if *pred_type == PredicateType::EquiJoin {
                                    Some(*pred_key)
                                } else {
                                    None
                                }
                            })
                            .next()
                            .unwrap();

                        for (pred_key, pred_type) in join_preds {
                            join_bitset.set(pred_key);
                            pred_map.remove_entry(&pred_key);
                        }

                        let mut props = props1.union(&props2);
                        props.preds.bitmap = join_bitset.bitmap;

                        // Compute cols to flow through. Retain all cols in the select-list + unbound preds
                        let mut colbitmap = select_list_quncol_bitset.clone().bitmap;
                        for (_, (_, pred_quncol_bitmap, _)) in pred_map.iter() {
                            colbitmap = colbitmap | pred_quncol_bitmap.bitmap;
                        }
                        props.cols.bitmap = (props.cols.bitmap & colbitmap);

                        let join_node = lop_graph.add_node_with_props(
                            LOP::HashJoin { join_pred },
                            props,
                            Some(vec![plan1_key, plan2_key]),
                        );
                        join_status = Some((plan1_key, plan2_key, join_node));

                        // For now, go with the first equi-join
                        break 'outer;
                    }
                }
            }

            if let Some((plan1_key, plan2_key, join_node)) = join_status {
                worklist.retain(|&elem| (elem != plan1_key && elem != plan2_key));
                worklist.push(join_node);
            } else {
                panic!("No join found!!!")
            }
        }

        if worklist.len() == 1 {
            let root_lop_key = worklist[0];

            let plan_filename = format!("{}/{}", GRAPHVIZDIR, "lop.dot");
            self.write_logical_plan_to_graphviz(&lop_graph, root_lop_key, &plan_filename);
            Ok(root_lop_key)
        } else {
            Err("Cannot find plan for qblock".to_string())
        }
    }

    pub fn write_logical_plan_to_graphviz(
        self: &QGM, lop_graph: &LOPGraph, lop_key: LOPKey, filename: &str,
    ) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        self.write_lop_to_graphviz(lop_graph, lop_key, &mut file);

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

    pub fn write_lop_to_graphviz(
        self: &QGM, lop_graph: &LOPGraph, lop_key: LOPKey, file: &mut File,
    ) -> std::io::Result<()> {
        let id = lop_key.printable_key();
        let (lop, props, children) = lop_graph.get3(lop_key);

        if let Some(children) = children {
            for &child_key in children.iter().rev() {
                let child_name = child_key.printable_key();
                fprint!(file, "    lopkey{} -> lopkey{};\n", child_name, id);
                self.write_lop_to_graphviz(lop_graph, child_key, file)?;
            }
        }
        let colstring = props.cols.printable(self);
        let predstring = props.preds.printable(self, true);

        let (label, extrastr) = match &lop {
            LOP::TableScan { input_cols } => {
                let input_cols = input_cols.printable(self);
                let input_cols = format!("(input = {})", input_cols);
                (String::from("TableScan"), input_cols)
            }
            LOP::HashJoin { join_pred } => {
                let join_pred = join_pred.printable(&self.expr_graph, true);
                let join_pred = format!("(joinpred = {})", join_pred);
                (String::from("HashJoin"), join_pred)
            }
            _ => (format!("{:?}", &lop), String::from("")),
        };

        fprint!(
            file,
            "    lopkey{}[label=\"{}|{:?}|{}|{}|{}\"];\n",
            id,
            label,
            props.quns.elements(),
            colstring,
            predstring,
            extrastr
        );

        Ok(())
    }
}

impl Bitset<QunCol> {
    fn printable(&self, qgm: &QGM) -> String {
        let cols = self
            .elements()
            .iter()
            .map(|&quncol| qgm.metadata.get_colname(quncol))
            .collect::<Vec<String>>();
        let mut colstring = String::from("");
        for col in cols {
            colstring.push_str(&col);
            colstring.push_str(" ");
        }
        colstring
    }
}

impl Bitset<ExprKey> {
    fn printable(&self, qgm: &QGM, do_escape: bool) -> String {
        let mut predstring = String::from("{");
        let preds = self.elements();
        for (ix, &pred_key) in preds.iter().enumerate() {
            let predstr = pred_key.printable(&qgm.expr_graph, do_escape);
            predstring.push_str(&predstr);
            if ix < preds.len() - 1 {
                predstring.push_str("|")
            }
        }
        predstring.push_str("}");
        predstring
    }
}
