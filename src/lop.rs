// LOP: Logical operators

use bimap::BiMap;
use partitions::PartitionVec;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::process::Command;

use crate::bitset::*;
use crate::expr::{Expr::*, *};
use crate::graph::*;
use crate::includes::*;
use crate::metadata::*;
use crate::qgm::*;
use regex::Regex;

pub type LOPGraph = Graph<LOPKey, LOP, LOPProps>;

/***************************************************************************************************/
impl LOPKey {
    pub fn printable_key(&self) -> String {
        format!("{:?}", *self).replace("(", "").replace(")", "")
    }

    pub fn printable(&self, lop_graph: &LOPGraph) -> String {
        let lop = &lop_graph.get(*self).value;
        format!("{:?}-{:?}", *lop, *self)
    }

    pub fn printable_id(&self) -> String {
        let re1 = Regex::new(r"^.*\(").unwrap();
        let re2 = Regex::new(r"\).*$").unwrap();

        let id = format!("{:?}", *self);
        let id = re1.replace_all(&id, "");
        let id = re2.replace_all(&id, "");
        id.to_string()
    }
}

/***************************************************************************************************/
#[derive(Debug)]
pub enum LOP {
    TableScan { input_cols: Bitset<QunCol> },
    HashJoin { equi_join_preds: Vec<(ExprKey, PredicateAlignment)> },
    Repartition { cpartitions: usize },
    Aggregation { group_by_len: usize },
}

/***************************************************************************************************/
#[derive(Debug, Clone)]
pub struct EmitCol {
    pub quncol: QunCol,
    pub expr_key: ExprKey,
}

#[derive(Debug, Clone)]
pub struct LOPProps {
    pub quns: Bitset<QunId>,
    pub cols: Bitset<QunCol>,
    pub preds: Bitset<ExprKey>,
    pub partdesc: PartDesc,
    pub emitcols: Option<Vec<EmitCol>>,
}

impl LOPProps {
    fn new(quns: Bitset<QunId>, cols: Bitset<QunCol>, preds: Bitset<ExprKey>, partdesc: PartDesc, emitcols: Option<Vec<EmitCol>>) -> Self {
        LOPProps {
            quns,
            cols,
            preds,
            partdesc,
            emitcols,
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PredicateType {
    Constant, // 1 = 2
    Local,    // t.col1 = t.col2 + 20
    EquiJoin, // r.col1 + 10 = s.col2 + 20
    Other,    // r.col1 > s.col1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// PredicateAlignment: Are the given equijoin predicate legs (lhs/rhs) aligned with subplan's legs
pub enum PredicateAlignment {
    Inapplicable,
    Aligned,  // r1 = s1 in the context of R join S
    Reversed, // s1 = r1 in the context of S join R
}

/***************************************************************************************************/
pub struct APSContext {
    all_quncols: Bitset<QunCol>,
    all_quns: Bitset<QunId>,
    all_preds: Bitset<ExprKey>,
}

impl APSContext {
    fn new(qgm: &QGM) -> Self {
        let mut all_quncols = Bitset::new();
        let mut all_quns = Bitset::new();
        let mut all_preds = Bitset::new();

        for quncol in qgm.iter_quncols() {
            all_quncols.set(quncol);
            all_quns.set(quncol.0);
        }
        for expr_key in qgm.iter_toplevel_exprs() {
            all_preds.set(expr_key);
        }

        APSContext {
            all_quncols,
            all_quns,
            all_preds,
        }
    }
}

/***************************************************************************************************/
type PredMap = HashMap<ExprKey, PredDesc>;

pub struct EqJoinDesc {
    lhs_quns: Bitset<QunId>,
    rhs_quns: Bitset<QunId>,
}

pub struct PredDesc {
    quncols: Bitset<QunCol>,
    quns: Bitset<QunId>,
    eqjoin_desc: Option<Box<EqJoinDesc>>,
}

pub struct ExprEqClass {
    next_id: usize,
    expr_id_map: BiMap<ExprKey, usize>,
    disjoint_sets: PartitionVec<ExprKey>,
}

impl ExprEqClass {
    pub fn new() -> Self {
        ExprEqClass {
            next_id: 0,
            expr_id_map: BiMap::new(),
            disjoint_sets: PartitionVec::with_capacity(64),
        }
    }

    pub fn set_eq(&mut self, expr1: ExprKey, expr2: ExprKey) {
        let id1 = self.key2id(expr1);
        let id2 = self.key2id(expr2);
        self.disjoint_sets.union(id1, id2);
    }

    pub fn check_eq(&self, _expr_graph: &ExprGraph, expr1: ExprKey, expr2: ExprKey) -> bool {
        if let Some(&id1) = self.expr_id_map.get_by_left(&expr1) {
            if let Some(&id2) = self.expr_id_map.get_by_left(&expr2) {
                return self.disjoint_sets.same_set(id1, id2);
            }
        }
        return false;
    }

    fn key2id(&mut self, expr_key: ExprKey) -> usize {
        if let Some(&id) = self.expr_id_map.get_by_left(&expr_key) {
            id
        } else {
            let id = self.next_id;
            self.expr_id_map.insert(expr_key, id);
            self.next_id = self.next_id + 1;
            self.disjoint_sets.push(expr_key);
            id
        }
    }
}
#[test]
fn sizes() {
    dbg!(std::mem::size_of::<PredDesc>());
}

impl QGM {
    pub fn build_logical_plan(self: &mut QGM, env: &Env) -> Result<(LOPGraph, LOPKey), String> {
        // Construct bitmaps
        let aps_context = APSContext::new(self);
        let mut lop_graph: LOPGraph = Graph::new();

        let lop_key = self.build_qblock_logical_plan(env, 0, self.main_qblock_key, &aps_context, &mut lop_graph, None);
        if let Ok(lop_key) = lop_key {
            let plan_pathname = format!("{}/{}", env.output_dir, "lop.dot");
            self.write_logical_plan_to_graphviz(&lop_graph, lop_key, &plan_pathname)?;
            Ok((lop_graph, lop_key))
        } else {
            Err("Something bad happened lol".to_string())
        }
    }

    pub fn classify_predicate(eqjoin_desc: &EqJoinDesc, lhs_props: &LOPProps, rhs_props: &LOPProps) -> (PredicateType, PredicateAlignment) {
        let (lhs_pred_quns, rhs_pred_quns) = (&eqjoin_desc.lhs_quns, &eqjoin_desc.rhs_quns);

        // pred-quns must be subset of plan quns
        if lhs_pred_quns.is_subset_of(&lhs_props.quns) && rhs_pred_quns.is_subset_of(&rhs_props.quns) {
            (PredicateType::EquiJoin, PredicateAlignment::Aligned)
        } else if lhs_pred_quns.is_subset_of(&rhs_props.quns) && rhs_pred_quns.is_subset_of(&lhs_props.quns) {
            // Swapped scenario
            (PredicateType::EquiJoin, PredicateAlignment::Reversed)
        } else {
            (PredicateType::Other, PredicateAlignment::Inapplicable)
        }
    }

    fn collect_selectlist_quncols(&self, aps_context: &APSContext, qblock: &QueryBlock) -> Bitset<QunCol> {
        let mut select_list_quncol = aps_context.all_quncols.clone_n_clear();
        qblock
            .select_list
            .iter()
            .flat_map(|ne| ne.expr_key.iter_quncols(&self.expr_graph))
            .for_each(|quncol| select_list_quncol.set(quncol));
        select_list_quncol
    }

    fn collect_preds(&self, aps_context: &APSContext, qblock: &QueryBlock) -> (PredMap, ExprEqClass) {
        let expr_graph = &self.expr_graph;
        let mut pred_map: PredMap = HashMap::new();

        let mut eqclass = ExprEqClass::new();
        let mut eqpred_legs = vec![];

        if let Some(pred_list) = qblock.pred_list.as_ref() {
            for &pred_key in pred_list.iter() {
                // Collect quns and quncols for each predicate
                let mut quncols = aps_context.all_quncols.clone_n_clear();
                let mut quns = aps_context.all_quns.clone_n_clear();

                for quncol in pred_key.iter_quncols(&self.expr_graph) {
                    quncols.set(quncol);
                    quns.set(quncol.0);
                }

                // For equijoin candidates, collect lhs and rhs quns
                let expr = expr_graph.get(pred_key);
                let eqjoin_desc = if let RelExpr(RelOp::Eq) = expr.value {
                    let children = expr.children.as_ref().unwrap();
                    let (lhs_child_key, rhs_child_key) = (children[0], children[1]);
                    let lhs_quns = aps_context.all_quns.clone_n_clear().init(lhs_child_key.iter_quns(expr_graph));
                    let rhs_quns = aps_context.all_quns.clone_n_clear().init(rhs_child_key.iter_quns(expr_graph));

                    if lhs_quns.len() > 0 && rhs_quns.len() > 0 {
                        let (lhs_hash, rhs_hash) = (lhs_child_key.hash(expr_graph), rhs_child_key.hash(expr_graph));
                        eqpred_legs.push((lhs_hash, lhs_child_key));
                        eqpred_legs.push((rhs_hash, rhs_child_key));
                        eqclass.set_eq(lhs_child_key, rhs_child_key);
                        Some(Box::new(EqJoinDesc { lhs_quns, rhs_quns }))
                    } else {
                        None
                    }
                } else {
                    None
                };
                pred_map.insert(pred_key, PredDesc { quncols, quns, eqjoin_desc });
            }
        }

        // SELECT expressions are also added to eq-class. These come into play when determining partitioning.
        for ne in qblock.select_list.iter() {
            let expr_key = ne.expr_key;
            let expr_hash = expr_key.hash(expr_graph);

            eqpred_legs.push((expr_hash, expr_key));
        }

        let mut visited = vec![false; eqpred_legs.len()];

        // Go over all eqjoin legs and equate all the ones that are structurally equivalent
        for (ix, &(expr_hash1, expr_key1)) in eqpred_legs.iter().enumerate() {
            if !visited[ix] {
                for (jx, &(expr_hash2, expr_key2)) in eqpred_legs.iter().enumerate() {
                    if jx > ix && expr_hash1 == expr_hash2 && Expr::isomorphic(expr_graph, expr_key1, expr_key1) {
                        eqclass.set_eq(expr_key1, expr_key2);
                        visited[jx] = true;
                    }
                }
            }
        }

        (pred_map, eqclass)
    }

    pub fn build_unary_plans(
        self: &QGM, env: &Env, aps_context: &APSContext, qblock: &QueryBlock, lop_graph: &mut LOPGraph, pred_map: &mut PredMap,
        select_list_quncol: &Bitset<QunCol>, worklist: &mut Vec<LOPKey>,
    ) -> Result<(), String> {
        let APSContext {
            all_quncols,
            all_quns,
            all_preds,
        } = aps_context;

        // Build unary POPs first
        for qun in qblock.quns.iter() {
            // Set quns
            let mut quns = all_quns.clone_n_clear();
            quns.set(qun.id);

            // Set input cols: find all column references for this qun
            let mut input_quncols = all_quncols.clone_n_clear();
            aps_context
                .all_quncols
                .elements()
                .iter()
                .filter(|&quncol| quncol.0 == qun.id)
                .for_each(|&quncol| input_quncols.set(quncol));

            // Set output cols + preds
            let mut unbound_quncols = select_list_quncol.clone();

            let mut preds = all_preds.clone_n_clear();
            pred_map.iter().for_each(|(&pred_key, PredDesc { quncols, quns, .. })| {
                if quns.get(qun.id) {
                    if quns.bitmap.len() == 1 {
                        // Set preds: find local preds that refer to this qun
                        preds.set(pred_key);
                    } else {
                        // Set output columns: Only project cols in the select-list + unbound join preds
                        unbound_quncols.bitmap = unbound_quncols.bitmap | quncols.bitmap;
                    }
                }
            });

            let mut output_quncols = select_list_quncol.clone();
            output_quncols.bitmap = unbound_quncols.bitmap & input_quncols.bitmap;

            // Remove all preds that will run on this tablescan as they've been bound already
            for pred_key in preds.elements().iter() {
                pred_map.remove_entry(pred_key);
            }

            // Build plan for nested query blocks
            let lopkey = if qblock.qbtype == QueryBlockType::GroupBy {
                let child_qblock_key = qun.qblock.unwrap();
                let child_qblock = &self.qblock_graph.get(child_qblock_key).value;
                let group_by_len = qblock.group_by.as_ref().unwrap().len();
                let expected_partitioning = child_qblock.select_list.iter().take(group_by_len).map(|ne| ne.expr_key).collect::<Vec<_>>();

                // child == subplan that we'll be aggregating.
                for e in expected_partitioning.iter() {
                    debug!("expected: {:?}", e.printable(&self.expr_graph, false))
                }

                let child_lop_key = self.build_qblock_logical_plan(env, qun.id, child_qblock_key, aps_context, lop_graph, Some(expected_partitioning))?;

                let children = Some(vec![child_lop_key]);

                let partdesc = lop_graph.get(child_lop_key).properties.partdesc.clone();
                let props = LOPProps::new(quns, output_quncols, preds, partdesc, None);
                lop_graph.add_node_with_props(LOP::Aggregation { group_by_len }, props, children)
            } else {
                let npartitions = if let Some(tabledesc) = qun.tabledesc.as_ref() {
                    tabledesc.get_part_desc().npartitions
                } else {
                    env.options.parallel_degree.unwrap_or(1) * 2 // todo: temporary hack to force different partition counts in a plan
                };
                let partdesc = PartDesc::new(npartitions, PartType::RAW);

                let props = LOPProps::new(quns, output_quncols, preds, partdesc, None);

                lop_graph.add_node_with_props(LOP::TableScan { input_cols: input_quncols }, props, None)
            };
            //debug!("Build TableScan: key={:?} {:?} id={}", lopkey, qun.display(), qun.id);
            worklist.push(lopkey);
        }
        Ok(())
    }

    pub fn build_qblock_logical_plan(
        self: &QGM, env: &Env, parent_qun_id: QunId, qblock_key: QueryBlockKey, aps_context: &APSContext, lop_graph: &mut LOPGraph,
        expected_partitioning: Option<Vec<ExprKey>>,
    ) -> Result<LOPKey, String> {
        let qblock = &self.qblock_graph.get(qblock_key).value;
        let mut worklist: Vec<LOPKey> = vec![];

        assert!(self.cte_list.len() == 0);

        let APSContext {
            all_quncols: _,
            all_quns: _,
            all_preds,
        } = aps_context;

        // Process select-list: Collect all QunCols
        let select_list_quncol = self.collect_selectlist_quncols(aps_context, qblock);

        // Process predicates: Collect quns, quncols. Also collect lhs/rhs quns for equi-join candidates
        let (mut pred_map, eqclass) = self.collect_preds(aps_context, qblock);

        // Build unary plans first (i.e. baseline single table scans)
        self.build_unary_plans(env, aps_context, qblock, lop_graph, &mut pred_map, &select_list_quncol, &mut worklist)?;

        // Run greedy join enumeration
        let n = qblock.quns.len();
        for _ix in (2..=n).rev() {
            let mut join_status = None;

            // Iterate over pairs of all plans in work-list
            'outer: for (_ix1, &lhs_plan_key) in worklist.iter().enumerate() {
                for (_ix2, &rhs_plan_key) in worklist.iter().enumerate() {
                    if lhs_plan_key == rhs_plan_key {
                        continue;
                    }

                    let lhs_props = &lop_graph.get(lhs_plan_key).properties;
                    let rhs_props = &lop_graph.get(rhs_plan_key).properties;

                    let join_quns_bitmap = lhs_props.quns.bitmap | rhs_props.quns.bitmap;

                    // Are there any join predicates between two subplans?
                    // P1.quns should be superset of LHS quns
                    // P2.quns should be superset of RHS quns
                    let join_preds = pred_map
                        .iter()
                        .filter_map(|(pred_key, pred_desc)| {
                            let PredDesc { quns, .. } = pred_desc;
                            let pred_quns_bitmap = quns.bitmap;
                            let is_subset = (pred_quns_bitmap & join_quns_bitmap) == pred_quns_bitmap;
                            if is_subset {
                                Some(*pred_key)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    // Only select equality predicates (hash/merge joins only)
                    let mut equi_join_preds = join_preds
                        .iter()
                        .filter_map(|&pred_key| {
                            if let Some(eqjoin_desc) = pred_map.get(&pred_key).unwrap().eqjoin_desc.as_ref() {
                                let join_class = Self::classify_predicate(&*eqjoin_desc, lhs_props, rhs_props);
                                if join_class.0 == PredicateType::EquiJoin {
                                    Some((pred_key, join_class.1))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    // Sort preds since the preceding hash-based ordering can be random
                    equi_join_preds.sort_by(|a, b| a.0.cmp(&b.0));

                    if equi_join_preds.len() > 0 {
                        let mut preds = all_preds.clone_n_clear();

                        for &pred_key in join_preds.iter() {
                            preds.set(pred_key);
                            pred_map.remove_entry(&pred_key);
                        }

                        // Initialize join properties
                        let mut quns = lhs_props.quns.clone();
                        quns.bitmap |= rhs_props.quns.bitmap;

                        let mut cols = lhs_props.cols.clone();
                        cols.bitmap |= rhs_props.cols.bitmap;

                        // Compute cols to flow through. Retain all cols in the select-list + unbound preds
                        let mut colbitmap = select_list_quncol.clone().bitmap;
                        for (_, PredDesc { quncols, .. }) in pred_map.iter() {
                            colbitmap = colbitmap | quncols.bitmap;
                        }
                        cols.bitmap = cols.bitmap & colbitmap;

                        // Repartition join legs as needed
                        let (lhs_partdesc, rhs_partdesc, _npartitions) =
                            Self::harmonize_partitions(env, lop_graph, lhs_plan_key, rhs_plan_key, &self.expr_graph, &equi_join_preds, &eqclass);

                        let lhs_repart_props = lhs_partdesc.map(|partdesc| LOPProps {
                            quns: lhs_props.quns.clone(),
                            cols: lhs_props.cols.clone(),
                            preds: lhs_props.preds.clone_n_clear(),
                            partdesc,
                            emitcols: None,
                        });
                        let rhs_repart_props = rhs_partdesc.map(|partdesc| LOPProps {
                            quns: rhs_props.quns.clone(),
                            cols: rhs_props.cols.clone(),
                            preds: rhs_props.preds.clone_n_clear(),
                            partdesc,
                            emitcols: None,
                        });

                        //let (lhs_partitions, rhs_partitions) = (npartitions, npartitions);
                        let (lhs_partitions, rhs_partitions) = (lhs_props.partdesc.npartitions, rhs_props.partdesc.npartitions);

                        let new_lhs_plan_key = if let Some(lhs_repart_props) = lhs_repart_props {
                            // Repartition LHS
                            lop_graph.add_node_with_props(LOP::Repartition { cpartitions: lhs_partitions }, lhs_repart_props, Some(vec![lhs_plan_key]))
                        } else {
                            lhs_plan_key
                        };
                        let new_rhs_plan_key = if let Some(rhs_repart_props) = rhs_repart_props {
                            // Repartition RHS
                            lop_graph.add_node_with_props(LOP::Repartition { cpartitions: rhs_partitions }, rhs_repart_props, Some(vec![rhs_plan_key]))
                        } else {
                            rhs_plan_key
                        };

                        // Join partitioning is identical to partitioning of children.
                        let lhs_props = &lop_graph.get(new_lhs_plan_key).properties;
                        let partdesc = lhs_props.partdesc.clone();

                        let props = LOPProps::new(quns, cols, preds, partdesc, None);

                        let join_node = lop_graph.add_node_with_props(LOP::HashJoin { equi_join_preds }, props, Some(vec![new_lhs_plan_key, new_rhs_plan_key]));
                        join_status = Some((lhs_plan_key, rhs_plan_key, join_node));

                        // For now, go with the first equi-join
                        break 'outer;
                    }
                }
            }

            if let Some((plan1_key, plan2_key, join_node)) = join_status {
                worklist.retain(|&elem| (elem != plan1_key && elem != plan2_key));
                worklist.insert(0, join_node);
            } else {
                panic!("No join found!!!")
            }
        }

        if worklist.len() == 1 {
            let mut root_lop_key = worklist[0];

            if let Some(expected_partitioning) = expected_partitioning {
                let props = &lop_graph.get(root_lop_key).properties;
                let empty_vec = vec![];
                let actual_partitioning = if let PartType::HASHEXPR(keys) = &props.partdesc.part_type {
                    keys
                } else {
                    &empty_vec
                };

                if !Self::is_correctly_partitioned(&self.expr_graph, &expected_partitioning, actual_partitioning, &eqclass) {
                    let partdesc = PartDesc {
                        npartitions: 4,
                        part_type: PartType::HASHEXPR(expected_partitioning),
                    };
                    let cpartitions = props.partdesc.npartitions;

                    let props = LOPProps {
                        quns: props.quns.clone(),
                        cols: props.cols.clone(),
                        preds: props.preds.clone_n_clear(),
                        partdesc,
                        emitcols: None,
                    };
                    root_lop_key = lop_graph.add_node_with_props(LOP::Repartition { cpartitions }, props, Some(vec![root_lop_key]));
                }
            }

            /*
            let mut props = lop_graph.get(root_lop_key).properties.clone();
            props.quns = props.quns.clear();
            props.cols = props.cols.clear();
            props.preds = props.preds.clear();

            // Add emitter
            let emitcols = qblock.select_list.clone();
            let root_lop_key = lop_graph.add_node_with_props(LOP::Emit { emitcols }, props, Some(vec![root_lop_key]));
            */
            let mut props = &mut lop_graph.get_mut(root_lop_key).properties;
            let emitcols = qblock
                .select_list
                .iter()
                .enumerate()
                .map(|(ix, ne)| EmitCol {
                    quncol: QunCol(parent_qun_id, ix),
                    expr_key: ne.expr_key,
                })
                .collect::<Vec<_>>();
            props.emitcols = Some(emitcols);

            info!("Created logical plan for qblock id: {}", qblock.id);

            Ok(root_lop_key)
        } else {
            Err("Cannot find plan for qblock".to_string())
        }
    }

    pub fn compute_join_partitioning_keys(expr_graph: &ExprGraph, join_preds: &Vec<(ExprKey, PredicateAlignment)>) -> (Vec<ExprKey>, Vec<ExprKey>) {
        // Compute expected partitioning keys
        let mut lhs_expected_keys = vec![];
        let mut rhs_expected_keys = vec![];
        for &(join_pred_key, alignment) in join_preds.iter() {
            let (expr, _, children) = expr_graph.get3(join_pred_key);
            if let RelExpr(RelOp::Eq) = expr {
                let children = children.unwrap();
                let (lhs_pred_key, rhs_pred_key) = (children[0], children[1]);
                if alignment == PredicateAlignment::Aligned {
                    lhs_expected_keys.push(lhs_pred_key);
                    rhs_expected_keys.push(rhs_pred_key);
                } else if alignment == PredicateAlignment::Reversed {
                    lhs_expected_keys.push(rhs_pred_key);
                    rhs_expected_keys.push(lhs_pred_key);
                } else {
                    assert!(false);
                }
            }
        }
        (lhs_expected_keys, rhs_expected_keys)
    }

    pub fn is_correctly_partitioned(expr_graph: &ExprGraph, expected_keys: &Vec<ExprKey>, actual_keys: &Vec<ExprKey>, eqclass: &ExprEqClass) -> bool {
        if expected_keys.len() == actual_keys.len() {
            actual_keys
                .iter()
                .zip(expected_keys.iter())
                .all(|(key1, key2)| eqclass.check_eq(expr_graph, *key1, *key2))
        } else {
            false
        }
    }

    // harmonize_partitions: Return a triplet indicating whether either/both legs of a join need to be repartitioned
    pub fn harmonize_partitions(
        env: &Env, lop_graph: &LOPGraph, lhs_plan_key: LOPKey, rhs_plan_key: LOPKey, expr_graph: &ExprGraph, join_preds: &Vec<(ExprKey, PredicateAlignment)>,
        eqclass: &ExprEqClass,
    ) -> (Option<PartDesc>, Option<PartDesc>, usize) {
        // Compare expected vs actual partitioning keys on both sides of the join
        // Both sides must be partitioned on equivalent keys and with identical partition counts

        // Compute actual partitioning keys
        let lhs_props = &lop_graph.get(lhs_plan_key).properties;
        let rhs_props = &lop_graph.get(rhs_plan_key).properties;
        let empty_vec = vec![];
        let lhs_actual_keys = if let PartType::HASHEXPR(keys) = &lhs_props.partdesc.part_type {
            keys
        } else {
            &empty_vec
        };
        let rhs_actual_keys = if let PartType::HASHEXPR(keys) = &rhs_props.partdesc.part_type {
            keys
        } else {
            &empty_vec
        };

        // Compute expected partitioning keys
        let (lhs_expected_keys, rhs_expected_keys) = Self::compute_join_partitioning_keys(expr_graph, join_preds);

        // todo: need to ensure #partitions are matched up correctly esp. in light of situations wherein one leg is correctly partitioned while the other isn't
        let lhs_partdesc = if Self::is_correctly_partitioned(expr_graph, &lhs_expected_keys, lhs_actual_keys, eqclass) {
            None
        } else {
            Some(PartDesc {
                npartitions: env.options.parallel_degree.unwrap_or(1),
                part_type: PartType::HASHEXPR(lhs_expected_keys),
            })
        };

        let rhs_partdesc = if Self::is_correctly_partitioned(expr_graph, &rhs_expected_keys, rhs_actual_keys, eqclass) {
            None
        } else {
            Some(PartDesc {
                npartitions: env.options.parallel_degree.unwrap_or(1),
                part_type: PartType::HASHEXPR(rhs_expected_keys),
            })
        };
        (lhs_partdesc, rhs_partdesc, 4)
    }

    pub fn write_logical_plan_to_graphviz(self: &QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pathname: &str) -> Result<(), String> {
        let mut file = std::fs::File::create(pathname).map_err(|err| f!("{:?}: {}", err, pathname))?;

        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        self.write_lop_to_graphviz(lop_graph, lop_key, &mut file)?;

        fprint!(file, "}}\n");

        drop(file);

        let oflag = format!("-o{}.jpg", pathname);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(pathname)
            .status()
            .expect("failed to execute process");

        Ok(())
    }

    pub fn write_lop_to_graphviz(self: &QGM, lop_graph: &LOPGraph, lop_key: LOPKey, file: &mut File) -> Result<(), String> {
        let id = lop_key.printable_key();
        let (lop, props, children) = lop_graph.get3(lop_key);

        if let Some(children) = children {
            for &child_key in children.iter() {
                let child_name = child_key.printable_key();
                fprint!(file, "    lopkey{} -> lopkey{};\n", child_name, id);
                self.write_lop_to_graphviz(lop_graph, child_key, file)?;
            }
        }
        let colstring = if let Some(emitcols) = props.emitcols.as_ref() {
            let emitcols = emitcols.iter().map(|e| e.expr_key).collect::<Vec<_>>();
            printable_preds(&emitcols, self, true)
        } else {
            props.cols.printable(self)
        };

        let predstring = props.preds.printable(self, true);

        let (label, extrastr) = match &lop {
            LOP::TableScan { input_cols } => {
                let input_cols = input_cols.printable(self);
                let extrastr = format!("(input = {})", input_cols);
                (String::from("TableScan"), extrastr)
            }
            LOP::HashJoin { equi_join_preds } => {
                let equi_join_preds = equi_join_preds.iter().map(|e| e.0).collect::<Vec<_>>();
                let extrastr = printable_preds(&equi_join_preds, self, true);
                (String::from("HashJoin"), extrastr)
            }
            LOP::Repartition { cpartitions } => {
                let extrastr = format!("c = {}", cpartitions);
                (String::from("Repartition"), extrastr)
            }
            LOP::Aggregation { group_by_len } => {
                let extrastr = format!("by_len = {}", group_by_len);
                (String::from("Aggregation"), extrastr)
            }
        };

        fprint!(
            file,
            "    lopkey{}[label=\"{}-{}|{:?}|{}|{}|{}|{}\"];\n",
            id,
            label,
            lop_key.printable_id(),
            props.quns.elements(),
            colstring,
            predstring,
            props.partdesc.printable(&self.expr_graph, true),
            extrastr
        );

        Ok(())
    }
}

impl Bitset<QunCol> {
    fn printable(&self, qgm: &QGM) -> String {
        let cols = self.elements().iter().map(|&quncol| qgm.metadata.get_colname(quncol)).collect::<Vec<String>>();
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
        printable_preds(&self.elements(), qgm, do_escape)
    }
}

fn printable_preds(preds: &Vec<ExprKey>, qgm: &QGM, do_escape: bool) -> String {
    let mut predstring = String::from("{");
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

#[derive(Debug)]
struct Foo {
    id: usize,
    name: String,
}

#[test]
fn partitions_test() {
    let mut partition_vec = PartitionVec::with_capacity(10);
    let foo = Foo {
        id: 1,
        name: "adarsh".to_string(),
    };

    // We can add more elements but this will reallocate.
    partition_vec.push(foo);
}
