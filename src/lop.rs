// lop: Logical operators

use std::collections::HashMap;

use bimap::BiMap;
use partitions::PartitionVec;

use crate::{
    bitset::Bitset,
    expr::{Expr::*, ExprGraph, *},
    graph::{ExprKey, Graph, LOPKey, QueryBlockKey},
    includes::*,
    metadata::{PartDesc, PartType},
    qgm::{Quantifier, QueryBlock, QueryBlockGraph, QueryBlockType},
    QGM,
};

pub type LOPGraph = Graph<LOPKey, LOP, LOPProps>;

/***************************************************************************************************/
impl LOPKey {
    pub fn get_schema(&self, qgm: &mut QGM, lop_graph: &LOPGraph) -> Schema {
        let lopprops = &lop_graph.get(*self).properties;
        let expr_graph = &qgm.expr_graph;
        let mut fields = vec![];

        for quncol in lopprops.cols.elements() {
            let field = qgm.metadata.get_field(quncol);
            if let Some(field) = field {
                fields.push(field.clone())
            }
        }

        if let Some(virtcols) = &lopprops.virtcols {
            for exprkey in virtcols.iter() {
                let field = exprkey.to_field(expr_graph);
                fields.push(field.clone())
            }
        }
        Schema::from(fields)
    }

    pub fn get_types(&self, qgm: &mut QGM, lop_graph: &LOPGraph) -> Vec<DataType> {
        let lopprops = &lop_graph.get(*self).properties;
        let expr_graph = &qgm.expr_graph;
        let mut types = vec![];

        for quncol in lopprops.cols.elements() {
            let typ = qgm.metadata.get_fieldtype(quncol);
            if let Some(typ) = typ {
                types.push(typ.clone())
            }
        }

        if let Some(virtcols) = &lopprops.virtcols {
            for exprkey in virtcols.iter() {
                let typ = exprkey.get_data_type(expr_graph);
                types.push(typ.clone())
            }
        }
        types
    }
}

/***************************************************************************************************/
#[derive(Debug)]
pub enum LOP {
    TableScan { input_projection: Bitset<QunCol> },
    HashJoin { lhs_join_keys: Vec<ExprKey>, rhs_join_keys: Vec<ExprKey> },
    Repartition { cpartitions: usize },
    Aggregation { key_len: usize },
}

/***************************************************************************************************/
pub type VirtCol = ExprKey;

#[derive(Debug, Clone)]
pub struct LOPProps {
    pub quns: Bitset<QunId>,
    pub cols: Bitset<QunCol>,
    pub virtcols: Option<Vec<VirtCol>>,
    pub preds: Bitset<ExprKey>,
    pub partdesc: PartDesc,
}

impl LOPProps {
    fn new(quns: Bitset<QunId>, cols: Bitset<QunCol>, virtcols: Option<Vec<VirtCol>>, preds: Bitset<ExprKey>, partdesc: PartDesc) -> Self {
        LOPProps { quns, cols, preds, partdesc, virtcols }
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
        let mut all_quncols = Bitset::default();
        let mut all_quns = Bitset::default();
        let mut all_preds = Bitset::default();

        for quncol in qgm.iter_quncols() {
            all_quncols.set(quncol);
            all_quns.set(quncol.0);
        }
        let mut panik = false;
        for expr_key in qgm.iter_toplevel_exprs() {
            all_preds.set(expr_key);

            let props = qgm.expr_graph.get_properties(expr_key);
            if matches!(props.data_type(), DataType::Null) {
                let s = expr_key.describe(&qgm.expr_graph, false);
                println!("APSContext::new(): Expression has NULL datatype: {:?}", s);
                panik = true;
            }
        }

        if panik {
            panic!("APSContext::new(): Unresolved datatypes in expression graph.");
        }

        APSContext { all_quncols, all_quns, all_preds }
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
    eqjoin_desc: Option<EqJoinDesc>,
}

pub struct ExprEqClass {
    next_id: usize,
    expr_id_map: BiMap<ExprKey, usize>,
    disjoint_sets: PartitionVec<ExprKey>,
}

impl Default for ExprEqClass {
    fn default() -> Self {
        ExprEqClass { next_id: 0, expr_id_map: BiMap::new(), disjoint_sets: PartitionVec::with_capacity(64) }
    }
}

impl ExprEqClass {
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
        false
    }

    fn key2id(&mut self, expr_key: ExprKey) -> usize {
        if let Some(&id) = self.expr_id_map.get_by_left(&expr_key) {
            id
        } else {
            let id = self.next_id;
            self.expr_id_map.insert(expr_key, id);
            self.next_id += 1;
            self.disjoint_sets.push(expr_key);
            id
        }
    }
}

impl QGM {
    pub fn build_logical_plan(self: &mut QGM, env: &Env) -> Result<(LOPGraph, LOPKey), String> {
        // Construct bitmaps
        let aps_context = APSContext::new(self);
        let mut lop_graph: LOPGraph = Graph::default();

        let main_qblock_key = self.main_qblock_key;
        let (qblock_graph, expr_graph, _) = self.borrow_parts();

        let lop_key = Self::build_qblock_logical_plan(qblock_graph, expr_graph, env, main_qblock_key, &aps_context, &mut lop_graph, None);
        if let Ok(lop_key) = lop_key {
            // Perform any rewrites
            let lop_key = self.qrw_add_repartitioning_keys_to_projections(&mut lop_graph, lop_key);
            let lop_key = self.qrw_pushdown_join_keys(&mut lop_graph, lop_key);

            let plan_pathname = format!("{}/{}", env.output_dir, "lop.dot");
            self.write_logical_plan_to_graphviz(&lop_graph, lop_key, &plan_pathname)?;
            Ok((lop_graph, lop_key))
        } else {
            Err("Something bad happened lol".to_string())
        }
    }

    fn append_virt_cols(lop_graph: &mut LOPGraph, lop_key: LOPKey, newcols: Option<&Vec<VirtCol>>) {
        let mut newcols = newcols.cloned();
        let props = &mut lop_graph.get_mut(lop_key).properties;

        match (&mut props.virtcols, &mut newcols) {
            (Some(origcols), Some(newcols)) => {
                let newcols = newcols.iter().filter(|&x| !newcols.contains(x));
                origcols.extend(newcols)
            }
            (None, Some(_)) => props.virtcols = newcols,
            _ => {}
        }
    }

    pub fn qrw_add_repartitioning_keys_to_projections(self: &QGM, lop_graph: &mut LOPGraph, root_lop_key: LOPKey) -> LOPKey {
        let mut iter = lop_graph.iter(root_lop_key);
        while let Some(lop_key) = iter.next(lop_graph) {
            let lop = lop_graph.get(lop_key);
            if matches!(lop.value, LOP::Repartition { .. }) {
                let child_lop_key = lop.children.as_ref().unwrap()[0];
                let partdesc = &lop.properties.partdesc;
                let virtcols: Option<Vec<VirtCol>> = Self::partdesc_to_virtcols(&self.expr_graph, partdesc);

                // Add partitioning columns to the Repartition projection
                Self::append_virt_cols(lop_graph, lop_key, virtcols.as_ref());

                // Add partitioning columns to the projection of the child node
                Self::append_virt_cols(lop_graph, child_lop_key, virtcols.as_ref());
            }
        }
        root_lop_key
    }

    pub fn qrw_pushdown_join_keys(self: &QGM, lop_graph: &mut LOPGraph, root_lop_key: LOPKey) -> LOPKey {
        let mut iter = lop_graph.iter(root_lop_key);
        while let Some(lop_key) = iter.next(lop_graph) {
            let lop = lop_graph.get(lop_key);
            match &lop.value {
                LOP::HashJoin { lhs_join_keys, rhs_join_keys } => {
                    // Only push down projections that are NOT column references. Singleton columns are already a part of the projection.
                    let lhs_has_columns_only = lhs_join_keys.iter().all(|e| e.is_column(&self.expr_graph));
                    let rhs_has_columns_only = rhs_join_keys.iter().all(|e| e.is_column(&self.expr_graph));
                    if !(lhs_has_columns_only && rhs_has_columns_only) {
                        let children = lop.children.clone();
                        let (lhs_join_keys, rhs_join_keys) = (lhs_join_keys.clone(), rhs_join_keys.clone());
                        let key_exprs = [lhs_join_keys, rhs_join_keys];
                        for ix in [0, 1] {
                            let child_lop_key = children.as_ref().unwrap()[ix];
                            let virtcols = Some(&key_exprs[ix]);

                            // Add partitioning columns to the Repartition projection
                            Self::append_virt_cols(lop_graph, child_lop_key, virtcols);
                        }
                    }
                }
                _ => {}
            }
        }
        root_lop_key
    }

    pub fn build_qblock_logical_plan(
        qblock_graph: &QueryBlockGraph, expr_graph: &mut ExprGraph, env: &Env, qblock_key: QueryBlockKey, aps_context: &APSContext, lop_graph: &mut LOPGraph,
        expected_partitioning: Option<&PartDesc>,
    ) -> Result<LOPKey, String> {
        let qblock = &qblock_graph.get(qblock_key).value;
        let mut worklist: Vec<LOPKey> = vec![];

        //assert!(self.cte_list.is_empty());

        let all_preds = &aps_context.all_preds;

        // Process select-list: Collect all QunCols
        let select_list_quncol = Self::collect_selectlist_quncols(qblock_graph, expr_graph, aps_context, qblock);

        // Process predicates: Collect quns, quncols. Also collect lhs/rhs quns for equi-join candidates
        let (mut pred_map, eqclass) = Self::collect_preds(qblock_graph, expr_graph, aps_context, qblock);

        // Build unary plans first (i.e. baseline single table scans)
        Self::build_unary_plans(qblock_graph, expr_graph, env, aps_context, qblock, lop_graph, &mut pred_map, &eqclass, &select_list_quncol, &mut worklist)?;

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

                    let join_quns = &lhs_props.quns | &rhs_props.quns;

                    // Are there any join predicates between two subplans?
                    // P1.quns should be superset of LHS quns
                    // P2.quns should be superset of RHS quns
                    let join_preds = pred_map
                        .iter()
                        .filter_map(|(pred_key, pred_desc)| {
                            let PredDesc { quns, .. } = pred_desc;
                            let is_subset = (quns & &join_quns) == *quns;
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
                                let join_class = Self::classify_predicate(eqjoin_desc, lhs_props, rhs_props);
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
                    let eqq = equi_join_preds.iter().map(|e| e.0).collect::<Vec<_>>();

                    if !equi_join_preds.is_empty() {
                        let mut preds = all_preds.clone_metadata();

                        for pred_key in join_preds.iter() {
                            // Don't add equijoin preds to the after-join list
                            if !eqq.contains(pred_key) {
                                preds.set(*pred_key);
                            }
                            pred_map.remove_entry(pred_key);
                        }

                        // Initialize join properties
                        let quns = &lhs_props.quns | &rhs_props.quns;
                        let mut cols = &lhs_props.cols | &rhs_props.cols;

                        // Compute cols to flow through. Retain all cols in the select-list + unbound preds
                        let mut flowcols = select_list_quncol.clone();
                        for (_, PredDesc { quncols, .. }) in pred_map.iter() {
                            flowcols |= quncols;
                        }
                        cols &= flowcols;

                        let (new_lhs_plan_key, new_rhs_plan_key, lhs_join_keys, rhs_join_keys, cpartitions) =
                            Self::repartition_join_legs(qblock_graph, expr_graph, env, lop_graph, lhs_plan_key, rhs_plan_key, &equi_join_preds, &eqclass);

                        // Join partitioning is identical to partitioning of the LHS.
                        let lhs_props = &lop_graph.get(new_lhs_plan_key).properties;
                        let mut partdesc = lhs_props.partdesc.clone();
                        partdesc.npartitions = cpartitions;

                        let props = LOPProps::new(quns, cols, None, preds, partdesc);

                        let join_lop_key = lop_graph.add_node_with_props(
                            LOP::HashJoin { lhs_join_keys, rhs_join_keys },
                            props,
                            Some(vec![new_lhs_plan_key, new_rhs_plan_key]),
                        );

                        join_status = Some((lhs_plan_key, rhs_plan_key, join_lop_key));

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
                root_lop_key = Self::repartition_if_needed(qblock_graph, expr_graph, lop_graph, root_lop_key, expected_partitioning, &eqclass);
            }

            if qblock.qbtype == QueryBlockType::GroupBy {
                // Only the select-list expressions flow out of a queryblock. We can clear the column bitset.
                let props = &mut lop_graph.get_mut(root_lop_key).properties;
                props.cols = props.cols.clone_metadata();
            } else {
                // Only the select-list expressions flow out of a queryblock. We can clear the column bitset.
                let props = &mut lop_graph.get_mut(root_lop_key).properties;
                let virtcols = qblock.select_list.iter().map(|ne| ne.expr_key).collect::<Vec<_>>();
                props.virtcols = Some(virtcols);
                props.cols = props.cols.clone_metadata();
            }

            info!("Created logical plan for qblock id: {}", qblock.id);

            Ok(root_lop_key)
        } else {
            Err("Cannot find plan for qblock".to_string())
        }
    }

    pub fn build_unary_plans(
        qblock_graph: &QueryBlockGraph, expr_graph: &mut ExprGraph, env: &Env, aps_context: &APSContext, qblock: &QueryBlock, lop_graph: &mut LOPGraph,
        pred_map: &mut PredMap, eqclass: &ExprEqClass, select_list_quncol: &Bitset<QunCol>, worklist: &mut Vec<LOPKey>,
    ) -> Result<(), String> {
        let APSContext { all_quncols, all_quns, all_preds } = aps_context;

        // Build unary POPs first
        for qun in qblock.quns.iter() {
            // Set quns
            let mut quns = all_quns.clone_metadata();
            quns.set(qun.id);

            // Set input cols: find all column references for this qun
            let mut input_quncols = all_quncols.clone_metadata();
            aps_context.all_quncols.elements().iter().filter(|&quncol| quncol.0 == qun.id).for_each(|&quncol| input_quncols.set(quncol));

            // Set output cols + preds
            let mut unbound_quncols = select_list_quncol.clone();

            let mut preds = all_preds.clone_metadata();
            pred_map.iter().for_each(|(&pred_key, PredDesc { quncols, quns, .. })| {
                if quns.get(qun.id) {
                    if quns.len() == 1 {
                        // Set preds: find local preds that refer to this qun
                        preds.set(pred_key);
                    } else {
                        // Set output columns: Only project cols in the select-list + unbound join preds
                        unbound_quncols |= quncols;
                    }
                }
            });

            let output_quncols = &unbound_quncols & &input_quncols;

            // Remove all preds that will run on this tablescan as they've been bound already
            for pred_key in preds.elements().iter() {
                pred_map.remove_entry(pred_key);
            }

            // Build plan for nested query blocks
            let lopkey = if qblock.qbtype == QueryBlockType::GroupBy {
                let child_qblock_key = qun.get_qblock_key().unwrap();
                let child_qblock = qblock_graph.get_value(child_qblock_key);
                let key_len = qblock.group_by.as_ref().unwrap().len();

                // Build Scan POP
                let child_lop_key = Self::build_qblock_logical_plan(qblock_graph, expr_graph, env, child_qblock_key, aps_context, lop_graph, None)?;
                let (_, child_props, _) = lop_graph.get3(child_lop_key);
                let child_props = child_props.clone();

                // Build Aggregation POP
                if child_props.partdesc.npartitions == 1 {
                    // Underlying aggregation input has one partition. Aggregate directly. No pre-agg needed.
                    let expected_partitioning = PartDesc { npartitions: 1, part_type: PartType::RAW };

                    let children = Some(vec![child_lop_key]);
                    let virtcols = qblock.select_list.iter().map(|ne| ne.expr_key).collect::<Vec<_>>();
                    let mut props = LOPProps::new(quns, output_quncols, Some(virtcols), preds, expected_partitioning);
                    lop_graph.add_node_with_props(LOP::Aggregation { key_len }, props, children)
                } else {
                    // Underlying aggregation input has multiple partitions. We aggregate in two steps.
                    let (pre_exprs, post_exprs) = Self::build_pre_and_post_aggs_virt_cols(env, qblock_graph, expr_graph, lop_graph, aps_context, qblock)?;
                    let expected_partitioning_expr = pre_exprs.iter().take(key_len).cloned().collect::<Vec<_>>();

                    // Build pre-aggregation POP
                    let preagg_lop = LOP::Aggregation { key_len };
                    let mut preagg_props = child_props.clone();
                    preagg_props.virtcols = Some(pre_exprs);
                    preagg_props.cols = child_props.cols.clone_metadata();
                    let preagg_children = Some(vec![child_lop_key]);
                    let preagg_lop_key = lop_graph.add_node_with_props(preagg_lop, preagg_props, preagg_children);

                    // Repartition: partition keys <= aggregation keys
                    let expected_partitioning =
                        PartDesc { npartitions: env.settings.parallel_degree.unwrap_or(1), part_type: PartType::HASHEXPR(expected_partitioning_expr) };
                    let repart_lop_key = Self::repartition_if_needed(qblock_graph, expr_graph, lop_graph, preagg_lop_key, &expected_partitioning, &eqclass);
                    let (repart_lop, repart_props, _) = lop_graph.get3(repart_lop_key);

                    // Build post-aggregation POP
                    let postagg_lop = LOP::Aggregation { key_len };
                    let mut postagg_props = repart_props.clone();
                    postagg_props.virtcols = Some(post_exprs);
                    postagg_props.cols = repart_props.cols.clone_metadata();
                    postagg_props.partdesc = expected_partitioning.clone();

                    let postagg_children = Some(vec![repart_lop_key]);
                    let postagg_lop_key = lop_graph.add_node_with_props(postagg_lop, postagg_props, postagg_children);
                    postagg_lop_key
                }
            } else {
                let npartitions = if let Some(tabledesc) = qun.tabledesc.as_ref() {
                    tabledesc.get_part_desc().unwrap().npartitions
                } else {
                    env.settings.parallel_degree.unwrap_or(1) * 2 // todo: temporary hack to force different partition counts in a plan
                };
                // Build Scan POP
                let partdesc = PartDesc::new(npartitions, PartType::RAW);
                let props = LOPProps::new(quns, output_quncols, None, preds, partdesc);
                lop_graph.add_node_with_props(LOP::TableScan { input_projection: input_quncols }, props, None)
            };
            //debug!("Build TableScan: key={:?} {:?} id={}", lopkey, qun.display(), qun.id);
            worklist.push(lopkey);
        }
        Ok(())
    }

    fn build_pre_and_post_aggs_virt_cols(
        env: &Env, qblock_graph: &QueryBlockGraph, expr_graph: &mut ExprGraph, lop_graph: &mut LOPGraph, aps_context: &APSContext, qblock: &QueryBlock,
    ) -> Result<(Vec<ExprKey>, Vec<ExprKey>), String> {
        let mut preaggs: Vec<(ExprKey, ExprKey)> = vec![]; // orig -> pre map
        let mut postaggs: Vec<ExprKey> = vec![];

        // Prime pre-agg list by adding grouping columns.
        let key_len = qblock.group_by.as_ref().unwrap().len();
        let child_qun = &qblock.quns[0];
        let qid = child_qun.id;
        let Some(child_qblock) = child_qun.get_qblock(qblock_graph) else { return Err("Bad!".to_string()) };
        for (cid, ek) in child_qblock.select_list.iter().take(key_len).map(|ne| ne.expr_key).enumerate() {
            let (expr, props, _) = expr_graph.get3(ek);
            let pre_expr = Expr::CID(qid, cid);
            let pre_expr_key = expr_graph.add_node_with_props(pre_expr, props.clone(), None);
            preaggs.push((pre_expr_key, pre_expr_key));
        }

        // Now translate original select list into new list, referencing outer expressions with references to inner ones
        let exprs = qblock.select_list.iter().map(|ne| ne.expr_key).collect::<Vec<_>>();
        for expr_key in exprs {
            let postagge = Self::build_one_pre_and_post_virt_col(qblock_graph, expr_graph, expr_key, &mut preaggs)?;
            postaggs.push(postagge);
        }
        let preagg = preaggs.iter().map(|&(e, _)| e).collect::<Vec<_>>();
        Ok((preagg, postaggs))
    }

    fn build_one_pre_and_post_virt_col(
        qblock_graph: &QueryBlockGraph, expr_graph: &mut ExprGraph, pre_expr_key: ExprKey, preaggs: &mut Vec<(ExprKey, ExprKey)>,
    ) -> Result<ExprKey, String> {
        debug!("build_one_pre_and_post_virt_col: {:?}", pre_expr_key.describe(expr_graph, false));

        if let Some(post_expr_key) = Self::find(expr_graph, preaggs, pre_expr_key) {
            return Ok(post_expr_key);
        } else {
            let (cur_expr, cur_props, cur_children) = expr_graph.get3(pre_expr_key);
            let (cur_expr, cur_props, cur_children) = (cur_expr.clone(), cur_props.clone(), cur_children.cloned());
            let post_expr_key = match cur_expr {
                Expr::CID(qid, cid) => {
                    let post_expr = Expr::CID(qid, preaggs.len());
                    let post_expr_key = expr_graph.add_node_with_props(post_expr, cur_props.clone(), None);
                    preaggs.push((pre_expr_key, post_expr_key));
                    debug!(
                        "build_one_pre_and_post_virt_col: {:?} -> {:?}",
                        pre_expr_key.describe(expr_graph, false),
                        post_expr_key.describe(expr_graph, false)
                    );
                    post_expr_key
                }
                Expr::AggFunction(aggtype, distinct) => {
                    let cur_child_expr_key = cur_children.unwrap()[0];
                    let cur_child = expr_graph.get_value(cur_child_expr_key);
                    let Expr::CID(qid, cid) = cur_child else { return Err("Bad child of agg-function, expecting CID".to_string()) };
                    let post_child_expr = Expr::CID(*qid, preaggs.len());
                    let post_child_expr_key = expr_graph.add_node_with_props(post_child_expr, cur_props.clone(), None);
                    let postaggtype = if aggtype == AggType::COUNT { AggType::SUM } else { aggtype };
                    let post_expr = Expr::AggFunction(postaggtype, distinct);
                    let post_expr_key = expr_graph.add_node_with_props(post_expr, cur_props, Some(vec![post_child_expr_key]));
                    preaggs.push((pre_expr_key, post_expr_key));
                    debug!(
                        "build_one_pre_and_post_virt_col: {:?} -> {:?}",
                        pre_expr_key.describe(expr_graph, false),
                        post_expr_key.describe(expr_graph, false)
                    );
                    post_expr_key
                }
                Expr::BinaryExpr(ArithOp::Div) | Expr::Cast => {
                    let post_children = cur_children
                        .unwrap()
                        .iter()
                        .map(|e| Self::build_one_pre_and_post_virt_col(qblock_graph, expr_graph, *e, preaggs).unwrap())
                        .collect::<Vec<_>>();
                    let (cur_expr, cur_props, _) = expr_graph.get3(pre_expr_key);
                    let post_expr = cur_expr.clone();
                    expr_graph.add_node_with_props(post_expr, cur_props.clone(), Some(post_children))
                }
                _ => return Err("build_one_pre_and_post_virt_col: Invalid expression!".to_string()),
            };
            Ok(post_expr_key)
        }
    }

    fn find(graph: &ExprGraph, pre_agg_list: &[(ExprKey, ExprKey)], pre_expr_key: ExprKey) -> Option<ExprKey> {
        // Does this expression already exist in pre_agg_list?
        for &(expr_key1, expr_key2) in pre_agg_list.iter() {
            if Expr::isomorphic(graph, pre_expr_key, expr_key1) {
                return Some(expr_key2);
            }
        }
        None
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

    fn collect_selectlist_quncols(qblock_graph: &QueryBlockGraph, expr_graph: &ExprGraph, aps_context: &APSContext, qblock: &QueryBlock) -> Bitset<QunCol> {
        let mut select_list_quncol = aps_context.all_quncols.clone_metadata();
        qblock.select_list.iter().flat_map(|ne| ne.expr_key.iter_quncols(expr_graph)).for_each(|quncol| select_list_quncol.set(quncol));
        select_list_quncol
    }

    fn collect_preds(qblock_graph: &QueryBlockGraph, expr_graph: &ExprGraph, aps_context: &APSContext, qblock: &QueryBlock) -> (PredMap, ExprEqClass) {
        let mut pred_map: PredMap = HashMap::new();

        let mut eqclass = ExprEqClass::default();
        let mut eqpred_legs = vec![];

        if let Some(pred_list) = qblock.pred_list.as_ref() {
            for &pred_key in pred_list.iter() {
                // Collect quns and quncols for each predicate
                let mut quncols = aps_context.all_quncols.clone_metadata();
                let mut quns = aps_context.all_quns.clone_metadata();

                for quncol in pred_key.iter_quncols(expr_graph) {
                    quncols.set(quncol);
                    quns.set(quncol.0);
                }

                // For equijoin candidates, collect lhs and rhs quns
                let expr = expr_graph.get(pred_key);
                let eqjoin_desc = if let RelExpr(RelOp::Eq) = expr.value {
                    let children = expr.children.as_ref().unwrap();
                    let (lhs_child_key, rhs_child_key) = (children[0], children[1]);
                    let lhs_quns = aps_context.all_quns.clone_metadata().init(lhs_child_key.iter_quns(expr_graph));
                    let rhs_quns = aps_context.all_quns.clone_metadata().init(rhs_child_key.iter_quns(expr_graph));

                    if !lhs_quns.is_empty() && !rhs_quns.is_empty() {
                        let (lhs_hash, rhs_hash) = (lhs_child_key.hash(expr_graph), rhs_child_key.hash(expr_graph));
                        eqpred_legs.push((lhs_hash, lhs_child_key));
                        eqpred_legs.push((rhs_hash, rhs_child_key));
                        eqclass.set_eq(lhs_child_key, rhs_child_key);
                        Some(EqJoinDesc { lhs_quns, rhs_quns })
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
}
