// lop_repartition: All repartitioning related code

use crate::{
    expr::{Expr::*, ExprGraph, *},
    graph::{ExprKey, Graph, LOPKey},
    includes::*,
    lop::{ExprEqClass, LOPGraph, LOPProps, PredicateAlignment, VirtCol, LOP},
    metadata::{PartDesc, PartType},
    QGM,
};

impl QGM {
    pub fn repartition_if_needed(self: &QGM, lop_graph: &mut LOPGraph, lop_key: LOPKey, expected_partitioning: &PartDesc, eqclass: &ExprEqClass) -> LOPKey {
        let props = lop_graph.get_properties(lop_key);
        let actual_partitioning = &props.partdesc;

        if Self::compare_part_descs(&self.expr_graph, expected_partitioning, actual_partitioning, eqclass) {
            lop_key
        } else {
            let mut partdesc = expected_partitioning.clone();
            let cpartitions = partdesc.npartitions;
            partdesc.npartitions = actual_partitioning.npartitions;

            let props = LOPProps { quns: props.quns.clone(), cols: props.cols.clone(), virtcols: None, preds: props.preds.clone_metadata(), partdesc };
            lop_graph.add_node_with_props(LOP::Repartition { cpartitions }, props, Some(vec![lop_key]))
        }
    }

    pub fn repartition_join_legs(
        self: &QGM, env: &Env, lop_graph: &mut Graph<LOPKey, LOP, LOPProps>, lhs_plan_key: LOPKey, rhs_plan_key: LOPKey, equi_join_preds: &[(ExprKey, PredicateAlignment)],
        eqclass: &ExprEqClass,
    ) -> (LOPKey, LOPKey, Vec<ExprKey>, Vec<ExprKey>, usize) {
        let lhs_props = &lop_graph.get(lhs_plan_key).properties;
        let rhs_props = &lop_graph.get(rhs_plan_key).properties;

        // Repartition join legs as needed
        let (lhs_partdesc, rhs_partdesc, lhs_join_keys, rhs_join_keys, cpartitions) =
            Self::harmonize_partitions(env, lop_graph, lhs_plan_key, rhs_plan_key, &self.expr_graph, equi_join_preds, eqclass);

        let lhs_repart_props = lhs_partdesc.map(|partdesc| {
            let virtcols = None;
            LOPProps { quns: lhs_props.quns.clone(), cols: lhs_props.cols.clone(), preds: lhs_props.preds.clone_metadata(), partdesc, virtcols }
        });
        let rhs_repart_props = rhs_partdesc.map(|partdesc| {
            let virtcols = None;
            LOPProps { quns: rhs_props.quns.clone(), cols: rhs_props.cols.clone(), preds: rhs_props.preds.clone_metadata(), partdesc, virtcols }
        });

        //let (lhs_partitions, rhs_partitions) = (npartitions, npartitions);
        //let (lhs_partitions, rhs_partitions) = (lhs_props.partdesc.npartitions, rhs_props.partdesc.npartitions);

        let new_lhs_plan_key = if let Some(lhs_repart_props) = lhs_repart_props {
            // Repartition LHS
            lop_graph.add_node_with_props(LOP::Repartition { cpartitions }, lhs_repart_props, Some(vec![lhs_plan_key]))
        } else {
            lhs_plan_key
        };
        let new_rhs_plan_key = if let Some(rhs_repart_props) = rhs_repart_props {
            // Repartition RHS
            lop_graph.add_node_with_props(LOP::Repartition { cpartitions }, rhs_repart_props, Some(vec![rhs_plan_key]))
        } else {
            rhs_plan_key
        };
        (new_lhs_plan_key, new_rhs_plan_key, lhs_join_keys, rhs_join_keys, cpartitions)
    }

    pub(crate) fn partdesc_to_virtcols(expr_graph: &ExprGraph, partdesc: &PartDesc) -> Option<Vec<VirtCol>> {
        // Only return virtual columns that are composite expressions (i.e. not plain columns)
        if let PartType::HASHEXPR(exprs) = &partdesc.part_type {
            let virtcols = exprs.iter().filter(|&expr_key| !expr_key.is_column(expr_graph)).cloned().collect::<Vec<_>>();
            if !virtcols.is_empty() {
                return Some(virtcols);
            }
        }
        None
    }

    pub fn compare_part_keys(expr_graph: &ExprGraph, keys1: &Vec<ExprKey>, keys2: &Vec<ExprKey>, eqclass: &ExprEqClass) -> bool {
        if keys1.len() == keys2.len() {
            keys2.iter().zip(keys1.iter()).all(|(key1, key2)| eqclass.check_eq(expr_graph, *key1, *key2))
        } else {
            false
        }
    }

    pub fn compare_part_descs(expr_graph: &ExprGraph, expected_desc: &PartDesc, actual_desc: &PartDesc, eqclass: &ExprEqClass) -> bool {
        if expected_desc.npartitions == actual_desc.npartitions {
            match (&expected_desc.part_type, &actual_desc.part_type) {
                (PartType::RAW, PartType::RAW) => true,
                (PartType::HASHEXPR(keys1), PartType::HASHEXPR(keys2)) => Self::compare_part_keys(expr_graph, keys1, keys2, eqclass),
                _ => false,
            }
        } else {
            false
        }
    }

    // harmonize_partitions: Return a triplet indicating whether either/both legs of a join need to be repartitioned
    pub fn harmonize_partitions(
        env: &Env, lop_graph: &LOPGraph, lhs_plan_key: LOPKey, rhs_plan_key: LOPKey, expr_graph: &ExprGraph, join_preds: &[(ExprKey, PredicateAlignment)],
        eqclass: &ExprEqClass,
    ) -> (Option<PartDesc>, Option<PartDesc>, Vec<ExprKey>, Vec<ExprKey>, usize) {
        // Compare expected vs actual partitioning keys on both sides of the join
        // Both sides must be partitioned on equivalent keys and with identical partition counts

        // Compute actual partitioning keys
        let lhs_props = lop_graph.get_properties(lhs_plan_key);
        let rhs_props = lop_graph.get_properties(rhs_plan_key);
        let empty_vec = vec![];
        let lhs_actual_keys = if let PartType::HASHEXPR(keys) = &lhs_props.partdesc.part_type { keys } else { &empty_vec };
        let rhs_actual_keys = if let PartType::HASHEXPR(keys) = &rhs_props.partdesc.part_type { keys } else { &empty_vec };

        // Compute expected partitioning keys
        let (lhs_join_keys, rhs_join_keys) = Self::compute_join_partitioning_keys(expr_graph, join_preds);

        // TODO: need to ensure #partitions are matched up correctly esp. in light of situations wherein one leg is correctly partitioned while the other isn't
        let lhs_partdesc = if Self::compare_part_keys(expr_graph, &lhs_join_keys, lhs_actual_keys, eqclass) {
            None
        } else {
            Some(PartDesc { npartitions: lhs_props.partdesc.npartitions, part_type: PartType::HASHEXPR(lhs_join_keys.clone()) })
        };

        let rhs_partdesc = if Self::compare_part_keys(expr_graph, &rhs_join_keys, rhs_actual_keys, eqclass) {
            None
        } else {
            Some(PartDesc { npartitions: rhs_props.partdesc.npartitions, part_type: PartType::HASHEXPR(rhs_join_keys.clone()) })
        };

        let npartitions = env.settings.parallel_degree.unwrap_or(1);

        (lhs_partdesc, rhs_partdesc, lhs_join_keys, rhs_join_keys, npartitions)
    }

    pub fn compute_join_partitioning_descs(
        env: &Env, expr_graph: &ExprGraph, join_preds: &[(ExprKey, PredicateAlignment)],
    ) -> (PartDesc, PartDesc, Vec<ExprKey>, Vec<ExprKey>) {
        let (lhs_join_keys, rhs_join_keys) = Self::compute_join_partitioning_keys(expr_graph, join_preds);
        let npartitions = env.settings.parallel_degree.unwrap_or(1);

        let lhs_part_desc = PartDesc { npartitions, part_type: PartType::HASHEXPR(lhs_join_keys.clone()) };

        let rhs_part_desc = PartDesc { npartitions, part_type: PartType::HASHEXPR(rhs_join_keys.clone()) };

        (lhs_part_desc, rhs_part_desc, lhs_join_keys, rhs_join_keys)
    }

    pub fn compute_join_partitioning_keys(expr_graph: &ExprGraph, join_preds: &[(ExprKey, PredicateAlignment)]) -> (Vec<ExprKey>, Vec<ExprKey>) {
        // Compute expected partitioning keys
        let mut lhs_join_keys = vec![];
        let mut rhs_join_keys = vec![];
        for &(join_pred_key, alignment) in join_preds.iter() {
            let (expr, _, children) = expr_graph.get3(join_pred_key);
            if let RelExpr(RelOp::Eq) = expr {
                let children = children.unwrap();
                let (lhs_pred_key, rhs_pred_key) = (children[0], children[1]);
                if alignment == PredicateAlignment::Aligned {
                    lhs_join_keys.push(lhs_pred_key);
                    rhs_join_keys.push(rhs_pred_key);
                } else if alignment == PredicateAlignment::Reversed {
                    lhs_join_keys.push(rhs_pred_key);
                    rhs_join_keys.push(lhs_pred_key);
                } else {
                    panic!("compute_join_partitioning_keys: bad alignment");
                }
            }
        }
        (lhs_join_keys, rhs_join_keys)
    }
}
