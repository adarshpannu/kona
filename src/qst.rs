// QST: Query Semantic Transforms

use std::rc::Rc;

use crate::{
    expr::{AggType, ArithOp, Expr, Expr::*, ExprGraph, ExprProp},
    graph::{ExprKey, Node, QueryBlockKey},
    includes::*,
    qgm::{QueryBlockType, QGM},
    row::Datum,
    NamedExpr, QGMMetadata, Quantifier, QueryBlock, QueryBlockGraph,
};

impl QGM {
    pub fn resolve(&mut self, env: &Env) -> Result<(), String> {
        // Resolve top-level QB
        let qbkey = self.main_qblock_key;
        QueryBlock::resolve(qbkey, env, &mut self.qblock_graph, &mut self.expr_graph, &mut self.metadata)?;

        Ok(())
    }
}

impl QueryBlock {
    pub fn resolve(
        qbkey: QueryBlockKey, env: &Env, qblock_graph: &mut QueryBlockGraph, expr_graph: &mut ExprGraph, metadata: &mut QGMMetadata,
    ) -> Result<(), String> {
        let qblock = &mut qblock_graph.get_mut(qbkey).value;

        // Resolve nested query blocks first
        let qbkey_children: Vec<QueryBlockKey> = qblock.quns.iter().filter_map(|qun| qun.get_qblock()).collect();
        for qbkey in qbkey_children {
            Self::resolve(qbkey, env, qblock_graph, expr_graph, metadata)?;
        }

        let mut qblock = &mut qblock_graph.get_mut(qbkey).value;
        let qblock_id = qblock.id;

        // Resolve base table QUNs next
        for qun in qblock.quns.iter_mut() {
            if let Some(tablename) = qun.get_basename() {
                let tbdesc = env.metadata.get_tabledesc(tablename);
                if let Some(tbdesc0) = tbdesc.as_ref() {
                    metadata.add_tabledesc(qun.id, Rc::clone(tbdesc0));
                    qun.tabledesc = tbdesc;
                } else {
                    return Err(format!("Table {} not cataloged.", enquote(tablename)));
                }
            }
        }

        // Resolve select list
        for ne in qblock.select_list.iter() {
            let expr_key = ne.expr_key;
            qblock.resolve_expr(env, expr_graph, metadata, expr_key, true)?;
        }

        // Resolve predicates
        if let Some(pred_list) = qblock.pred_list.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_key in pred_list {
                qblock.resolve_expr(env, expr_graph, metadata, expr_key, false)?;
                expr_key.get_boolean_factors(&expr_graph, &mut boolean_factors)
            }
            qblock.pred_list = Some(boolean_factors);
        }

        // Resolve group-by
        if let Some(group_by) = qblock.group_by.as_ref() {
            for &expr_key in group_by.iter() {
                qblock.resolve_expr(env, expr_graph, metadata, expr_key, false)?;
            }
        }

        // Resolve having-clause
        if let Some(having_clause) = qblock.having_clause.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_key in having_clause {
                qblock.resolve_expr(env, expr_graph, metadata, expr_key, true)?;
                expr_key.get_boolean_factors(&expr_graph, &mut boolean_factors)
            }
            qblock.having_clause = Some(boolean_factors);
        }

        // Resolve group-by/having clauses, if they exist
        // If a GROUP BY is present, all select_list expressions must either by included in the group_by, or they must be aggregate functions
        if qblock.group_by.is_some() {
            Self::split_groupby(qbkey, env, qblock_graph, expr_graph)?;
        }
        info!("Resolved qblock id: {}", qblock_id);
        Ok(())
    }

    pub fn split_groupby(qbkey: QueryBlockKey, env: &Env, qblock_graph: &mut QueryBlockGraph, expr_graph: &mut ExprGraph) -> Result<(), String> {
        let inner_qb_key = qblock_graph.add_node(QueryBlock::new0(expr_graph.next_id(), QueryBlockType::Select), None);
        let [outer_qb_node, inner_qb_node] = qblock_graph.get_disjoint_mut([qbkey, inner_qb_key]);

        let qblock = &mut outer_qb_node.value;

        let agg_qun_id = expr_graph.next_id();

        // Replace group_by expressions with references to child qun
        let group_by = replace(&mut qblock.group_by, None).unwrap();
        let group_by_expr_count = group_by.len();

        let having_clause = replace(&mut qblock.having_clause, None);

        // Construct inner select-list by first adding GROUP-BY clause expressions
        let mut inner_select_list = group_by
            .iter()
            .map(|&expr_key| NamedExpr::new(None, expr_key, &expr_graph))
            .collect::<Vec<NamedExpr>>();

        // Augment inner select-list by extracting parameters from `agg(parameter)` expressions
        for ne in qblock.select_list.iter_mut() {
            Self::transform_groupby_expr(env, expr_graph, &mut inner_select_list, group_by_expr_count, agg_qun_id, &mut ne.expr_key)?;
        }

        // Transform HAVING clause expressions -> outer qb predicates
        let outer_pred_list = if let Some(having_clause) = having_clause {
            let mut new_having_clause = vec![];
            for having_pred in having_clause.iter() {
                let mut new_pred_id = *having_pred;
                Self::transform_groupby_expr(env, expr_graph, &mut inner_select_list, group_by_expr_count, agg_qun_id, &mut new_pred_id)?;
                new_having_clause.push(new_pred_id);
            }
            Some(new_having_clause)
        } else {
            None
        };

        let outer_qb = qblock;
        let inner_qb = QueryBlock::new(
            expr_graph.next_id(),
            replace(&mut outer_qb.name, None),
            QueryBlockType::Select,
            inner_select_list,
            replace(&mut outer_qb.quns, vec![]),
            replace(&mut outer_qb.pred_list, None),
            None,
            None,
            None,
            outer_qb.distinct,
            None,
        );

        let group_by = group_by
            .iter()
            .enumerate()
            .map(|(cid, ..)| expr_graph.add_node(Expr::CID(agg_qun_id, cid), None))
            .collect::<Vec<_>>();

        inner_qb_node.value = inner_qb;
        let outer_qun = Quantifier::new_qblock(agg_qun_id, inner_qb_key, None);
        outer_qb.name = None;
        outer_qb.qbtype = QueryBlockType::GroupBy;
        outer_qb.quns = vec![outer_qun];
        outer_qb.pred_list = outer_pred_list;
        outer_qb.group_by = Some(group_by);

        Ok(())
    }

    fn find(graph: &ExprGraph, select_list: &Vec<NamedExpr>, group_by_expr_count: usize, expr_key: ExprKey) -> Option<usize> {
        // Does this expression already exist in the select_list[..until_index]?
        for (ix, ne) in select_list.iter().enumerate() {
            if ix >= group_by_expr_count {
                return None;
            } else if Expr::isomorphic(graph, expr_key, ne.expr_key) {
                return Some(ix);
            }
        }
        None
    }

    fn append(expr_graph: &ExprGraph, select_list: &mut Vec<NamedExpr>, expr_key: ExprKey) -> usize {
        // Does this expression already exist in the select_list?
        if let Some(ix) = Self::find(expr_graph, select_list, select_list.len(), expr_key) {
            // ... yes, return its index
            return ix;
        }
        // ... append it to list
        select_list.push(NamedExpr { alias: None, expr_key });
        return select_list.len() - 1;
    }

    // transform_groupby_expr: Traverse expression `expr_key` (which is part of aggregate qb select list) such that any subexpressions are replaced with
    // corresponding references to inner/select qb select-list using CID#
    pub fn transform_groupby_expr(
        env: &Env, expr_graph: &mut ExprGraph, select_list: &mut Vec<NamedExpr>, group_by_expr_count: usize, qunid: QunId, expr_key: &mut ExprKey,
    ) -> Result<(), String> {
        //debug!("transform_groupby_expr: {:?}", expr_key.printable(&expr_graph, false));

        let node = expr_graph.get(*expr_key);
        if let AggFunction(aggtype, _) = node.value {
            // Aggregate-function: replace argument with CID reference to inner query-block
            let child_key = node.children.as_ref().unwrap()[0];
            let cid = Self::append(expr_graph, select_list, child_key);
            let new_child_key = if aggtype == AggType::AVG {
                // AVG -> SUM / COUNT
                let cid = expr_graph.add_node(CID(qunid, cid), None);
                let sum = expr_graph.add_node(AggFunction(AggType::SUM, false), Some(vec![cid]));
                let cnt = expr_graph.add_node(AggFunction(AggType::COUNT, false), Some(vec![cid]));
                expr_graph.add_node(BinaryExpr(ArithOp::Div), Some(vec![sum, cnt]))
            } else {
                expr_graph.add_node(CID(qunid, cid), None)
            };
            let node = expr_graph.get_mut(*expr_key);
            node.children = Some(vec![new_child_key]);
            if aggtype == AggType::AVG {
                *expr_key = new_child_key;
            }
        } else if let Some(cid) = Self::find(&expr_graph, &select_list, group_by_expr_count, *expr_key) {
            // Expression in GROUP-BY list, all good
            let new_child_key = expr_graph.add_node(CID(qunid, cid), None);
            *expr_key = new_child_key;
        } else if let Column { colname, .. } = &node.value {
            // User error: Unaggregated expression in select-list not in group-by clause
            return Err(format!(
                "Column {} is referenced in select-list/having-clause but it is not specified in the GROUP-BY list",
                colname
            ));
        } else if let Some(mut children) = node.children.clone() {
            let mut children2 = vec![];
            for child_key in children.iter_mut() {
                Self::transform_groupby_expr(env, expr_graph, select_list, group_by_expr_count, qunid, child_key)?;
                children2.push(*child_key);
            }
            let node = expr_graph.get_mut(*expr_key);
            node.children = Some(children);
        }
        Ok(())
    }

    pub fn resolve_column(&self, _env: &Env, prefix: Option<&String>, colname: &String) -> Result<(QunCol, DataType, ColId), String> {
        let mut retval = None;
        let colid = 0;

        for qun in self.quns.iter() {
            let desc = qun.tabledesc.as_ref().unwrap().clone();
            let field = if let Some(prefix) = prefix {
                // Prefixed column: look at specific qun
                if qun.matches_name_or_alias(prefix) {
                    desc.get_column(colname)
                } else {
                    None
                }
            } else {
                // Unprefixed column: look at all QUNs
                desc.get_column(colname)
            };

            if let Some((colid, field)) = field {
                if retval.is_none() {
                    retval = Some((QunCol(qun.id, colid), field.data_type.clone()));
                } else {
                    return Err(format!(
                        "Column {} found in multiple tables. Use tablename prefix to disambiguate.",
                        enquote(colname)
                    ));
                }
            }

            // Stop looking if we've searched for this column in a specified table
            if prefix.is_some() && qun.matches_name_or_alias(prefix.unwrap()) {
                break;
            }
        }

        if let Some(retval) = retval {
            Ok((retval.0, retval.1, colid))
        } else {
            let colstr = if let Some(prefix) = prefix {
                format!("{}.{}", prefix, colname)
            } else {
                format!("{}", colname)
            };
            Err(format!("Column {} not found in any table.", colstr))
        }
    }

    pub fn resolve_expr(
        &self, env: &Env, expr_graph: &mut ExprGraph, metadata: &mut QGMMetadata, expr_key: ExprKey, agg_fns_allowed: bool,
    ) -> Result<(), String> {
        let children_agg_fns_allowed = if let AggFunction(_, _) = expr_graph.get(expr_key).value {
            // Nested aggregate functions not allowed
            false
        } else {
            agg_fns_allowed
        };

        let children = expr_graph.get(expr_key).children.clone();

        let mut children_datatypes = vec![];

        if let Some(children) = children {
            for child_key in children {
                self.resolve_expr(env, expr_graph, metadata, child_key, children_agg_fns_allowed)?;
                let datatype = expr_graph.get(child_key).properties.datatype.clone();
                children_datatypes.push(datatype);
            }
        }

        let mut node: &mut Node<ExprKey, Expr, ExprProp> = expr_graph.get_mut(expr_key);
        let expr = &mut node.value;
        //info!("Check: {:?}", expr);

        let datatype = match expr {
            CID(_, _) => node.properties.datatype.clone(),
            RelExpr(..) => {
                // Check argument types
                if children_datatypes[0] != children_datatypes[1] {
                    return Err("Datatype mismatch".to_string());
                } else {
                    DataType::Boolean
                }
            }
            BinaryExpr(..) => {
                // Check argument types
                if is_numeric(&children_datatypes[0]) && is_numeric(&children_datatypes[1]) {
                    children_datatypes[0].clone()
                } else {
                    return Err("Binary operands must be numeric types".to_string());
                }
            }
            Column {
                prefix,
                colname,
                ref mut qunid,
                ref mut colid,
            } => {
                let (quncol, datatype, ..) = self.resolve_column(env, prefix.as_ref(), colname)?;
                *qunid = quncol.0;
                *colid = quncol.1;
                datatype
            }
            LogExpr(..) => DataType::Boolean,
            Literal(Datum::STR(_)) => DataType::Utf8,
            Literal(Datum::INT(_)) => DataType::Int64,
            Literal(Datum::BOOL(_)) => DataType::Boolean,
            AggFunction(aggtype, ..) => {
                if !agg_fns_allowed {
                    return Err(format!("Aggregate function {:?} not allowed.", aggtype));
                }
                match aggtype {
                    AggType::COUNT => DataType::Int64,
                    AggType::MIN | AggType::MAX => children_datatypes[0].clone(),
                    AggType::SUM | AggType::AVG => {
                        if is_numeric(&children_datatypes[0]) {
                            children_datatypes[0].clone()
                        } else {
                            return Err(format!("SUM() {:?} only allowed for numeric datatypes.", aggtype));
                        }
                    }
                }
            }
            NegatedExpr => {
                if is_numeric(&children_datatypes[0]) {
                    children_datatypes[0].clone()
                } else {
                    return Err(format!("Only numeric datatypes can be negated."));
                }
            }
            _ => {
                debug!("TODO: {:?}", &expr);
                todo!();
            }
        };
        node.properties.datatype = datatype;
        Ok(())
    }
}

fn is_numeric(dt: &DataType) -> bool {
    match dt {
        DataType::Int64 => true,
        _ => false,
    }
}
