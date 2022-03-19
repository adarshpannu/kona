// QST: Query Semantic Transforms

use crate::expr::{Expr::*, *};
use crate::graph::*;
use crate::qgm::*;
use crate::row::{DataType, Datum};

use crate::includes::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

impl QGM {
    pub fn resolve(&mut self, env: &Env) -> Result<(), String> {
        debug!("Normalize QGM");

        // Resolve top-level QB
        let qbkey = self.main_qblock_key;
        QueryBlock::resolve(
            qbkey,
            env,
            &mut self.qblock_graph,
            &mut self.expr_graph,
            &mut self.metadata,
        )?;

        Ok(())
    }
}

fn dquote(s: &String) -> String {
    format!("\"{}\"", s)
}

pub struct QueryBlockColidDispenser {
    hashmap: HashMap<QunCol, ColId>,
    next_id: ColId,
}

impl QueryBlockColidDispenser {
    fn new() -> QueryBlockColidDispenser {
        QueryBlockColidDispenser {
            hashmap: HashMap::new(),
            next_id: 0,
        }
    }

    fn next_id(&mut self, quncol: QunCol) -> ColId {
        let next_id = self.next_id;
        let e = self.hashmap.entry(quncol).or_insert(next_id);
        if *e == next_id {
            self.next_id = next_id + 1;
        }
        //debug!("Assigned {:?} -> {}", &quncol, *e);
        *e
    }
}

impl QueryBlock {
    pub fn resolve(
        qbkey: QueryBlockKey, env: &Env, qblock_graph: &mut QueryBlockGraph, expr_graph: &mut ExprGraph,
        metadata: &mut QGMMetadata,
    ) -> Result<(), String> {
        let mut qblock = qblock_graph.get1_mut(qbkey).get_select_block_mut();
        let mut qbkey_children = vec![];

        debug!("Resolve query block: {}", qblock.id);

        for qun in qblock.quns.iter_mut() {
            if let Some(qbkey) = qun.qblock.as_ref() {
                qbkey_children.push(*qbkey);
            }
        }

        for qbkey in qbkey_children {
            Self::resolve(qbkey, env, qblock_graph, expr_graph, metadata);
        }

        let mut qblock = qblock_graph.get1_mut(qbkey).get_select_block_mut();

        let mut colid_dispenser = QueryBlockColidDispenser::new();

        // Resolve QUNs first (base tables and any nested subqueries)
        for qun in qblock.quns.iter_mut() {
            if let Some(tablename) = qun.tablename.as_ref() {
                let tbdesc = env.metadata.get_tabledesc(tablename);
                if let Some(tbdesc0) = tbdesc.as_ref() {
                    metadata.add_tabledesc(qun.id, Rc::clone(tbdesc0));
                    qun.tabledesc = tbdesc;
                } else {
                    return Err(format!("Table {} not cataloged.", dquote(tablename)));
                }
            }
        }

        // Resolve select list
        for ne in qblock.select_list.iter() {
            let expr_id = ne.expr_id;
            qblock.resolve_expr(env, expr_graph, metadata, &mut colid_dispenser, expr_id, true)?;
        }

        // Resolve predicates
        if let Some(pred_list) = qblock.pred_list.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_id in pred_list {
                qblock.resolve_expr(env, expr_graph, metadata, &mut colid_dispenser, expr_id, false)?;
                Self::get_boolean_factors(&expr_graph, expr_id, &mut boolean_factors)
            }
            qblock.pred_list = Some(boolean_factors);
        }

        // Resolve group-by
        if let Some(group_by) = qblock.group_by.as_ref() {
            for &expr_id in group_by.iter() {
                qblock.resolve_expr(env, expr_graph, metadata, &mut colid_dispenser, expr_id, false)?;
            }
        }

        // Resolve having-clause
        if let Some(having_clause) = qblock.having_clause.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_id in having_clause {
                qblock.resolve_expr(env, expr_graph, metadata, &mut colid_dispenser, expr_id, true)?;
                Self::get_boolean_factors(&expr_graph, expr_id, &mut boolean_factors)
            }
            qblock.having_clause = Some(boolean_factors);
        }

        for qun in qblock.quns.iter() {
            //info!("Qun: {}, column_map:{:?}", qun.id, qun.column_read_map)
        }

        // Resolve group-by/having clauses, if they exist
        // If a GROUP BY is present, all select_list expressions must either by included in the group_by, or they must be aggregate functions
        if qblock.group_by.is_some() {
            Self::split_groupby(qbkey, env, qblock_graph, expr_graph)?;
        }

        Ok(())
    }

    pub fn split_groupby(
        qbkey: QueryBlockKey, env: &Env, qblock_graph: &mut QueryBlockGraph, expr_graph: &mut ExprGraph,
    ) -> Result<(), String> {
        let inner_qb_key = qblock_graph.add_node(QueryBlockNode::Unassigned, None);
        let [outer_qb_node, inner_qb_node] = qblock_graph.get1_disjoint_mut([qbkey, inner_qb_key]);

        let mut qblock = outer_qb_node.contents.get_select_block_mut();

        let agg_qun_id = expr_graph.next_id();

        //let select_list = replace(&mut qblock.select_list, vec![]);
        let group_by = replace(&mut qblock.group_by, None).unwrap();
        let group_by_expr_count = group_by.len();

        let having_clause = replace(&mut qblock.having_clause, None);

        // Transform group_by into inner-select-list
        let mut inner_select_list = group_by
            .into_iter()
            .map(|expr_id| NamedExpr::new(None, expr_id, &expr_graph))
            .collect::<Vec<NamedExpr>>();

        // Contruct outer select-list by replacing agg() references with inner columns
        for ne in qblock.select_list.iter_mut() {
            Self::transform_groupby_expr(
                env,
                expr_graph,
                &mut inner_select_list,
                group_by_expr_count,
                agg_qun_id,
                &mut ne.expr_id,
            )?;
        }

        // Fixup having clause -> outer qb filter
        let outer_pred_list = if let Some(having_clause) = having_clause {
            let mut new_having_clause = vec![];
            for having_pred in having_clause.iter() {
                let mut new_pred_id = *having_pred;
                Self::transform_groupby_expr(
                    env,
                    expr_graph,
                    &mut inner_select_list,
                    group_by_expr_count,
                    agg_qun_id,
                    &mut new_pred_id,
                );
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

        inner_qb_node.contents = QueryBlockNode::Select(inner_qb);
        let outer_qun = Quantifier::new(agg_qun_id, None, Some(inner_qb_key), None);
        outer_qb.name = None;
        outer_qb.qbtype = QueryBlockType::GroupBy;
        outer_qb.quns = vec![outer_qun];
        outer_qb.pred_list = outer_pred_list;

        Ok(())
    }

    fn find(
        graph: &ExprGraph, select_list: &Vec<NamedExpr>, group_by_expr_count: usize, expr_id: ExprKey,
    ) -> Option<usize> {
        // Does this expression already exist in the select_list[..until_index]?
        for (ix, ne) in select_list.iter().enumerate() {
            if ix >= group_by_expr_count {
                return None;
            } else if Expr::isomorphic(graph, expr_id, ne.expr_id) {
                return Some(ix);
            }
        }
        None
    }

    fn append(expr_graph: &ExprGraph, select_list: &mut Vec<NamedExpr>, expr_id: ExprKey) -> usize {
        // Does this expression already exist in the select_list?
        if let Some(ix) = Self::find(expr_graph, select_list, select_list.len(), expr_id) {
            // ... yes, return its index
            return ix;
        }
        // ... append it to list
        select_list.push(NamedExpr { alias: None, expr_id });
        return select_list.len() - 1;
    }

    // transform_groupby_expr: Traverse expression `expr_id` (which is part of aggregate qb select list) such that any subexpressions are replaced with
    // corresponding references to inner/select qb select-list using CID#
    pub fn transform_groupby_expr(
        env: &Env, expr_graph: &mut ExprGraph, select_list: &mut Vec<NamedExpr>, group_by_expr_count: usize,
        qunid: QunId, expr_id: &mut ExprKey,
    ) -> Result<(), String> {
        let node = expr_graph.get(*expr_id);
        if let AggFunction(aggtype, _) = node.contents {
            // Aggregate-function: replace argument with CID reference to inner query-block
            let child_id = node.children.as_ref().unwrap()[0];
            let cid = Self::append(expr_graph, select_list, child_id);
            let new_child_id = if aggtype == AggType::AVG {
                // AVG -> SUM / COUNT
                let cid = expr_graph.add_node(CID(qunid, cid), None);
                let sum = expr_graph.add_node(AggFunction(AggType::SUM, false), Some(vec![cid]));
                let cnt = expr_graph.add_node(AggFunction(AggType::COUNT, false), Some(vec![cid]));
                expr_graph.add_node(BinaryExpr(ArithOp::Div), Some(vec![sum, cnt]))
            } else {
                expr_graph.add_node(CID(qunid, cid), None)
            };
            let node = expr_graph.get_mut(*expr_id);
            node.children = Some(vec![new_child_id]);
        } else if let Some(cid) = Self::find(&expr_graph, &select_list, group_by_expr_count, *expr_id) {
            // Expression in GROUP-BY list, all good
            let new_child_id = expr_graph.add_node(CID(qunid, cid), None);
            *expr_id = new_child_id;
        } else if let Column {
            prefix,
            colname,
            qunid,
            colid,
        } = &node.contents
        {
            // User error: Unaggregated expression in select-list not in group-by clause
            return Err(format!(
                "Column {} is referenced in select-list/having-clause but it is not specified in the GROUP-BY list",
                colname
            ));
        } else if let Some(mut children) = node.children.clone() {
            let mut children2 = vec![];
            for child_id in children.iter_mut() {
                Self::transform_groupby_expr(env, expr_graph, select_list, group_by_expr_count, qunid, child_id)?;
                children2.push(*child_id);
            }
            let node = expr_graph.get_mut(*expr_id);
            node.children = Some(children);
        }
        Ok(())
    }

    pub fn resolve_column(
        &self, env: &Env, colid_dispenser: &mut QueryBlockColidDispenser, prefix: Option<&String>, colname: &String,
    ) -> Result<(QunCol, DataType, ColId), String> {
        let mut retval = None;
        let mut colid = 0;

        for qun in self.quns.iter() {
            let desc = qun.tabledesc.as_ref().unwrap().clone();
            let coldesc = if let Some(prefix) = prefix {
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

            if let Some(coldesc) = coldesc {
                if retval.is_none() {
                    retval = Some((QunCol(qun.id, coldesc.colid), coldesc.datatype));
                    colid = colid_dispenser.next_id(retval.unwrap().0);
                    let mut column_map = qun.column_read_map.borrow_mut();
                    column_map.insert(coldesc.colid, colid);
                } else {
                    return Err(format!(
                        "Column {} found in multiple tables. Use tablename prefix to disambiguate.",
                        dquote(colname)
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
        &self, env: &Env, expr_graph: &mut ExprGraph, metadata: &mut QGMMetadata,
        colid_dispenser: &mut QueryBlockColidDispenser, expr_id: ExprKey, agg_fns_allowed: bool,
    ) -> Result<DataType, String> {
        let children_agg_fns_allowed = if let AggFunction(_, _) = expr_graph.get(expr_id).contents {
            // Nested aggregate functions not allowed
            false
        } else {
            agg_fns_allowed
        };

        let children = expr_graph.get_children(expr_id);
        let mut children_datatypes = vec![];

        if let Some(children) = children {
            for child_id in children {
                let datatype = self.resolve_expr(
                    env,
                    expr_graph,
                    metadata,
                    colid_dispenser,
                    child_id,
                    children_agg_fns_allowed,
                )?;
                children_datatypes.push(datatype);
            }
        }

        let mut node = expr_graph.get_mut(expr_id);
        let mut expr = &mut node.contents;
        //info!("Check: {:?}", expr);

        let datatype = match expr {
            CID(_, _) => node.properties.datatype,
            RelExpr(relop) => {
                // Check argument types
                if children_datatypes[0] != children_datatypes[1] {
                    return Err("Datatype mismatch".to_string());
                } else {
                    DataType::BOOL
                }
            }
            BinaryExpr(binop) => {
                // Check argument types
                if children_datatypes[0] != DataType::INT || children_datatypes[0] != DataType::INT {
                    return Err("Binary operands must be numeric types".to_string());
                // FIXME for other numeric types. Also support string addition?
                } else {
                    children_datatypes[0]
                }
            }
            Column {
                prefix,
                colname,
                qunid: ref mut qunid,
                colid: ref mut colid,
            } => {
                let (quncol, datatype, qtuple_ix) =
                    self.resolve_column(env, colid_dispenser, prefix.as_ref(), colname)?;
                *qunid = quncol.0;
                *colid = quncol.1;
                /// FIXME - No longer using qtuple!! *colid =  qtuple_ix;
                //debug!("ASSIGN {:?}.{:?} = qunid={}, qtuple_ix={}", prefix, colname, qunid, qtuple_ix);
                datatype
            }
            LogExpr(logop) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            Literal(Datum::INT(_)) => DataType::INT,
            Literal(Datum::DOUBLE(_, _)) => DataType::DOUBLE,
            Literal(Datum::BOOL(_)) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            AggFunction(aggtype, is_distinct) => {
                if !agg_fns_allowed {
                    return Err(format!("Aggregate function {:?} not allowed.", aggtype));
                }
                match aggtype {
                    AggType::COUNT => DataType::INT,
                    AggType::MIN | AggType::MAX => children_datatypes[0],
                    AggType::SUM | AggType::AVG => {
                        if children_datatypes[0] != DataType::INT && children_datatypes[0] != DataType::DOUBLE {
                            return Err(format!("SUM() {:?} only allowed for numeric datatypes.", aggtype));
                        }
                        children_datatypes[0]
                    }
                }
            }
            _ => {
                debug!("TODO: {:?}", &expr);
                todo!();
            }
        };
        node.properties.datatype = datatype;
        Ok(datatype)
    }

    pub fn get_boolean_factors(expr_graph: &ExprGraph, pred_id: ExprKey, boolean_factors: &mut Vec<ExprKey>) {
        let (expr, _, children) = expr_graph.get3(pred_id);
        if let LogExpr(crate::expr::LogOp::And) = expr {
            let children = children.unwrap();
            let lhs = children[0];
            let rhs = children[1];
            Self::get_boolean_factors(expr_graph, lhs, boolean_factors);
            Self::get_boolean_factors(expr_graph, rhs, boolean_factors);
        } else {
            boolean_factors.push(pred_id)
        }
    }
}
