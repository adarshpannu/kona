// QST: Query Semantics

use crate::ast::{Expr::*, *};
use crate::graph::{Graph, NodeId};
use crate::row::{DataType, Datum};

use crate::includes::*;
use log::Log;
use slotmap::secondary::Entry;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct QunColumn {
    qun_id: QunId,
    col_id: ColId,
    datatype: DataType,
}

impl QGM {
    pub fn resolve(&mut self, env: &Env) -> Result<(), String> {
        debug!("Normalize QGM");

        // Resolve top-level QB
        self.qblock.resolve(env, &mut self.graph)?;

        Ok(())
    }
}

fn dquote(s: &String) -> String {
    format!("\"{}\"", s)
}

pub struct QueryBlockColidDispenser {
    hashmap: HashMap<QunColumn, usize>,
    next_id: usize,
}

impl QueryBlockColidDispenser {
    fn new() -> QueryBlockColidDispenser {
        QueryBlockColidDispenser {
            hashmap: HashMap::new(),
            next_id: 0,
        }
    }

    fn next_id(&mut self, quncol: QunColumn) -> usize {
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
    pub fn resolve(&mut self, env: &Env, graph: &mut Graph<Expr>) -> Result<(), String> {
        debug!("Resolve query block: {}", self.id);

        let mut colid_dispenser = QueryBlockColidDispenser::new();

        //let qgm_resolved_filename = format!("{}/{}", GRAPHVIZDIR, "qgm_resolved.dot");
        //qgm.write_qgm_to_graphviz(&qgm_resolved_filename, false);

        // Resolve QUNs first (base tables and any nested subqueries)
        for qun in self.quns.iter_mut() {
            if let Some(tablename) = qun.tablename.as_ref() {
                let tbdesc = env.metadata.get_tabledesc(tablename);
                if tbdesc.is_none() {
                    return Err(format!("Table {} not cataloged.", dquote(tablename)));
                }
                qun.tabledesc = tbdesc;
            } else if let Some(qblock) = qun.qblock.as_ref() {
                let mut qblock = qblock.borrow_mut();
                qblock.resolve(env, graph);
            }
        }

        // Resolve select list
        for ne in self.select_list.iter() {
            let expr_id = ne.expr_id;
            self.resolve_expr(env, graph, &mut colid_dispenser, expr_id, true)?;
        }

        // Resolve predicates
        if let Some(pred_list) = self.pred_list.as_ref() {
            for &expr_id in pred_list {
                self.resolve_expr(env, graph, &mut colid_dispenser, expr_id, false)?;
            }
        }

        // Resolve group-by
        if let Some(group_by) = self.group_by.as_ref() {
            for &expr_id in group_by.iter() {
                self.resolve_expr(env, graph, &mut colid_dispenser, expr_id, false)?;
            }
        }

        // Resolve having-clause
        if let Some(having_clause) = self.having_clause.as_ref() {
            for &expr_id in having_clause {
                self.resolve_expr(env, graph, &mut colid_dispenser, expr_id, true)?;
            }
        }

        for qun in self.quns.iter() {
            //info!("Qun: {}, column_map:{:?}", qun.id, qun.column_read_map)
        }

        // Resolve group-by/having clauses, if they exist
        // If a GROUP BY is present, all select_list expressions must either by included in the group_by, or they must be aggregate functions
        self.split_groupby(env, graph)?;

        Ok(())
    }

    pub fn split_groupby(&mut self, env: &Env, graph: &mut Graph<Expr>) -> Result<(), String> {
        if self.group_by.is_some() {
            //let select_list = replace(&mut self.select_list, vec![]);
            let group_by = replace(&mut self.group_by, None).unwrap();
            let group_by_expr_count = group_by.len();

            let having_clause = replace(&mut self.having_clause, None);

            // Transform group_by into inner-select-list
            let mut inner_select_list = group_by
                .into_iter()
                .map(|expr_id| NamedExpr::new(None, expr_id, &graph))
                .collect::<Vec<NamedExpr>>();

            // Contruct outer select-list by replacing agg() references with inner columns
            for ne in self.select_list.iter_mut() {
                Self::transform_groupby_expr(env, graph, &mut inner_select_list, group_by_expr_count, &mut ne.expr_id)?;
            }

            // Fixup having clause -> outer qb filter
            let outer_pred_list = if let Some(having_clause) = having_clause {
                let mut new_having_clause = vec![];
                for having_pred in having_clause.iter() {
                    let mut new_pred_id = *having_pred;
                    Self::transform_groupby_expr(
                        env,
                        graph,
                        &mut inner_select_list,
                        group_by_expr_count,
                        &mut new_pred_id,
                    );
                    new_having_clause.push(new_pred_id);
                }
                Some(new_having_clause)
            } else {
                None
            };

            let outer_qb = self;
            let inner_qb = QueryBlock::new(
                graph.next_id(),
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
            let inner_qb = Some(Rc::new(RefCell::new(inner_qb)));
            let outer_qun = Quantifier::new(graph.next_id(), None, inner_qb, None);
            outer_qb.name = None;
            outer_qb.qbtype = QueryBlockType::GroupBy;
            outer_qb.quns = vec![outer_qun];
            outer_qb.pred_list = outer_pred_list;
        }
        Ok(())
    }

    fn find(
        graph: &Graph<Expr>, select_list: &Vec<NamedExpr>, group_by_expr_count: usize, expr_id: NodeId,
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

    fn append(graph: &Graph<Expr>, select_list: &mut Vec<NamedExpr>, expr_id: NodeId) -> usize {
        // Does this expression already exist in the select_list?
        if let Some(ix) = Self::find(graph, select_list, select_list.len(), expr_id) {
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
        env: &Env, graph: &mut Graph<Expr>, select_list: &mut Vec<NamedExpr>, group_by_expr_count: usize,
        expr_id: &mut NodeId,
    ) -> Result<(), String> {
        let node = graph.get_node(*expr_id);
        if let AggFunction(aggtype, _) = node.inner {
            // Aggregate-function: replace argument with CID reference to inner query-block
            let child_id = node.children.as_ref().unwrap()[0];
            let cid = Self::append(graph, select_list, child_id);
            let new_child_id = if aggtype == AggType::AVG {
                // AVG -> SUM / COUNT
                let cid = graph.add_node(CID(cid), None);
                let sum = graph.add_node(AggFunction(AggType::SUM, false), Some(vec![cid]));
                let cnt = graph.add_node(AggFunction(AggType::COUNT, false), Some(vec![cid]));
                graph.add_node(BinaryExpr(ArithOp::Div), Some(vec![sum, cnt]))
            } else {
                graph.add_node(CID(cid), None)
            };
            let node = graph.get_node_mut(*expr_id);
            node.children = Some(vec![new_child_id]);
        } else if let Some(cid) = Self::find(&graph, &select_list, group_by_expr_count, *expr_id) {
            // Expression in GROUP-BY list, all good
            let new_child_id = graph.add_node(CID(cid), None);
            *expr_id = new_child_id;
        } else if let Column {
            prefix,
            colname,
            qun_id,
            offset,
        } = &node.inner
        {
            // User error: Unaggregated expression in select-list not in group-by clause
            return Err(format!(
                "Column {} is referenced in select-list/having-clause but it is not specified in the GROUP-BY list",
                colname
            ));
        } else if let Some(mut children) = node.children.clone() {
            let mut children2 = vec![];
            for child_id in children.iter_mut() {
                Self::transform_groupby_expr(env, graph, select_list, group_by_expr_count, child_id)?;
                children2.push(*child_id);
            }
            let node = graph.get_node_mut(*expr_id);
            node.children = Some(children);
        }
        Ok(())
    }

    pub fn resolve_column(
        &self, env: &Env, colid_dispenser: &mut QueryBlockColidDispenser, prefix: Option<&String>, colname: &String,
    ) -> Result<(QunColumn, usize), String> {
        let mut retval = None;
        let mut offset = 0;

        for qun in self.quns.iter() {
            let desc = qun.tabledesc.as_ref().unwrap().clone();
            let coldesc = if let Some(prefix) = prefix {
                // Prefixed column: look at specific qun
                if qun.matches_name_or_alias(prefix) {
                    desc.get_coldesc(colname)
                } else {
                    None
                }
            } else {
                // Unprefixed column: look at all QUNs
                desc.get_coldesc(colname)
            };

            if let Some(coldesc) = coldesc {
                if retval.is_none() {
                    retval = Some(QunColumn {
                        qun_id: qun.id,
                        col_id: coldesc.0,
                        datatype: coldesc.1,
                    });
                    offset = colid_dispenser.next_id(retval.unwrap());
                    let mut column_map = qun.column_read_map.borrow_mut();
                    column_map.insert(coldesc.0, offset);
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
            Ok((retval, offset))
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
        &self, env: &Env, graph: &mut Graph<Expr>, colid_dispenser: &mut QueryBlockColidDispenser, expr_id: NodeId,
        agg_fns_allowed: bool,
    ) -> Result<DataType, String> {
        let children_agg_fns_allowed = if let AggFunction(_, _) = graph.get_node(expr_id).inner {
            // Nested aggregate functions not allowed
            false
        } else {
            agg_fns_allowed
        };

        let children = graph.get_children(expr_id);
        let mut children_datatypes = vec![];

        if let Some(children) = children {
            for child_id in children {
                let datatype = self.resolve_expr(env, graph, colid_dispenser, child_id, children_agg_fns_allowed)?;
                children_datatypes.push(datatype);
            }
        }

        let mut node = graph.get_node_mut(expr_id);
        let mut expr = &mut node.inner;
        //info!("Check: {:?}", expr);

        let datatype = match expr {
            CID(_) => node.datatype,
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
                ref mut qun_id,
                ref mut offset,
            } => {
                let (quncol, qtuple_ix) = self.resolve_column(env, colid_dispenser, prefix.as_ref(), colname)?;
                *qun_id = quncol.qun_id;
                *offset = qtuple_ix;
                //debug!("ASSIGN {:?}.{:?} = qun_id={}, qtuple_ix={}", prefix, colname, qun_id, qtuple_ix);
                quncol.datatype
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
        node.datatype = datatype;
        Ok(datatype)
    }

    pub fn extract(&mut self, graph: &mut Graph<Expr>, pred_id: NodeId, pred_list: &mut Vec<NodeId>) {
        let (expr, children) = graph.get_node_with_children(pred_id);
        if let LogExpr(crate::ast::LogOp::And) = expr {
            let children = children.unwrap();
            let lhs = children[0];
            let rhs = children[1];
            self.extract(graph, lhs, pred_list);
            self.extract(graph, rhs, pred_list);
        } else {
            pred_list.push(pred_id)
        }
    }
}
