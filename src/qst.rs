// QST: Query Semantic Transforms

use std::rc::Rc;

use crate::{
    datum::Datum,
    expr::{AggType, ArithOp, Expr, Expr::*, ExprGraph, ExprProp},
    graph::{ExprKey, Node, QueryBlockKey},
    includes::*,
    qgm::{NamedExpr, QGMMetadata, Quantifier, QueryBlock, QueryBlockGraph, QueryBlockType, QGM},
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
    pub fn resolve(qbkey: QueryBlockKey, env: &Env, qblock_graph: &mut QueryBlockGraph, expr_graph: &mut ExprGraph, metadata: &mut QGMMetadata) -> Result<(), String> {
        let qblock = &mut qblock_graph.get_mut(qbkey).value;

        // Ensure that every quantifier in this qblock is uniquely identifiable
        let qun_aliases = qblock.quns.iter().filter_map(|qun| qun.get_alias()).collect::<Vec<_>>();
        if has_duplicates(&qun_aliases) {
            return Err("Query has two or more quantifiers with the same aliases.".to_owned());
        }

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

        // Resolve any stars (*)
        qblock.resolve_star(env, expr_graph)?;

        // Resolve select list
        for ne in qblock.select_list.iter() {
            let expr_key = ne.expr_key;
            qblock.resolve_expr(env, expr_graph, expr_key, true)?;
        }

        // Resolve predicates
        if let Some(pred_list) = qblock.pred_list.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_key in pred_list {
                qblock.resolve_expr(env, expr_graph, expr_key, false)?;
                expr_key.get_boolean_factors(expr_graph, &mut boolean_factors)
            }
            qblock.pred_list = Some(boolean_factors);
        }

        // Resolve group-by
        if let Some(group_by) = qblock.group_by.as_ref() {
            for &expr_key in group_by.iter() {
                qblock.resolve_expr(env, expr_graph, expr_key, false)?;
            }
        }

        // Resolve having-clause
        if let Some(having_clause) = qblock.having_clause.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_key in having_clause {
                qblock.resolve_expr(env, expr_graph, expr_key, true)?;
                expr_key.get_boolean_factors(expr_graph, &mut boolean_factors)
            }
            qblock.having_clause = Some(boolean_factors);
        }

        // Resolve group-by/having clauses, if they exist
        // If a GROUP BY is present, all select_list expressions must either by included in the group_by, or they must be aggregate functions
        if qblock.group_by.is_some() {
            Self::split_groupby(qbkey, qblock_graph, expr_graph)?;
        }
        info!("Resolved qblock id: {}", qblock_id);
        Ok(())
    }

    pub fn split_groupby(qbkey: QueryBlockKey, qblock_graph: &mut QueryBlockGraph, expr_graph: &mut ExprGraph) -> Result<(), String> {
        let inner_qb_key = qblock_graph.add_node(QueryBlock::new0(expr_graph.next_id(), QueryBlockType::Select), None);
        //let [outer_qb_node, inner_qb_node] = qblock_graph.get_disjoint_mut([qbkey, inner_qb_key]);
        let outer_qb_node = qblock_graph.get_mut(qbkey);

        let outer_qb = &mut outer_qb_node.value;

        let agg_qun_id = expr_graph.next_id();

        // Replace group_by expressions with references to child qun
        let group_by = replace(&mut outer_qb.group_by, None).unwrap();
        let group_by_expr_count = group_by.len();

        let having_clause = replace(&mut outer_qb.having_clause, None);

        // Construct inner select-list by first adding GROUP-BY clause expressions
        let mut inner_select_list = group_by.iter().map(|&expr_key| NamedExpr::new(None, expr_key, expr_graph)).collect::<Vec<NamedExpr>>();

        // Augment inner select-list by extracting parameters from `agg(parameter)` expressions
        for ne in outer_qb.select_list.iter_mut() {
            Self::transform_groupby_expr(expr_graph, &mut inner_select_list, group_by_expr_count, agg_qun_id, &mut ne.expr_key)?;
        }

        // Transform HAVING clause expressions -> outer qb predicates
        let outer_pred_list = if let Some(having_clause) = having_clause {
            let mut new_having_clause = vec![];
            for having_pred in having_clause.iter() {
                let mut new_pred_id = *having_pred;
                Self::transform_groupby_expr(expr_graph, &mut inner_select_list, group_by_expr_count, agg_qun_id, &mut new_pred_id)?;
                new_having_clause.push(new_pred_id);
            }
            Some(new_having_clause)
        } else {
            None
        };

        let group_by = group_by
            .iter()
            .enumerate()
            .map(|(cid, ..)| {
                let gbcol_expr_key = inner_select_list[cid].expr_key;
                let gbcol_props = expr_graph.get_properties(gbcol_expr_key);
                let gbcol_props = ExprProp { data_type: gbcol_props.data_type().clone() };
                expr_graph.add_node_with_props(Expr::CID(agg_qun_id, cid), gbcol_props, None)
            })
            .collect::<Vec<_>>();

        let inner_qb = QueryBlock::new(
            expr_graph.next_id(),
            replace(&mut outer_qb.name, None),
            QueryBlockType::Select,
            inner_select_list,
            std::mem::take(&mut outer_qb.quns),
            std::mem::take(&mut outer_qb.pred_list),
            None,
            None,
            None,
            outer_qb.distinct,
            None,
        );

        let outer_qun = Quantifier::new_qblock(agg_qun_id, inner_qb_key, None);
        outer_qb.name = None;
        outer_qb.qbtype = QueryBlockType::GroupBy;
        outer_qb.quns = vec![outer_qun];
        outer_qb.pred_list = outer_pred_list;
        outer_qb.group_by = Some(group_by);

        let inner_qb_node = qblock_graph.get_mut(inner_qb_key);
        inner_qb_node.value = inner_qb;

        Ok(())
    }

    fn find(graph: &ExprGraph, select_list: &[NamedExpr], group_by_expr_count: usize, expr_key: ExprKey) -> Option<usize> {
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

    fn append(expr_graph: &ExprGraph, select_list: &mut Vec<NamedExpr>, expr_key: ExprKey) -> (usize, DataType) {
        // Does this expression already exist in the select_list?
        let cid = if let Some(ix) = Self::find(expr_graph, select_list, select_list.len(), expr_key) {
            // ... yes, return its index
            ix
        } else {
            // ... append it to list
            select_list.push(NamedExpr { alias: None, expr_key });
            select_list.len() - 1
        };
        let cid_props = expr_graph.get_properties(select_list[cid].expr_key);
        let data_type = cid_props.data_type().clone();
        (cid, data_type)
    }

    // transform_groupby_expr: Traverse expression `expr_key` (which is part of aggregate qb select list) such that any subexpressions are replaced with
    // corresponding references to inner/select qb select-list using CID#
    pub fn transform_groupby_expr(
        expr_graph: &mut ExprGraph, select_list: &mut Vec<NamedExpr>, group_by_expr_count: usize, qunid: QunId, expr_key: &mut ExprKey,
    ) -> Result<(), String> {
        //debug!("transform_groupby_expr: {:?}", expr_key.printable(&expr_graph, false));

        let node = expr_graph.get(*expr_key);
        if let AggFunction(aggtype, _) = node.value {
            // Aggregate-function: replace argument with CID reference to inner query-block
            let child_key = node.children.as_ref().unwrap()[0];
            let (cid, data_type) = Self::append(expr_graph, select_list, child_key);
            let new_child_key = if aggtype == AggType::AVG {
                // AVG -> SUM / COUNT
                let cid = expr_graph.add_node_with_props(CID(qunid, cid), ExprProp::new(data_type), None);
                let sum = expr_graph.add_node_with_props(AggFunction(AggType::SUM, false), ExprProp::new(DataType::Int64), Some(vec![cid]));
                let cnt = expr_graph.add_node_with_props(AggFunction(AggType::COUNT, false), ExprProp::new(DataType::Int64), Some(vec![cid]));
                expr_graph.add_node_with_props(BinaryExpr(ArithOp::Div), ExprProp::new(DataType::Float64), Some(vec![sum, cnt]))
            } else {
                expr_graph.add_node_with_props(CID(qunid, cid), ExprProp { data_type }, None)
            };
            let node = expr_graph.get_mut(*expr_key);
            node.children = Some(vec![new_child_key]);
            if aggtype == AggType::AVG {
                *expr_key = new_child_key;
            }
        } else if let Some(cid) = Self::find(expr_graph, select_list, group_by_expr_count, *expr_key) {
            // Expression in GROUP-BY list, all good
            let cid_props = expr_graph.get_properties(select_list[cid].expr_key);
            let data_type = cid_props.data_type().clone();
            let new_child_key = expr_graph.add_node_with_props(CID(qunid, cid), ExprProp { data_type }, None);
            *expr_key = new_child_key;
        } else if let Column { colname, .. } = &node.value {
            // User error: Unaggregated expression in select-list not in group-by clause
            return Err(format!("Column {} is referenced in select-list/having-clause but it is not specified in the GROUP-BY list", colname));
        } else if let Some(mut children) = node.children.clone() {
            let mut children2 = vec![];
            for child_key in children.iter_mut() {
                Self::transform_groupby_expr(expr_graph, select_list, group_by_expr_count, qunid, child_key)?;
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
                    return Err(format!("Column {} found in multiple tables. Use tablename prefix to disambiguate.", enquote(colname)));
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
            let colstr = if let Some(prefix) = prefix { format!("{}.{}", prefix, colname) } else { colname.to_string() };
            Err(format!("Column {} not found in any table.", colstr))
        }
    }

    pub fn resolve_expr(&self, env: &Env, expr_graph: &mut ExprGraph, expr_key: ExprKey, agg_fns_allowed: bool) -> Result<(), String> {
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
                self.resolve_expr(env, expr_graph, child_key, children_agg_fns_allowed)?;
                let datatype = expr_graph.get(child_key).properties.data_type().clone();
                children_datatypes.push(datatype);
            }
        }

        let node: &mut Node<ExprKey, Expr, ExprProp> = expr_graph.get_mut(expr_key);
        let expr = &mut node.value;
        //info!("Check: {:?}", expr);

        let datatype = match expr {
            CID(_, _) => node.properties.data_type().clone(),
            RelExpr(..) => {
                // Check argument types
                if children_datatypes[0] != children_datatypes[1] {
                    return Err(f!("Datatype mismatch: {:?} vs {:?}  ({}:{})", children_datatypes[0], children_datatypes[1], file!(), line!()));
                } else {
                    DataType::Boolean
                }
            }
            BinaryExpr(arithop) => {
                // Check argument types
                if is_numeric(&children_datatypes[0]) && is_numeric(&children_datatypes[1]) {
                    match arithop {
                        ArithOp::Add | ArithOp::Sub | ArithOp::Mul => children_datatypes[0].clone(),
                        ArithOp::Div => DataType::Float64,
                    }
                } else {
                    return Err("Binary operands must be numeric types".to_string());
                }
            }
            Column { prefix, colname, ref mut qunid, ref mut colid } => {
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
                    return Err(String::from("Only numeric datatypes can be negated."));
                }
            }
            _ => {
                panic!("Unexpected expression found: {:?}", &expr);
            }
        };
        node.properties.set_data_type(datatype);
        Ok(())
    }

    pub fn resolve_star(&mut self, _env: &Env, expr_graph: &mut ExprGraph) -> Result<(), String> {
        let select_list = replace(&mut self.select_list, vec![]);
        let mut new_select_list = vec![];

        for ne in select_list.into_iter() {
            let expr = expr_graph.get_value(ne.expr_key);
            let isstar_with_prefix = if let Expr::Star { prefix } = &expr { (true, prefix.clone()) } else { (false, None) };

            if isstar_with_prefix.0 {
                let prefix = isstar_with_prefix.1;

                // Handle two cases:
                //    SELECT * (no prefix)
                //    SELECT TABLENAME.* (matching prefix)
                let qun_iter = self.quns.iter().filter(|qun| if prefix.is_none() || qun.get_alias() == prefix.as_ref() { true } else { false });
                for qun in qun_iter {
                    let desc = &**qun.tabledesc.as_ref().unwrap();
                    for field in desc.fields().iter() {
                        let column = Column { prefix: prefix.clone(), colname: field.name.clone(), qunid: 0, colid: 0 };
                        let new_expr_key = expr_graph.add_node(column, None);
                        let named_expr = NamedExpr { alias: None, expr_key: new_expr_key };
                        new_select_list.push(named_expr);
                    }
                }
            } else {
                new_select_list.push(ne);
            }
        }
        self.select_list = new_select_list;
        Ok(())
    }
}

fn is_numeric(dt: &DataType) -> bool {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => true,
        _ => false,
    }
}
