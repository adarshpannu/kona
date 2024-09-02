// QST: Query Semantic Transforms

use core::panic;
use std::rc::Rc;

use arrow2::compute::cast::can_cast_types;
use itertools::Itertools;

use crate::{
    datum::{get_rank, is_numeric},
    expr::{AggType, ArithOp, Expr, Expr::*, ExprGraph, ExprProp},
    graph::{ExprKey, Node, QueryBlockKey},
    includes::*,
    metadata::{QueryDesc, TableDesc},
    qgm::{NamedExpr, QGMMetadata, Quantifier, QueryBlock, QueryBlockGraph, QueryBlockType, QGM},
};

impl QGM {
    pub fn resolve(&mut self, env: &Env) -> Result<(), String> {
        // Resolve top-level QB
        let qbkey = self.main_qblock_key;
        QueryBlock::resolve(qbkey, env, self)?;

        Ok(())
    }

    pub fn borrow_parts(&mut self) -> (&mut QueryBlockGraph, &mut ExprGraph, &mut QGMMetadata) {
        (&mut self.qblock_graph, &mut self.expr_graph, &mut self.metadata)
    }
}

impl QueryBlock {
    pub fn resolve(qbkey: QueryBlockKey, env: &Env, qgm: &mut QGM) -> Result<Rc<dyn TableDesc>, String> {
        // Resolve group-by/having clauses, if they exist
        // If a GROUP BY is present, all select_list expressions must either by included in the group_by, or they must be aggregate functions
        let qblock = &mut qgm.qblock_graph.get_mut(qbkey).value;
        //let qbid = qblock.id;

        if qblock.group_by.is_some() {
            Self::split_groupby(qbkey, qgm)?;
        }

        //let qgm_postsplit_pathname = format!("{}/QB_{}-{}", env.output_dir, qbid, "qgm_postsplit.dot");
        //qgm.write_qgm_to_graphviz(&qgm_postsplit_pathname, false)?;

        let qblock = &mut qgm.qblock_graph.get_mut(qbkey).value;
        let is_group_by = qblock.qbtype == QueryBlockType::GroupBy;

        // Ensure that every quantifier in this qblock is uniquely identifiable
        let qun_aliases = qblock.quns.iter().filter_map(|qun| qun.get_alias().cloned()).collect::<Vec<_>>();
        if has_duplicates(&qun_aliases) {
            return Err("Query has two or more quantifiers with the same aliases.".to_owned());
        }

        // Resolve nested query blocks first
        let qbkey_children: Vec<(QunId, QueryBlockKey)> = qblock.quns.iter().filter_map(|qun| qun.get_qblock().map(|qbkey| (qun.id, qbkey))).collect();
        for (qunid, qbkey) in qbkey_children {
            let qdesc = Self::resolve(qbkey, env, qgm)?;
            qgm.metadata.add_tabledesc(qunid, qdesc);
        }

        let (qblock_graph, expr_graph, metadata) = qgm.borrow_parts();
        let qblock = &mut qblock_graph.get_mut(qbkey).value;
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
            qblock.resolve_expr(env, expr_graph, metadata, expr_key, is_group_by)?;
        }

        // Resolve predicates
        if let Some(pred_list) = qblock.pred_list.as_ref() {
            let mut boolean_factors = vec![];
            for &expr_key in pred_list {
                qblock.resolve_expr(env, expr_graph, metadata, expr_key, is_group_by)?;
                expr_key.get_boolean_factors(expr_graph, &mut boolean_factors)
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
                qblock.resolve_expr(env, expr_graph, metadata, expr_key, is_group_by)?;
                expr_key.get_boolean_factors(expr_graph, &mut boolean_factors)
            }
            qblock.having_clause = Some(boolean_factors);
        }

        info!("Resolved qblock id: {}", qblock_id);

        let qdesc = qblock.get_projection(expr_graph);

        Ok(qdesc)
    }

    pub fn get_projection(&self, expr_graph: &ExprGraph) -> Rc<dyn TableDesc> {
        let fields = self
            .select_list
            .iter()
            .map(|ne| {
                let expr_key = ne.expr_key;
                let name = ne.get_name();
                let typ = expr_key.get_data_type(expr_graph);

                Field::new(name, typ.clone(), false)
            })
            .collect_vec();
        let qdesc = Rc::new(QueryDesc::new(fields));
        qdesc
    }

    pub fn split_groupby(qbkey: QueryBlockKey, qgm: &mut QGM) -> Result<(), String> {
        let (qblock_graph, expr_graph, ..) = qgm.borrow_parts();

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
        let mut inner_select_list = group_by.iter().map(|&expr_key| NamedExpr::new(None, expr_key)).collect::<Vec<NamedExpr>>();

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
    // SUM(c1) => Outer QB             SUM ($qunid.colid)
    //                                        |
    //                                        V
    //            Inner QB select list    => c1
    pub fn transform_groupby_expr(
        expr_graph: &mut ExprGraph, select_list: &mut Vec<NamedExpr>, group_by_expr_count: usize, qunid: QunId, expr_key: &mut ExprKey,
    ) -> Result<(), String> {
        //debug!("transform_groupby_expr: {:?}", expr_key.describe(&expr_graph, false));

        let node = expr_graph.get(*expr_key);
        if let AggFunction(aggtype, _) = node.value {
            // Aggregate-function: replace argument with CID reference to inner query-block
            let child_key = node.children.as_ref().unwrap()[0];
            let (cid, data_type) = Self::append(expr_graph, select_list, child_key);
            let new_child_key = if aggtype == AggType::AVG {
                // AVG -> SUM / COUNT
                let cid = expr_graph.add_node_with_props(CID(qunid, cid), ExprProp::new(data_type.clone()), None);
                let sum = expr_graph.add_node_with_props(AggFunction(AggType::SUM, false), ExprProp::new(data_type), Some(vec![cid]));
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

    #[tracing::instrument(fields(children = ?children, children_datatypes = ?children_datatypes), skip_all, parent = None)]
    pub fn harmonize_expr_types(expr_graph: &mut ExprGraph, children: &Vec<ExprKey>, children_datatypes: &Vec<DataType>) -> Result<(DataType, Option<Vec<ExprKey>>), String> {
        let (lhs, rhs) = (expr_graph.get_value(children[0]), expr_graph.get_value(children[1]));
        let (lhs_key, rhs_key) = (children[0], children[1]);
        let (lhs_datatype, rhs_datatype) = (&children_datatypes[0], &children_datatypes[1]);
        let is_lhs_literal = matches!(lhs, Expr::Literal(_));
        let is_rhs_literal = matches!(rhs, Expr::Literal(_));

        if children_datatypes[0] != children_datatypes[1] {
            // Cases:
            //    Literal vs Literal  (2 == 2.0)
            //    Expr vs Literal     (intcol vs 2.0) / (floatcol vs 2)
            //    Literal vs Expr     (same as above, but flipped)
            //    Expr vs Expr        (intcol = floatcol)
            match (is_lhs_literal, is_rhs_literal) {
                (false, false) => {
                    let (lhs_rank, rhs_rank) = (get_rank(lhs_datatype), get_rank(rhs_datatype));
                    if lhs_rank > rhs_rank {
                        if !can_cast_types(rhs_datatype, lhs_datatype) {
                            return Err(f!("Cannot cast {:?} to {:?}", rhs_datatype, lhs_datatype));
                        }
                        // Upcast RHS (e.g. floatcol = intcol) => floatcol = cast(intcol as float)
                        let cast_props = ExprProp { data_type: lhs_datatype.clone() };
                        let cast_node = expr_graph.add_node_with_props(Expr::Cast, cast_props, Some(vec![rhs_key]));
                        return Ok((lhs_datatype.clone(), Some(vec![lhs_key, cast_node])));
                    } else {
                        // Upcast LHS (e.g. intcol = floatcol)
                    }
                }
                _ => {
                    let lhsstr = lhs_key.describe(&expr_graph, false);
                    let rhsstr = rhs_key.describe(&expr_graph, false);
                    panic!("Cannot harmonize {}: {:?} and {}: {:?}", lhsstr, lhs_datatype, rhsstr, rhs_datatype);
                }
            }
            //return Err(f!("Binary operands don't have identical types: {:?} vs {:?}", children_datatypes[0], children_datatypes[1]));
        } else {
        }
        Ok((children_datatypes[0].clone(), Some(children.clone())))
    }

    #[tracing::instrument(fields(expr = expr_key.to_string()), skip_all, parent = None)]
    pub fn resolve_expr(&self, env: &Env, expr_graph: &mut ExprGraph, metadata: &mut QGMMetadata, expr_key: ExprKey, agg_fns_allowed: bool) -> Result<(), String> {
        debug!("Unresolved expression: {} ...", expr_key.describe(expr_graph, false));

        let children_agg_fns_allowed = if let AggFunction(_, _) = expr_graph.get(expr_key).value {
            // Nested aggregate functions not allowed
            false
        } else {
            agg_fns_allowed
        };

        // Resolve children first
        let mut children_datatypes = vec![];
        let children = expr_graph.get(expr_key).children.clone();
        if let Some(children) = children.clone() {
            for child_key in children {
                self.resolve_expr(env, expr_graph, metadata, child_key, children_agg_fns_allowed)?;
                let datatype = expr_graph.get(child_key).properties.data_type().clone();
                children_datatypes.push(datatype);
            }
        }

        // Now resolve current expr node
        let (expr, props, ..) = expr_graph.get3(expr_key);

        let (resolved_expr, resolved_datatype, resolved_children) = match expr {
            CID(qunid, colid) => {
                let quncol = QunCol(*qunid, *colid);
                let datatype = metadata.get_fieldtype(quncol).unwrap();
                (None, datatype, children)
            }
            RelExpr(..) => {
                // Check argument types
                if children_datatypes[0] != children_datatypes[1] {
                    return Err(f!("Datatype mismatch: {:?} vs {:?}  ({}:{})", children_datatypes[0], children_datatypes[1], file!(), line!()));
                }
                (None, DataType::Boolean, children)
            }
            BinaryExpr(arithop) => {
                let arithop = *arithop;
                // Check argument types
                if is_numeric(&children_datatypes[0]) && is_numeric(&children_datatypes[1]) {
                    let (datatype, children) = Self::harmonize_expr_types(expr_graph, &children.unwrap(), &children_datatypes)?;
                    let datatype = match arithop {
                        ArithOp::Add | ArithOp::Sub | ArithOp::Mul => datatype,
                        ArithOp::Div => DataType::Float64,
                    };
                    (None, datatype, children)
                } else {
                    return Err("Binary operands must be numeric types".to_string());
                }
            }
            Column { prefix, colname, .. } => {
                let (quncol, datatype, ..) = self.resolve_column(env, prefix.as_ref(), colname)?;
                let resolved_expr = Some(Column { prefix: prefix.clone(), colname: colname.clone(), qunid: quncol.0, colid: quncol.1 });
                (resolved_expr, datatype, None)
            }
            LogExpr(..) => (None, DataType::Boolean, children),
            Literal(Utf8(_)) => (None, DataType::Utf8, children),
            Literal(Int64(_)) => (None, DataType::Int64, children),
            Literal(Float64(_)) => (None, DataType::Float64, children),
            Literal(Boolean(_)) => (None, DataType::Boolean, children),
            AggFunction(aggtype, ..) => {
                if !agg_fns_allowed {
                    return Err(format!("Aggregate function {:?} not allowed.", aggtype));
                }
                let datatype = match aggtype {
                    AggType::COUNT => DataType::Int64,
                    AggType::MIN | AggType::MAX => children_datatypes[0].clone(),
                    AggType::SUM => {
                        if is_numeric(&children_datatypes[0]) {
                            children_datatypes[0].clone()
                        } else {
                            return Err(format!("SUM() {:?} only allowed for numeric datatypes.", aggtype));
                        }
                    }
                    AggType::AVG => DataType::Float64,
                };
                (None, datatype, children)
            }
            NegatedExpr => {
                if is_numeric(&children_datatypes[0]) {
                    (None, children_datatypes[0].clone(), children)
                } else {
                    return Err(String::from("Only numeric datatypes can be negated."));
                }
            }
            Cast => {
                // Perform cast
                let child_expr_key = expr_graph.get(expr_key).children.as_ref().unwrap()[0];
                let from_expr = expr_graph.get_value(child_expr_key);
                let new_value = Self::resolve_cast(from_expr, props.data_type())?;
                let datatype = new_value.datatype();
                (Some(Literal(new_value)), datatype, None)
            }
            _ => {
                panic!("Unexpected expression found: {:?}", &expr);
            }
        };

        debug!("Resolved expression: {:?} {}: {:?}", expr_key, expr_key.describe(expr_graph, false), resolved_datatype);

        let node: &mut Node<ExprKey, Expr, ExprProp> = expr_graph.get_mut(expr_key);
        if let Some(resolved_expr) = resolved_expr {
            node.value = resolved_expr;
        }
        node.properties.set_data_type(resolved_datatype);
        node.children = resolved_children;

        Ok(())
    }

    pub fn resolve_cast(from_expr: &Expr, to_datatype: &DataType) -> Result<Datum, String> {
        if let Literal(from_value) = from_expr {
            let to_value = match (from_value, to_datatype) {
                (Utf8(s), DataType::Int64) => {
                    let to_value: i64 = s.parse::<i64>().map_err(stringify)?;
                    Int64(to_value)
                }
                (Utf8(s), DataType::Date32) => {
                    let date: chrono::NaiveDate = s.parse::<chrono::NaiveDate>().map_err(stringify)?;
                    let date: i32 = chrono::Datelike::num_days_from_ce(&date) - arrow2::temporal_conversions::EPOCH_DAYS_FROM_CE;
                    Date32(date)
                }
                _ => todo!(),
            };
            return Ok(to_value);
        } else {
            panic!("resolve_cast(): Can only cast to literals")
        }
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
