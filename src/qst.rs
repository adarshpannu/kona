// QST: Query Semantics

use crate::ast::QGM;
use crate::ast::{Expr, Expr::*, QueryBlock};
use crate::graph::{Graph, NodeId};
use crate::row::{DataType, Datum};

use crate::includes::*;
use log::Log;
use std::rc::Rc;

impl QGM {
    pub fn resolve(&mut self, env: &Env) -> Result<(), String> {
        info!("Normalize QGM");

        // Resolve top-level QB
        self.qblock.resolve(env, &mut self.graph)?;

        Ok(())
    }
}

fn dquote(s: &String) -> String {
    format!("\"{}\"", s)
}

impl QueryBlock {
    pub fn resolve(&mut self, env: &Env, graph: &mut Graph<Expr>) -> Result<(), String> {
        // Resolve QUNs first (base tables and any nested subqueries)
        for qun in self.quns.iter_mut() {
            let tablename = qun.name.as_ref().unwrap(); // this will fail for subqueries, and that's ok for now
            let tbdesc = env.metadata.get_tabledesc(tablename);
            if tbdesc.is_none() {
                return Err(format!("Table {} not cataloged.", dquote(tablename)));
            }
            qun.tabledesc = tbdesc;
        }

        // Resolve predicates
        let mut pred_list = vec![];
        if self.pred_list.is_some() {
            let expr_id = self.pred_list.unwrap();

            Expr::resolve(env, graph, self, expr_id)?;

            self.extract(graph, expr_id, &mut pred_list)
        }

        for exprid in pred_list {
            let expr = graph.get_node(exprid);
            info!("Extracted: {:?}", expr)
        }

        Ok(())
    }

    pub fn resolve_column(
        &self, env: &Env, prefix: Option<&String>, colname: &String,
    ) -> Result<(QunId, ColId, DataType), String> {
        let mut retval = None;

        for qun in self.quns.iter() {
            let desc = qun.tabledesc.as_ref().unwrap().clone();
            let mut curval = None;

            if let Some(prefix) = prefix {
                // Prefixed column: look at specific qun
                if qun.matches_name_or_alias(prefix) {
                    let coldesc = desc.get_coldesc(colname);
                    if let Some(coldesc) = coldesc {
                        curval = Some((qun.id, coldesc.0, coldesc.1))
                    }
                }
            } else {
                // Unprefixed column: look at all QUNs
                let coldesc = desc.get_coldesc(colname);
                if let Some(coldesc) = coldesc {
                    curval = Some((qun.id, coldesc.0, coldesc.1))
                }
            }
            if curval.is_some() {
                if retval.is_none() {
                    retval = curval;
                } else {
                    return Err(format!(
                        "Column {} found in multiple tables. Use tablename prefix to disambiguate.",
                        dquote(colname)
                    ));
                }
            }
        }
        retval.ok_or_else(|| {
            let colstr = if let Some(prefix) = prefix {
                format!("{}.{}", prefix, colname)
            } else {
                format!("{}", colname)
            };
            format!("Column {} not found in any table.", colstr)
        })
    }

    pub fn resolve_column_old(&self, env: &Env, colname: &String) -> Result<(QunId, ColId, DataType), String> {
        // Find column in a single table.
        let mut retval = None;
        for qun in self.quns.iter() {
            let tablename = qun.name.as_ref();
            let curval = tablename
                .and_then(|tn| env.metadata.get_tabledesc(tn))
                .and_then(|desc| desc.get_coldesc(colname))
                .and_then(|ct| Some((qun.id, ct.0, ct.1)));

            if curval.is_some() {
                if retval.is_none() {
                    retval = curval;
                } else {
                    return Err(format!(
                        "Column {} found in multiple tables. Use tablename prefix to disambiguate.",
                        dquote(colname)
                    ));
                }
            }
        }
        retval.ok_or(format!("Column {} not found in any table.", dquote(colname)))
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

impl Expr {
    pub fn resolve(
        env: &Env, graph: &mut Graph<Expr>, qblock: &QueryBlock, expr_id: NodeId,
    ) -> Result<DataType, String> {
        let children = graph.get_children(expr_id);
        let mut children_datatypes = vec![];

        if let Some(children) = children {
            for child_id in children {
                let datatype = Expr::resolve(env, graph, qblock, child_id)?;
                children_datatypes.push(datatype);
            }
        }

        let mut node = graph.get_node_mut(expr_id);
        let expr = &node.inner;
        info!("Check: {:?}", expr);

        let datatype = match expr {
            RelExpr(relop) => {
                // Check argument types
                if children_datatypes[0] != children_datatypes[1] {
                    return Err("Datatype mismatch".to_string());
                } else {
                    DataType::BOOL
                }
            }
            Column { prefix, colname } => {
                /*
                if let Some(prefix) = prefix {
                    if let Some(tabledesc) = env.metadata.get_tabledesc(prefix) {
                        let datatype = tabledesc.get_coldesc(colname);
                        if datatype.is_none() {
                            return Err(format!("Column {}.{} not found", prefix, colname));
                        } else {
                            node.inner = CID {
                                qun_ix: 111,
                                col_ix: 222,
                            };
                            datatype.unwrap().1
                        }
                    } else {
                        return Err(format!("Table {} not found", prefix));
                    }
                } else {
                    // Find colname in exactly one table.
                    let (qunid, colid, datatype) = qblock.resolve_column(env, colname)?;
                    info!("Found {} in table {}", colname, qunid);
                    datatype
                }
                */
                let coldesc = qblock.resolve_column(env, prefix.as_ref(), colname)?;
                coldesc.2
            }
            LogExpr(logop) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            Literal(Datum::INT(_)) => DataType::INT,
            Literal(Datum::DOUBLE(_, _)) => DataType::DOUBLE,
            Literal(Datum::BOOL(_)) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            _ => DataType::UNKNOWN,
        };
        node.datatype = datatype;
        Ok(datatype)
    }
}
