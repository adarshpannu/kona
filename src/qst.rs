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
        self.qblock.resolve(env)?;

        // Extract preds under AND
        let qblock = &self.qblock;
        let mut pred_list = vec![];
        if qblock.pred_list.is_some() {
            let expr_id = qblock.pred_list.unwrap();

            Expr::resolve(env, &mut self.graph, qblock, expr_id)?;

            QGM::extract(self, expr_id, &mut pred_list)
        }

        for exprid in pred_list {
            let expr = self.graph.get_node(exprid);
            info!("Extracted: {:?}", expr)
        }
        Ok(())
    }

    pub fn extract(&mut self, pred_id: NodeId, pred_list: &mut Vec<NodeId>) {
        let (expr, children) = self.graph.get_node_with_children(pred_id);
        if let LogExpr(crate::ast::LogOp::And) = expr {
            let children = children.unwrap();
            let lhs = children[0];
            let rhs = children[1];
            self.extract(lhs, pred_list);
            self.extract(rhs, pred_list);
        } else {
            pred_list.push(pred_id)
        }
    }
}

fn dquote(s: &String) -> String {
    format!("\"{}\"", s)
}

impl QueryBlock {
    pub fn resolve(&self, env: &Env) -> Result<(), String> {
        // Resolve QUNs first (base tables and any nested subqueries)
        for qun in self.quns.iter() {
            let tablename = qun.name.as_ref().unwrap();  // this will fail for subqueries, and that's ok for now
            let tbdesc = env.metadata.get_tabledesc(tablename);
            if tbdesc.is_none() {
                return Err(format!("Table {} not cataloged.", dquote(tablename)));
            }
        }
        Ok(())
    }

    pub fn resolve_coltype(&self, env: &Env, colname: &String) -> Result<(&String, DataType), String> {
        // Find column in a single table.
        let mut retval = None;
        for qun in self.quns.iter() {
            let tablename = qun.name.as_ref();
            let curval = tablename
                .and_then(|tn| env.metadata.get_tabledesc(tn))
                .and_then(|desc| desc.coltype(colname))
                .and_then(|ct| Some((tablename.unwrap(), ct)));

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
            Column { tablename, colname } => {
                if let Some(tablename) = tablename {
                    if let Some(tabledesc) = env.metadata.get_tabledesc(tablename) {
                        let datatype = tabledesc.coltype(colname);
                        if datatype.is_none() {
                            return Err(format!("Column {}.{} not found", tablename, colname));
                        } else {
                            node.inner = CID {
                                qun_ix: 111,
                                col_ix: 222,
                            };
                            datatype.unwrap()
                        }
                    } else {
                        return Err(format!("Table {} not found", tablename));
                    }
                } else {
                    // Find colname in exactly one table.
                    let (tablename, datatype) = qblock.resolve_coltype(env, colname)?;
                    info!("Found {} in table {}", colname, tablename);
                    datatype
                }
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
