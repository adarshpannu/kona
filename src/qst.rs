// QST: Query Semantics

use crate::ast::QGM;
use crate::ast::{Expr, Expr::*, QueryBlock};
use crate::graph::{Graph, NodeId};
use crate::row::{DataType, Datum};

use crate::includes::*;
use log::Log;
use std::rc::Rc;

impl QGM {
    pub fn normalize(&mut self, env: &Env) {
        info!("Normalize QGM");

        // Extract preds under AND
        let qblock = &self.qblock;
        let mut pred_list = vec![];
        if qblock.pred_list.is_some() {
            let expr_id = qblock.pred_list.unwrap();

            Expr::typecheck(env, &mut self.graph, qblock, expr_id);

            QGM::extract(self, expr_id, &mut pred_list)
        }

        for exprid in pred_list {
            let expr = self.graph.get_node(exprid);
            info!("Extracted: {:?}", expr)
        }
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

impl Expr {
    pub fn typecheck(env: &Env, graph: &mut Graph<Expr>, qblock: &QueryBlock, expr_id: NodeId) -> Result<DataType, String> {
        let children = graph.get_children(expr_id);
        let mut children_datatypes = vec![];

        if let Some(children) = children {
            for child_id in children {
                let datatype = Expr::typecheck(env, graph, qblock, child_id);
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
                    return Err("Datatype mismatch".to_string())
                } else {
                    DataType::BOOL
                }
             },
            Column { tablename, colname } => {
                if let Some(tablename) = tablename {
                    if let Some(tabledesc) = env.metadata.get_tabledesc(tablename) {
                        let datatype = tabledesc.coltype(colname);
                        if datatype.is_none() {
                            return Err(format!("Column {}.{} not found", tablename, colname))
                        } else {
                            datatype.unwrap()
                        }
                    } else {
                        return Err(format!("Table {} not found", tablename))
                    }
                } else {
                    // Find colname in exactly one table.
                    let (tablename, datatype) = env.metadata.coltype(colname)?;
                    println!("Found {} in table {}", colname, tablename);
                    datatype
                }
            }
            LogExpr(logop) => { DataType::BOOL },
            Literal(Datum::STR(_)) => DataType::STR,
            Literal(Datum::INT(_)) => DataType::INT,
            Literal(Datum::DOUBLE(_, _)) => DataType::DOUBLE,
            Literal(Datum::BOOL(_)) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            _ => DataType::UNKNOWN
        };
        node.datatype = datatype;
        Ok(datatype)
    }
}
