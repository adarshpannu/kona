// QST: Query Semantics

use crate::ast::QGM;
use crate::ast::{Expr, Expr::*, QueryBlock};
use crate::graph::{Graph, NodeId};
use crate::row::{DataType, Datum};

use crate::includes::*;
use log::Log;
use slotmap::secondary::Entry;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct QunColumn {
    qun_id: QunId,
    col_id: ColId,
    datatype: DataType
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
        println!("Assigned {:?} -> {}", &quncol, *e);
        *e
    }
}

impl QueryBlock {
    pub fn resolve(&mut self, env: &Env, graph: &mut Graph<Expr>) -> Result<(), String> {
        let mut colid_dispenser = QueryBlockColidDispenser::new();

        // Resolve QUNs first (base tables and any nested subqueries)
        for qun in self.quns.iter_mut() {
            let tablename = qun.name.as_ref().unwrap(); // this will fail for subqueries, and that's ok for now
            let tbdesc = env.metadata.get_tabledesc(tablename);
            if tbdesc.is_none() {
                return Err(format!("Table {} not cataloged.", dquote(tablename)));
            }
            qun.tabledesc = tbdesc;
        }

        // Resolve select list
        for ne in self.select_list.iter() {
            let expr_id = ne.expr_id;
            self.resolve_expr(env, graph, &mut colid_dispenser, expr_id)?;
        }

        // Resolve predicates
        let mut pred_list = vec![];
        if self.pred_list.is_some() {
            let expr_id = self.pred_list.unwrap();

            self.resolve_expr(env, graph, &mut colid_dispenser, expr_id)?;

            self.extract(graph, expr_id, &mut pred_list)
        }

        for exprid in pred_list {
            let expr = graph.get_node(exprid);
            //info!("Extracted: {:?}", expr)
        }

        for qun in self.quns.iter() {
            info!("Qun: {}, column_map:{:?}", qun.id, qun.column_map)
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
                    let mut column_map = qun.column_map.borrow_mut();
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
    ) -> Result<DataType, String> {
        let children = graph.get_children(expr_id);
        let mut children_datatypes = vec![];

        if let Some(children) = children {
            for child_id in children {
                let datatype = self.resolve_expr(env, graph, colid_dispenser, child_id)?;
                children_datatypes.push(datatype);
            }
        }

        let mut node = graph.get_node_mut(expr_id);
        let expr = &node.inner;
        //info!("Check: {:?}", expr);

        let datatype = match expr {
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
            Column { prefix, colname } => {
                let (quncol, qtuple_ix) = self.resolve_column(env, colid_dispenser, prefix.as_ref(), colname)?;
                debug!("ASSIGN <== {}", qtuple_ix);
                node.inner = CID(qtuple_ix);
                quncol.datatype
            }
            LogExpr(logop) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            Literal(Datum::INT(_)) => DataType::INT,
            Literal(Datum::DOUBLE(_, _)) => DataType::DOUBLE,
            Literal(Datum::BOOL(_)) => DataType::BOOL,
            Literal(Datum::STR(_)) => DataType::STR,
            _ => todo!(),
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
