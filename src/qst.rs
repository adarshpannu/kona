// QST: Query Semantics

use crate::ast::QGM;
use crate::ast::{QueryBlock, Expr, Expr::*};
use crate::includes::*;
use log::Log;
use std::rc::Rc;

impl QGM {
    pub fn normalize(&mut self) {
        info!("Normalize QGM");

        // Extract preds under AND
        let qblock = &self.qblock;
        let mut pred_list = vec![];
        if qblock.pred_list.is_some() {
            let expr = qblock.pred_list.as_ref().unwrap();
            QGM::extract(expr, &mut pred_list)
        }

        for expr in pred_list {
            info!("Extracted: {:?}", expr)
        }
    }

    pub fn extract(NodeId: &NodeId, pred_list: &mut Vec<NodeId>) {
        let expr = &*NodeId.borrow();
        if let LogExpr(lhs, crate::ast::LogOp::And, rhs) = expr {
            QGM::extract(lhs, pred_list);
            if let Some(rhs) = rhs {
                QGM::extract(rhs, pred_list);
            }
        } else {
            pred_list.push(Rc::clone(NodeId))
        }
    }

    fn check(NodeId: &NodeId) {
        let mut expr = &mut *NodeId.borrow_mut();
        expr.check();
    }
}

impl Expr {
    pub fn children(&self) -> Vec<NodeId> {
        let retval = vec![];
        let v = match &self {
            RelExpr(lhs, op, rhs) => vec![lhs.clone(), rhs.clone()],

            BetweenExpr(e, lhs, rhs) => vec![lhs.clone(), rhs.clone()],

            LogExpr(lhs, op, rhs) => {
                if rhs.is_some() {
                    vec![lhs.clone(), rhs.as_ref().unwrap().clone()]
                } else {
                    vec![lhs.clone()]
                }
            }

            BinaryExpr(lhs, op, rhs) => vec![lhs.clone(), rhs.clone()],

            NegatedExpr(expr) => vec![expr.clone()],

            ScalarFunction(name, args) => vec![],

            AggFunction(aggtype, arg) => vec![arg.clone()],

            Subquery(subq) => unimplemented!(),

            InSubqExpr(lhs, rhs) => vec![lhs.clone(), rhs.clone()],

            InListExpr(lhs, args) => unimplemented!(),

            ExistsExpr(lhs) => unimplemented!(),

            _ => vec![]
        };
        retval
    }

    pub fn check(&mut self) -> Vec<NodeId> {
        let retval = vec![];
        let v = match &self {
            RelExpr(lhs, op, rhs) => vec![lhs.clone(), rhs.clone()],

            BetweenExpr(e, lhs, rhs) => vec![lhs.clone(), rhs.clone()],

            LogExpr(lhs, op, rhs) => {
                if rhs.is_some() {
                    vec![lhs.clone(), rhs.as_ref().unwrap().clone()]
                } else {
                    vec![lhs.clone()]
                }
            }

            BinaryExpr(lhs, op, rhs) => vec![lhs.clone(), rhs.clone()],

            NegatedExpr(expr) => vec![expr.clone()],

            ScalarFunction(name, args) => vec![],

            AggFunction(aggtype, arg) => vec![arg.clone()],

            Subquery(subq) => unimplemented!(),

            InSubqExpr(lhs, rhs) => vec![lhs.clone(), rhs.clone()],

            InListExpr(lhs, args) => unimplemented!(),

            ExistsExpr(lhs) => unimplemented!(),

            _ => vec![]
        };
        retval
    }

}

impl QueryBlock {
    pub fn check(&mut self) {

    }
}
