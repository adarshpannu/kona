// QST: Query Semantics

use log::Log;
use std::rc::Rc;
use crate::includes::*;
use crate::ast::QGM;
use crate::ast::{Expr, Expr::*};

impl QGM {
    pub fn normalize(&mut self) {
        info!("Normalize QGM");

        // Extract preds under AND
        let qblock = &self.qblock;
        let mut pred_list = vec![];
        if qblock.pred_list.len() > 0 {
            let expr = &qblock.pred_list[0];
            QGM::extract(expr, &mut pred_list)
        }

        for expr in pred_list {
            info!("Extracted: {:?}", expr)
        }
    }

    pub fn extract(exprlink: &ExprLink, pred_list: &mut Vec<ExprLink>) {
        let expr = &*exprlink.borrow();
        if let LogExpr(lhs, crate::ast::LogOp::And, rhs) = expr {
            QGM::extract(lhs, pred_list);
            if let Some(rhs) = rhs {
                QGM::extract(rhs, pred_list);
            }
        } else {
            pred_list.push(Rc::clone(exprlink))
        }
    }

    fn check(exprlink: &ExprLink) {
        
    }
}
