use crate::expr::{Expr::*, *};
use crate::pop::*;
use crate::graph::*;
use crate::includes::*;
use crate::qgm::*;
use crate::lop::*;
use crate::row::*;
use crate::pop::*;

/***************************************************************************************************/
impl Expr {
    pub fn eval<'a>(graph: &ExprGraph, expr_key: ExprKey, row: &'a Row) -> Datum {
        let (expr, _, children) = &graph.get3(expr_key);
        match expr {
            CID(qunid, colid) => row.get_column(*colid).clone(),
            Column {
                prefix,
                colname,
                qunid,
                colid,
            } => row.get_column(*colid).clone(),
            Literal(lit) => lit.clone(),
            RelExpr(op) => {
                let children = children.unwrap();
                let c0 = Expr::eval(graph, children[0], row);
                let c1 = Expr::eval(graph, children[1], row);
                let res = match (c0, op, c1) {
                    (Datum::INT(i1), RelOp::Eq, Datum::INT(i2)) => i1 == i2,
                    (Datum::INT(i1), RelOp::Ne, Datum::INT(i2)) => i1 != i2,
                    (Datum::INT(i1), RelOp::Le, Datum::INT(i2)) => i1 <= i2,
                    (Datum::INT(i1), RelOp::Lt, Datum::INT(i2)) => i1 < i2,
                    (Datum::INT(i1), RelOp::Ge, Datum::INT(i2)) => i1 >= i2,
                    (Datum::INT(i1), RelOp::Gt, Datum::INT(i2)) => i1 > i2,
                    (Datum::STR(s1), RelOp::Eq, Datum::STR(s2)) => *s1 == *s2,
                    (Datum::STR(s1), RelOp::Ne, Datum::STR(s2)) => *s1 != *s2,
                    _ => panic!("Internal error: Operands of RelOp not resolved yet."),
                };
                Datum::BOOL(res)
            }
            BinaryExpr(op) => {
                let children = children.unwrap();
                let c0 = Expr::eval(graph, children[0], row);
                let c1 = Expr::eval(graph, children[1], row);
                let res = match (c0, op, c1) {
                    (Datum::INT(i1), ArithOp::Add, Datum::INT(i2)) => i1 + i2,
                    (Datum::INT(i1), ArithOp::Sub, Datum::INT(i2)) => i1 - i2,
                    (Datum::INT(i1), ArithOp::Mul, Datum::INT(i2)) => i1 * i2,
                    (Datum::INT(i1), ArithOp::Div, Datum::INT(i2)) => i1 / i2,
                    _ => panic!("Internal error: Operands of ArithOp not resolved yet."),
                };
                Datum::INT(res)
            }
            LogExpr(op) => {
                let children = children.unwrap();
                let c0 = Expr::eval(graph, children[0], row);
                let c1 = if children.len() == 2 {
                    Expr::eval(graph, children[1], row)
                } else {
                    Datum::NULL
                };
                let res = match (c0, op, c1) {
                    (Datum::BOOL(b0), LogOp::And, Datum::BOOL(b1)) => b0 && b1,
                    (Datum::BOOL(b0), LogOp::Or, Datum::BOOL(b1)) => b0 || b1,
                    (Datum::BOOL(b0), LogOp::Not, _) => !b0,
                    _ => panic!("Internal error: Operands of LogExpr not resolved yet."),
                };
                Datum::BOOL(res)
            }
            _ => {
                debug!("Expr::eval: {:?} not implemented.", &expr);
                unimplemented!()
            }
        }
    }
}
