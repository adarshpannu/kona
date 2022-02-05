use crate::includes::*;
use crate::ast::*;
use crate::row::*;
use crate::graph::*;

enum Instruction {
    GetColumn(usize),
    RelExpr(RelOp),
}

struct Compiler;

impl Compiler {
    pub fn compile(graph: &Graph<Expr>, top: NodeId) {
        let expr = &graph.get_node(top).inner;
        /*
        match expr {

        }
        */
    }
}

struct CCode {

}

/***************************************************************************************************/
impl Expr {
    
    pub fn eval<'a>(&'a self, row: &'a Row) -> Datum {
        match self {
            /*
            CID(cid) => row.get_column(*cid).clone(),
            Literal(lit) => lit.clone(),
            BinaryExpr(b1, op, b2) => {
                let b1 = b1.borrow().eval(row);
                let b2 = b2.borrow().eval(row);
                let res = match (b1, op, b2) {
                    (Datum::INT(i1), ArithOp::Add, Datum::INT(i2)) => i1 + i2,
                    (Datum::INT(i1), ArithOp::Sub, Datum::INT(i2)) => i1 - i2,
                    (Datum::INT(i1), ArithOp::Mul, Datum::INT(i2)) => i1 * i2,
                    (Datum::INT(i1), ArithOp::Div, Datum::INT(i2)) => i1 / i2,
                    _ => panic!("Internal error: Operands of ArithOp not resolved yet."),
                };
                Datum::INT(res)
            }
            RelExpr(b1, op, b2) => {
                let b1 = b1.borrow().eval(row);
                let b2 = b2.borrow().eval(row);
                let res = match (b1, op, b2) {
                    (Datum::INT(i1), RelOp::Eq, Datum::INT(i2)) => i1 == i2,
                    (Datum::INT(i1), RelOp::Ne, Datum::INT(i2)) => i1 != i2,
                    (Datum::INT(i1), RelOp::Le, Datum::INT(i2)) => i1 <= i2,
                    (Datum::INT(i1), RelOp::Lt, Datum::INT(i2)) => i1 < i2,
                    (Datum::INT(i1), RelOp::Ge, Datum::INT(i2)) => i1 >= i2,
                    (Datum::INT(i1), RelOp::Gt, Datum::INT(i2)) => i1 > i2,
                    _ => panic!("Internal error: Operands of RelOp not resolved yet."),
                };
                Datum::BOOL(res)
            }
            */
            _ => unimplemented!(),
        }
    }
}
