use crate::ast::*;
use crate::flow::*;
use crate::graph::*;
use crate::includes::*;
use crate::row::*;

pub struct Compiler;

impl Compiler {
    pub fn compile(env: &Env, qgm: &QGM) -> Result<Flow, String> {
        let arena: NodeArena = Arena::new();
        let graph = &qgm.graph;
        let topqblock = &qgm.qblock;

        assert!(topqblock.quns.len() == 1);
        for qun in topqblock.quns.iter() {
            assert!(qun.name.is_some() && qun.qblock.is_none());

            let colmap = qun.column_map.borrow().clone();

            if let Some(name) = &qun.name {
                let csvnode = CSVNode::new(env, &arena, name.clone(), 4, colmap);
                let emit = csvnode.emit(&arena);
            }
        }

        let flow = Flow {
            id: 99,
            nodes: arena.into_vec(),
            //graph: graph
        };
        Ok(flow)
    }
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
