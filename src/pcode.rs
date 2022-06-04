// pcode

pub use crate::{expr::*, flow::*, graph::*, includes::*, lop::*, pop::*, qgm::*, row::*};

#[derive(Debug, Serialize, Deserialize)]
pub struct PCode {
    instructions: Vec<PInstruction>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlOp {
    ReturnIfTrue,
    ReturnIfFalse,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PInstruction {
    RegisterId(usize),
    Literal(Datum),
    NegatedExpr,
    BinaryExpr(ArithOp),
    RelExpr(RelOp),
    LogExpr(LogOp),
    ControlOp(ControlOp),
}

impl ExprKey {
    pub fn compile(&self, expr_graph: &ExprGraph, pcode: &mut PCode, register_allocator: &mut RegisterAllocator) {
        let (expr, _, children) = expr_graph.get3(*self);

        // Post-order traversal (i.e. children before parents)
        if let Some(children) = children {
            for &child_expr_key in children {
                child_expr_key.compile(expr_graph, pcode, register_allocator)
            }
        }

        let inst = match expr {
            Expr::CID(colid, ..) => PInstruction::RegisterId(*colid),
            Expr::Literal(value) => PInstruction::Literal(value.clone()),
            Expr::Column { qunid, colid, .. } => {
                let regid = register_allocator.get_id(QunCol(*qunid, *colid));
                PInstruction::RegisterId(regid)
            }
            Expr::BinaryExpr(op) => PInstruction::BinaryExpr(*op),
            Expr::RelExpr(op) => PInstruction::RelExpr(*op),
            Expr::LogExpr(op) => PInstruction::LogExpr(*op),
            Expr::NegatedExpr => PInstruction::NegatedExpr,
            _ => panic!("Expression not compilable yet: {:?}", expr),
        };

        pcode.push(inst);
    }
}

impl PCode {
    pub fn new() -> PCode {
        PCode { instructions: vec![] }
    }
    pub fn push(&mut self, inst: PInstruction) {
        debug!("Instruction: {:?}", inst);
        self.instructions.push(inst)
    }

    pub fn eval(&self, registers: &Row) -> Datum {
        let mut stack = vec![];
        //debug!("--- Eval on row: {}", registers);
        for inst in self.instructions.iter() {
            //debug!("Run inst: {:?}, stack = {:?}", inst, stack);
            match inst {
                PInstruction::RegisterId(id) => stack.push(registers.get_column(*id).clone()),
                PInstruction::Literal(datum) => stack.push(datum.clone()),
                PInstruction::RelExpr(op) => {
                    let (c0, c1) = (stack.pop().unwrap(), stack.pop().unwrap());
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
                    //debug!("Eval returned {}", res);
                    stack.push(Datum::BOOL(res))
                }
                PInstruction::BinaryExpr(op) => {
                    let (c0, c1) = (stack.pop().unwrap(), stack.pop().unwrap());
                    let res = match (c0, op, c1) {
                        (Datum::INT(i1), ArithOp::Add, Datum::INT(i2)) => i1 + i2,
                        (Datum::INT(i1), ArithOp::Sub, Datum::INT(i2)) => i1 - i2,
                        (Datum::INT(i1), ArithOp::Mul, Datum::INT(i2)) => i1 * i2,
                        (Datum::INT(i1), ArithOp::Div, Datum::INT(i2)) => i1 / i2,
                        _ => panic!("Internal error: Operands of ArithOp not resolved yet."),
                    };
                    stack.push(Datum::INT(res))
                }
                PInstruction::LogExpr(op) => {
                    let c0 = stack.pop().unwrap();
                    let c1 = if matches!(op, LogOp::Not) { Datum::NULL } else { stack.pop().unwrap() };
                    let res = match (c0, op, c1) {
                        (Datum::BOOL(b0), LogOp::And, Datum::BOOL(b1)) => b0 && b1,
                        (Datum::BOOL(b0), LogOp::Or, Datum::BOOL(b1)) => b0 || b1,
                        (Datum::BOOL(b0), LogOp::Not, _) => !b0,
                        _ => panic!("Internal error: Operands of LogExpr not resolved yet."),
                    };
                    stack.push(Datum::BOOL(res))
                }
                PInstruction::NegatedExpr => {
                    let c0 = stack.pop().unwrap();
                    match c0 {
                        Datum::INT(i1) => stack.push(Datum::INT(-i1)),
                        _ => panic!("Internal error: Operands of NegatedExpr must be numeric."),
                    }
                }
                _ => {
                    debug!("Instruction inst: {:?} not implemented yet. Possibly invalid?", inst);
                    todo!()
                }
            }
        }
        let retval = stack.pop();
        retval.unwrap()
    }
}
