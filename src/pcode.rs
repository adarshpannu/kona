// pcode

pub use crate::{expr::*, flow::*, graph::*, includes::*, lop::*, pop::*, qgm::*, row::*};
use arrow2::scalar::{new_scalar, PrimitiveScalar, Scalar};

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
    Column(ColumnPosition),
    Literal(Datum),
    NegatedExpr,
    BinaryExpr(ArithOp),
    RelExpr(RelOp),
    LogExpr(LogOp),
    ControlOp(ControlOp),
}

impl ExprKey {
    pub fn compile(&self, expr_graph: &ExprGraph, pcode: &mut PCode, cpt: &ColumnPositionTable) {
        let (expr, _, children) = expr_graph.get3(*self);

        // Post-order traversal (i.e. children before parents)
        if let Some(children) = children {
            for &child_expr_key in children {
                child_expr_key.compile(expr_graph, pcode, cpt)
            }
        }

        let inst = match expr {
            Expr::CID(colid, ..) => PInstruction::Column(ColumnPosition { column_position: *colid }),
            Expr::Literal(value) => PInstruction::Literal(value.clone()),
            Expr::Column { qunid, colid, .. } => {
                let cpos = cpt.get(QunCol(*qunid, *colid));
                PInstruction::Column(cpos)
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

#[derive(Debug)]
enum Column<'a> {
    Ref(&'a Box<dyn Array>),
    Owned(Box<dyn Array>),
}

impl<'a> Column<'a> {
    fn get(&self) -> &Box<dyn Array> {
        match self {
            Column::Ref(r) => *r,
            Column::Owned(o) => &o,
        }
    }
}

#[derive(Debug)]
enum PCodeStack<'a> {
    Datum(Datum),
    Column(Column<'a>),
}

impl PCode {
    pub fn new() -> PCode {
        PCode { instructions: vec![] }
    }
    pub fn push(&mut self, inst: PInstruction) {
        debug!("Instruction: {:?}", inst);
        self.instructions.push(inst)
    }

    pub fn eval(&self, input: &ChunkBox) -> Box<dyn Array> {
        let mut stack: Vec<PCodeStack> = vec![];
        for inst in self.instructions.iter() {
            match inst {
                PInstruction::Column(id) => stack.push(PCodeStack::Column(Column::Ref(&input[id.column_position]))),
                PInstruction::Literal(datum) => stack.push(PCodeStack::Datum(datum.clone())),
                PInstruction::BinaryExpr(op) => {
                    let (rhs, lhs) = (stack.pop().unwrap(), stack.pop().unwrap());
                    match (lhs, op, rhs) {
                        (PCodeStack::Column(lhs), arithop, PCodeStack::Column(rhs)) => {
                            let lhs = lhs.get().as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                            let rhs = rhs.get().as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                            let array: Box<dyn Array> = match arithop {
                                ArithOp::Add => Box::new(arithmetics::basic::add(lhs, rhs)),
                                _ => todo!(),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        (PCodeStack::Column(lhs), arithop, PCodeStack::Datum(Datum::INT(i))) => {
                            let lhs = lhs.get().as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                            let rhs = &(i as i64);
                            let array: Box<dyn Array> = match arithop {
                                ArithOp::Add => Box::new(arithmetics::basic::add_scalar(lhs, rhs)),
                                ArithOp::Sub => Box::new(arithmetics::basic::sub_scalar(lhs, rhs)),
                                ArithOp::Mul => Box::new(arithmetics::basic::mul_scalar(lhs, rhs)),
                                ArithOp::Div => Box::new(arithmetics::basic::div_scalar(lhs, rhs)),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        _ => todo!(),
                    }
                }
                PInstruction::RelExpr(op) => {
                    let (rhs, lhs) = (stack.pop().unwrap(), stack.pop().unwrap());
                    match (lhs, op, rhs) {
                        (PCodeStack::Column(lhs), relop, PCodeStack::Column(rhs)) => {
                            let lhs = &**lhs.get();
                            let rhs = &**rhs.get();
                            let array: Box<dyn Array> = match relop {
                                RelOp::Lt => Box::new(comparison::lt(lhs, rhs)),
                                RelOp::Le => Box::new(comparison::lt_eq(lhs, rhs)),
                                RelOp::Eq => Box::new(comparison::eq(lhs, rhs)),
                                RelOp::Ne => Box::new(comparison::neq(lhs, rhs)),
                                RelOp::Ge => Box::new(comparison::gt_eq(lhs, rhs)),
                                RelOp::Gt => Box::new(comparison::gt(lhs, rhs)),
                                _ => todo!(),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        (PCodeStack::Column(lhs), relop, PCodeStack::Datum(Datum::INT(i))) => {
                            let lhs = &**lhs.get();
                            let scalar = PrimitiveScalar::new(DataType::Int64, Some(i as i64));
                            let rhs = &scalar;
                            let array: Box<dyn Array> = match relop {
                                RelOp::Lt => Box::new(comparison::lt_scalar(lhs, rhs)),
                                RelOp::Le => Box::new(comparison::lt_eq_scalar(lhs, rhs)),
                                RelOp::Eq => Box::new(comparison::eq_scalar(lhs, rhs)),
                                RelOp::Ne => Box::new(comparison::neq_scalar(lhs, rhs)),
                                RelOp::Ge => Box::new(comparison::gt_eq_scalar(lhs, rhs)),
                                RelOp::Gt => Box::new(comparison::gt_scalar(lhs, rhs)),
                                _ => todo!(),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        _ => todo!(),
                    }
                }
                PInstruction::LogExpr(op) => {
                    let (rhs, lhs) = (stack.pop().unwrap(), stack.pop().unwrap());
                    match (lhs, op, rhs) {
                        (PCodeStack::Column(lhs), relop, PCodeStack::Column(rhs)) => {
                            let lhs = lhs.get().as_any().downcast_ref::<BooleanArray>().unwrap();
                            let rhs = rhs.get().as_any().downcast_ref::<BooleanArray>().unwrap();
                            let array: Box<dyn Array> = match relop {
                                LogOp::And => Box::new(boolean::and(lhs, rhs)),
                                LogOp::Or => Box::new(boolean::or(lhs, rhs)),
                                _ => todo!(),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        _ => todo!(),
                    }
                }
                PInstruction::NegatedExpr => {
                    let lhs = stack.pop().unwrap();
                    match lhs {
                        PCodeStack::Column(lhs) => {
                            let lhs = &**lhs.get();
                            let array = arithmetics::neg(lhs);
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        _ => todo!(),
                    }
                }
                _ => {
                    debug!("Instruction inst: {:?} not implemented yet. Possibly invalid?", inst);
                    todo!()
                }
            }
        }
        let array = stack.pop().unwrap();
        match array {
            PCodeStack::Column(Column::Owned(array)) => array,
            PCodeStack::Column(Column::Ref(array)) => array.clone(),
            _ => panic!("unexpected value"),
        }
    }
}
