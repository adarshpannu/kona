// pcode

#![allow(clippy::borrowed_box)]

use std::fmt;

use arrow2::{scalar::{PrimitiveScalar, Scalar, Utf8Scalar}, compute::cast::{CastOptions, self}};

use crate::{
    datum::Datum,
    expr::{ArithOp, Expr, ExprGraph, LogOp, RelOp},
    graph::ExprKey,
    includes::*,
    pop::{Projection, ProjectionMap},
};

#[derive(Debug, Default, Serialize, Deserialize)]
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
    Column(ColId),
    Literal(Datum),
    NegatedExpr,
    BinaryExpr(ArithOp),
    RelExpr(RelOp),
    LogExpr(LogOp),
    ControlOp(ControlOp),
    Cast(DataType),
}

impl ExprKey {
    #[tracing::instrument(fields(expr = self.to_string()), skip_all, parent = None)]
    pub fn compile(&self, expr_graph: &ExprGraph, pcode: &mut PCode, proj_map: &mut ProjectionMap) {
        debug!("Compile expression: {}", self.describe(expr_graph, false));

        let prj = Projection::VirtCol(*self);
        let colid = proj_map.get(prj);
        let inst = if let Some(colid) = colid {
            PInstruction::Column(colid)
        } else {
            let (expr, props, children) = expr_graph.get3(*self);

            // Post-order traversal (i.e. children before parents except when compiling aggs)
            if !matches!(expr, Expr::AggFunction(..)) {
                if let Some(children) = children {
                    for &child_expr_key in children {
                        child_expr_key.compile(expr_graph, pcode, proj_map)
                    }
                }
            }

            match expr {
                Expr::CID(_, colid) => PInstruction::Column(*colid),
                Expr::Literal(value) => PInstruction::Literal(value.clone()),
                Expr::Column { qunid, colid, .. } => {
                    let prj = Projection::QunCol(QunCol(*qunid, *colid));
                    let colid = proj_map.get(prj).unwrap();
                    PInstruction::Column(colid)
                }
                Expr::BinaryExpr(op) => PInstruction::BinaryExpr(*op),
                Expr::RelExpr(op) => PInstruction::RelExpr(*op),
                Expr::LogExpr(op) => PInstruction::LogExpr(*op),
                Expr::NegatedExpr => PInstruction::NegatedExpr,
                Expr::AggFunction(agg_type, _) => {
                    let child_expr = expr_graph.get_value(children.unwrap()[0]);
                    if let Expr::CID(_, colid) = child_expr {
                        let array_id = proj_map.set_agg(*agg_type, *colid, props.data_type().clone());
                        PInstruction::Column(array_id)
                    } else {
                        panic!("Malformed agg expression. Maybe no GROUP BY clause specified?")
                    }
                }
                Expr::Cast => PInstruction::Cast(props.data_type.clone()),
                _ => panic!("Expression not compilable yet: {:?}", expr),
            }
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
            Column::Ref(r) => r,
            Column::Owned(o) => o,
        }
    }
}

enum PCodeStack<'a> {
    Datum(Datum),
    Column(Column<'a>),
}

impl<'a> fmt::Debug for PCodeStack<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            PCodeStack::Column(col) => f!("{:?}", col.get().data_type()),
            PCodeStack::Datum(datum) => f!("{:?}", datum),
        };
        write!(fmt, "{}", display_str)
    }
}

impl PCode {
    pub fn push(&mut self, inst: PInstruction) {
        debug!("Instruction: {:?}", inst);
        self.instructions.push(inst)
    }

    pub fn eval(&self, input: &ChunkBox) -> Box<dyn Array> {

        debug!("eval: {:?}", self);
        
        let mut stack: Vec<PCodeStack> = vec![];
        for inst in self.instructions.iter() {
            match inst {
                PInstruction::Column(id) => stack.push(PCodeStack::Column(Column::Ref(&input[*id]))),
                PInstruction::Literal(datum) => stack.push(PCodeStack::Datum(datum.clone())),
                PInstruction::BinaryExpr(op) => {
                    let (rhs, lhs) = (stack.pop().unwrap(), stack.pop().unwrap());

                    match (lhs, op, rhs) {
                        (PCodeStack::Column(lhs), arithop, PCodeStack::Column(rhs)) => {
                            let lhs = &**lhs.get();
                            let rhs = &**rhs.get();
                            let array: Box<dyn Array> = match arithop {
                                ArithOp::Add => arithmetics::add(lhs, rhs),
                                ArithOp::Sub => arithmetics::sub(lhs, rhs),
                                ArithOp::Mul => arithmetics::mul(lhs, rhs),
                                ArithOp::Div => arithmetics::div(lhs, rhs),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        (PCodeStack::Column(lhs), arithop, PCodeStack::Datum(Datum::Int64(i))) => {
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
                        (PCodeStack::Column(lhs), arithop, PCodeStack::Datum(Datum::Float64(fvalue))) => {
                            let lhs = lhs.get().as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                            let rhs = &f64::from(fvalue);
                            let array: Box<dyn Array> = match arithop {
                                ArithOp::Add => Box::new(arithmetics::basic::add_scalar(lhs, rhs)),
                                ArithOp::Sub => Box::new(arithmetics::basic::sub_scalar(lhs, rhs)),
                                ArithOp::Mul => Box::new(arithmetics::basic::mul_scalar(lhs, rhs)),
                                ArithOp::Div => Box::new(arithmetics::basic::div_scalar(lhs, rhs)),
                            };
                            stack.push(PCodeStack::Column(Column::Owned(array)));
                        }
                        (lhs, op, rhs) => {
                            todo!("Not yet implemented: {:?} {:?} {:?}", lhs, op, rhs)
                        }
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
                        (PCodeStack::Column(lhs), relop, PCodeStack::Datum(d)) => {
                            let scalar_i64; // = PrimitiveScalar::new(DataType::Int64, Some(0 as i64));
                            let scalar_utf8; // = PrimitiveScalar::new(DataType::Int64, Some(0 as i64));
                            let scalar_i32;

                            let lhs = &**lhs.get();
                            let rhs: &dyn Scalar = match d {
                                Datum::Int64(i) => {
                                    scalar_i64 = PrimitiveScalar::new(DataType::Int64, Some(i as i64));
                                    &scalar_i64
                                }
                                Datum::Utf8(s) => {
                                    let s = &*s.clone();
                                    scalar_utf8 = Utf8Scalar::<i32>::new(Some(s));
                                    &scalar_utf8
                                }
                                Datum::Date32(d) => {
                                    scalar_i32 = PrimitiveScalar::new(DataType::Date32, Some(d as i32));
                                    &scalar_i32
                                }
                                _ => todo!(),
                            };
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
                        (lhs, op, rhs) => {
                            todo!("Not yet implemented: {:?} {:?} {:?}", lhs, op, rhs)
                        }
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
                PInstruction::Cast(to_datatype) => {
                    let lhs = stack.pop().unwrap();
                    match lhs {
                        PCodeStack::Column(lhs) => {
                            let lhs = &**lhs.get();
                            let cast_options = CastOptions::default();
                            let array = cast::cast(lhs, to_datatype, cast_options).unwrap();
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
