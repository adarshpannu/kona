#![allow(warnings)]

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::process::Command;

pub use crate::{expr::*, flow::*, graph::*, includes::*, lop::*, pop::*, qgm::*, row::*};

#[derive(Debug, Serialize, Deserialize)]
pub struct PCode {
    instructions: Vec<PInstruction>,
}

impl PCode {
    pub fn new() -> PCode {
        PCode { instructions: vec![] }
    }
    pub fn push(&mut self, inst: PInstruction) {
        debug!("Instruction: {:?}", inst);
        self.instructions.push(inst)
    }
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
    Subquery(QueryBlockKey),
    AggFunction(AggType, bool),
    ScalarFunction(String),
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
            Expr::Literal(value) => PInstruction::Literal(value.clone()),
            Expr::Column { qunid, colid, .. } => {
                let regid = register_allocator.get_id(QunCol(*qunid, *colid));
                PInstruction::RegisterId(regid)
            }
            Expr::BinaryExpr(op) => PInstruction::BinaryExpr(*op),
            Expr::RelExpr(op) => PInstruction::RelExpr(*op),
            Expr::NegatedExpr => PInstruction::NegatedExpr,
            _ => panic!(format!("Expression not compilable yet: {:?}", expr)),
        };

        pcode.push(inst);
    }
}
