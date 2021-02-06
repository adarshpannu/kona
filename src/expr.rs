#![allow(warnings)]

use std::ops;

enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

enum LogOp {
    And,
    Or,
    Not,
}

enum RelOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/***************************************************************************************************/
enum Expr {
    CID(usize),
    CN(String),
    IntegerLiteral(usize),
    StringLiteral(String),
    ArithExpr(Box<Expr>, ArithOp, Box<Expr>),
    RelExpr(Box<Expr>, RelOp, Box<Expr>),
}

use Expr::*;

/***************************************************************************************************/
impl ops::Add for Expr {
    type Output = Self;

    fn add(self, other: Self) -> Expr {
        Expr::ArithExpr(Box::new(self), ArithOp::Add, Box::new(other))
    }
}

impl ops::Sub for Expr {
    type Output = Self;

    fn sub(self, other: Self) -> Expr {
        Expr::ArithExpr(Box::new(self), ArithOp::Sub, Box::new(other))
    }
}

impl ops::Mul for Expr {
    type Output = Self;

    fn mul(self, other: Self) -> Expr {
        Expr::ArithExpr(Box::new(self), ArithOp::Mul, Box::new(other))
    }
}

impl ops::Div for Expr {
    type Output = Self;

    fn div(self, other: Self) -> Expr {
        Expr::ArithExpr(Box::new(self), ArithOp::Div, Box::new(other))
    }
}



#[test]
fn test() {
    let e = RelExpr(Box::new(CID(10) + CID(20)), RelOp::Gt, Box::new(IntegerLiteral(30)));
}

