#![allow(warnings)]

use std::fmt;
use std::ops;

#[derive(Debug)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl fmt::Display for ArithOp {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            ArithOp::Add => '+',
            ArithOp::Sub => '-',
            ArithOp::Mul => '*',
            ArithOp::Div => '/',
        };
        write!(f, "{}", display_str)
    }
}

#[derive(Debug)]
pub enum LogOp {
    And,
    Or,
    Not,
}

impl fmt::Display for LogOp {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            LogOp::And => "&&",
            LogOp::Or => "||",
            LogOp::Not => "!",
        };
        write!(f, "{}", display_str)
    }
}

#[derive(Debug)]
pub enum RelOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl fmt::Display for RelOp {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            RelOp::Eq => "==",
            RelOp::Ne => "!=",
            RelOp::Gt => ">",
            RelOp::Ge => ">=",
            RelOp::Lt => "<",
            RelOp::Le => "<="

        };
        write!(f, "{}", display_str)
    }
}

/***************************************************************************************************/
#[derive(Debug)]
pub enum Expr {
    CID(usize),
    CN(String),
    IntegerLiteral(usize),
    StringLiteral(String),
    ArithExpr(Box<Expr>, ArithOp, Box<Expr>),
    RelExpr(Box<Expr>, RelOp, Box<Expr>),
}

impl fmt::Display for Expr {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CID(cid) => write!(f, "#{}", cid),
            CN(cn) => write!(f, "${}", cn),
            IntegerLiteral(il) => write!(f, "{}", il),
            StringLiteral(sl) => write!(f, "{}", sl),
            ArithExpr(lhs, op, rhs) => write!(f, "({} {} {})", lhs, op, rhs),
            RelExpr(lhs, op, rhs) => write!(f, "({} {} {})", lhs, op, rhs),
        }
        //write!(f, "{}", self.0)
    }
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
    let e: Expr = RelExpr(
        Box::new(CID(0) + CN("abc".to_owned())),
        RelOp::Gt,
        Box::new(IntegerLiteral(30)),
    );
    println!("{}", e)
}
