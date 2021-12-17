use crate::includes::*;

use crate::expr::Expr;
use crate::metadata::TableDesc;
use crate::sqlparser;

use std::cell::RefCell;
use std::rc::Rc;

type Quantifier = String;

pub struct ParserState {
    qblocks: Vec<QueryBlock>
}

impl ParserState {
    pub fn new() -> Self {
        ParserState { qblocks: vec![] }
    }
}

#[derive(Debug)]
pub enum AST {
    CatalogTable {
        name: String,
        options: Vec<(String, String)>,
    },
    DescribeTable {
        name: String,
    },
    QGM(QGM)
}

#[derive(Debug)]
pub struct QGM {
    pub qblocks: Vec<QueryBlock>
}

#[derive(Debug)]
pub struct NamedExpr {
    pub name: Option<String>,
    pub expr: ExprLink,
}

#[derive(Debug)]
pub struct QueryBlock {
    pub name: Option<String>,
    pub select_list: Vec<NamedExpr>,
    pub quns: Vec<Quantifier>,
    pub pred_list: Vec<ExprLink>,
}

impl QueryBlock {
    pub fn new(
        name: Option<String>, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>, pred_list: Vec<ExprLink>,
    ) -> Self {
        QueryBlock { name, select_list, quns, pred_list }
    }
}

pub enum TableReference {
    Identifier(String),
    QueryBlock(QueryBlock)
}

/*
#[test]
fn sqlparser() {
    assert!(sqlparser::LogExprParser::new().parse("col1 = 10").is_ok());
    assert!(sqlparser::LogExprParser::new().parse("(col1 = 10)").is_ok());

    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 and col2 < 20")
        .is_ok());
    assert!(sqlparser::LogExprParser::new().parse("(col2 < 20)").is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 or (col2 < 20)")
        .is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 and (col2 < 20)")
        .is_ok());

    assert!(sqlparser::LogExprParser::new()
        .parse("col1 >= 10 or col2 <= 20")
        .is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 and col2 < 20 or col3 != 30")
        .is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 = 10 or col2 = 20 and col3 > 30")
        .is_ok());

    //let expr = sqlparser::LogExprParser::new().parse("col1 = 10 and col2 = 20 and (col3 > 30)").unwrap();
    let expr: ExprLink = sqlparser::LogExprParser::new()
        .parse("(col2 > 20) and (col3 > 30) or (col4 < 40)")
        .unwrap();
    //let mut exprvec= vec![];
    //dbg!(normalize(&expr, &mut exprvec));
}

fn normalize(expr: &Box<Expr>, exprvec: &mut Vec<&Box<Expr>>) -> bool {
    if let LogExpr(left, LogOp::And, right) = **expr {
        normalize(&left, exprvec);
        normalize(&right, exprvec);
    } else {
        println!("{:?}", expr);
        exprvec.push(expr);
    }
    false
}
*/
