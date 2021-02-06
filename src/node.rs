#![allow(warnings)]

use crate::expr;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use super::expr::{Expr::*, *};

/***************************************************************************************************/
trait Node {
    //fn children(&self) -> Option<Vec<Rc<RefCell<Node>>>>;
    fn select(self, cols: Vec<usize>) -> SelectNode<Self>
    where
        Self: Sized,
    {
        SelectNode::new(self, cols)
    }

    fn filter(self, expr: Expr) -> FilterNode<Self>
    where
        Self: Sized,
    {
        FilterNode::new(self, expr)
    }
}

/***************************************************************************************************/
#[derive(Debug)]
struct CSVScanNode<'a> {
    filename: &'a str,
    iter: io::Lines<io::BufReader<File>>,
}

impl<'a> Node for CSVScanNode<'a> {}

impl<'a> CSVScanNode<'a> {
    fn new(filename: &str) -> CSVScanNode {
        let iter = read_lines(&filename).unwrap();

        CSVScanNode { filename, iter }
    }
}

impl<'a> fmt::Display for CSVScanNode<'a> {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CSVScanNode({})", self.filename)
    }
}

use fmt::Display;
use io::BufReader;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

/***************************************************************************************************/
#[derive(Debug)]
struct SelectNode<T> {
    cols: Vec<usize>,
    child: T,
}

impl<T> SelectNode<T> {
    fn new(child: T, cols: Vec<usize>) -> SelectNode<T> {
        SelectNode { child, cols }
    }
}

impl<T> Node for SelectNode<T> {}

impl<T> fmt::Display for SelectNode<T>
where
    T: Display,
{
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SelectNode({:?}, {})", &self.cols, &self.child)
    }
}

/***************************************************************************************************/
#[derive(Debug)]
struct FilterNode<T> {
    child: T,
    expr: Expr,
}

impl<T> FilterNode<T> {
    fn new(child: T, expr: Expr) -> FilterNode<T> {
        if let Expr::RelExpr(..) = expr {
            FilterNode { child, expr }
        } else {
            panic!("Invalid filter expression")
        }
    }
}

impl<T> Node for FilterNode<T> {}

impl<T> fmt::Display for FilterNode<T>
where
    T: Display,
{
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FilterNode({}, {})", &self.expr, &self.child)
    }
}

/***************************************************************************************************/
#[test]
fn test() {
    let expr = RelExpr(
        Box::new(CID(1)),
        RelOp::Gt,
        Box::new(IntegerLiteral(20)),
    );

    let filename = "/Users/adarshrp/Projects/flare/src/data/emp.csv";
    let node = CSVScanNode::new(filename)
        .select(vec![0, 1])
        .filter(expr)
        .select(vec![0, 1]);

    println!("{}", node);
}
