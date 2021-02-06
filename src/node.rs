#![allow(warnings)]

use std::cell::RefCell;
use std::rc::Rc;
use crate::expr;

use super::expr::*;

/***************************************************************************************************/
trait Node {
    //fn children(&self) -> Option<Vec<Rc<RefCell<Node>>>>;
    fn select(self, cols: Vec<usize>) -> SelectNode<Self>
    where
        Self: Sized,
    {
        SelectNode::new(self, cols)
    }
}

/***************************************************************************************************/
struct CSVScanNode<'a> {
    filename: &'a str,
}

impl<'a> Node for CSVScanNode<'a> {}

impl<'a> CSVScanNode<'a> {
    fn new(filename: &str) -> CSVScanNode {
        CSVScanNode { filename }
    }
}

/***************************************************************************************************/

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

/***************************************************************************************************/

struct FilterNode<T> {
    child: T,
    expr: Expr
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


/***************************************************************************************************/
#[test]
fn test() {
    let node = CSVScanNode::new("c:/").select(vec![0, 1]);
}
