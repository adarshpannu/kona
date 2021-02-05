#![allow(warnings)]

use std::cell::RefCell;
use std::rc::Rc;

/***************************************************************************************************/
trait Node {
    //fn children(&self) -> Option<Vec<Rc<RefCell<Node>>>>;
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
    child: T,
}

impl<T> SelectNode<T>
where
    T: Node,
{
    fn new(child: T) -> SelectNode<T> {
        SelectNode {child}
    }
}

/***************************************************************************************************/
#[test]
fn test() {
    let scan_node = CSVScanNode::new("c:/");
    let sel_node = SelectNode::new(scan_node);
}
