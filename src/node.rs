#![allow(warnings)]

use crate::expr::{Expr::*, *};
use crate::row::*;
use fmt::Display;
use io::BufReader;
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

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

    fn next(&mut self) -> Option<Row> {
        None
    }
}

/***************************************************************************************************/
#[derive(Debug)]
struct CSVScanNode<'a> {
    filename: &'a str,
    coltypes: Vec<DataType>,
    iter: io::Lines<io::BufReader<File>>,
}

impl<'a> Node for CSVScanNode<'a> {
    fn next(&mut self) -> Option<Row> {
        if let Some(line) = self.iter.next() {
            let line = line.ok().unwrap();
            Some(Row::from_csv_line(&line))
        } else {
            None
        }
    }
}

impl<'a> CSVScanNode<'a> {
    fn new(filename: &str) -> CSVScanNode {
        let iter = read_lines(&filename).unwrap();
        let coltypes = Self::infer_datatypes(&filename);
        CSVScanNode {
            filename,
            coltypes,
            iter,
        }
    }

    fn infer_datatype(str: &String) -> DataType {
        let res = str.parse::<i32>();
        if res.is_ok() {
            DataType::INT
        } else if str.eq("true") || str.eq("false") {
            DataType::BOOL
        } else {
            DataType::STR
        }
    }

    fn infer_datatypes(filename: &str) -> Vec<DataType> {
        let mut iter = read_lines(&filename).unwrap();
        let mut colnames: Vec<String> = vec![];
        let mut coltypes: Vec<DataType> = vec![];
        let mut first_row = true;

        while let Some(line) = iter.next() {
            let cols: Vec<String> = line.unwrap().split(',').map(|e| e.to_owned()).collect();
            if colnames.len() == 0 {
                colnames = cols;
            } else {
                for (ix, col) in cols.iter().enumerate() {
                    let datatype = CSVScanNode::infer_datatype(col);
                    if first_row {
                        coltypes.push(datatype)
                    } else if coltypes[ix] != DataType::STR {
                        coltypes[ix] = datatype;
                    } else {
                        coltypes[ix] = DataType::STR;
                    }
                }
                first_row = false;
            }
        }
        dbg!(&coltypes);
        coltypes
    }
}

impl<'a> fmt::Display for CSVScanNode<'a> {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CSVScanNode({})", self.filename)
    }
}

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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test() {
        let expr = RelExpr(
            Box::new(CID(1)),
            RelOp::Gt,
            Box::new(Literal(Datum::INT(30))),
        );

        let filename = "/Users/adarshrp/Projects/flare/src/data/emp.csv";
        let node = CSVScanNode::new(filename)
            .select(vec![0, 1])
            .filter(expr)
            .select(vec![0, 1]);

        println!("{}", node);

        let mut node = CSVScanNode::new(filename);

        while let Some(row) = node.next() {
            println!("-- {}", row);
        }
    }
}
