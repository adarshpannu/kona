#![allow(dead_code)]

use crate::expr::*;
use crate::row::*;
use fmt::Display;
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::rc::Rc;

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
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
    iter: io::Lines<io::BufReader<File>>,
}

impl<'a> Node for CSVScanNode<'a> {
    fn next(&mut self) -> Option<Row> {
        if let Some(line) = self.iter.next() {
            let line = line.unwrap();
            let cols = line
                .split(',')
                .enumerate()
                .map(|(ix, col)| match self.coltypes[ix] {
                    DataType::INT => {
                        let ival = col.parse::<isize>().unwrap();
                        Datum::INT(ival)
                    }
                    DataType::STR => Datum::STR(Rc::new(col.to_owned())),
                    _ => unimplemented!(),
                })
                .collect::<Vec<Datum>>();
            Some(Row::from(cols))
        } else {
            None
        }
    }
}

impl<'a> CSVScanNode<'a> {
    fn new(filename: &str) -> CSVScanNode {
        let mut iter = read_lines(&filename).unwrap();
        let (colnames, coltypes) = Self::infer_metadata(&filename);

        // Consume the header row
        iter.next();

        CSVScanNode {
            filename,
            colnames,
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

    fn infer_metadata(filename: &str) -> (Vec<String>, Vec<DataType>) {
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
        //dbg!(&colnames);
        //dbg!(&coltypes);
        (colnames, coltypes)
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
    colids: Vec<usize>,
    child: T,
}

impl<T> SelectNode<T> {
    fn new(child: T, cols: Vec<usize>) -> SelectNode<T> {
        println!("New cols {:?}", cols);

        SelectNode { child, colids: cols }
    }
}

impl<T> Node for SelectNode<T>
where
    T: Node,
{
    fn next(&mut self) -> Option<Row> {
        if let Some(row) = self.child.next() {
            return Some(row.select(&self.colids))
        } else {
            return None;
        }
    }
}


impl<T> fmt::Display for SelectNode<T>
where
    T: Display,
{
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SelectNode({:?}, {})", &self.colids, &self.child)
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

impl<T> Node for FilterNode<T>
where
    T: Node,
{
    fn next(&mut self) -> Option<Row> {
        while let Some(e) = self.child.next() {
            if let Datum::BOOL(b) = self.expr.eval(&e) {
                if b {
                    return Some(e);
                }
            }
        }
        return None;
    }
}

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
            Box::new(Literal(Datum::INT(15))),
        );

        let filename = "/Users/adarshrp/Projects/flare/src/data/emp.csv";
        let mut node = CSVScanNode::new(filename)
            .filter(expr)
            .select(vec![2, 0]);

        println!("{}", node);

        //let mut node = CSVScanNode::new(filename);

        while let Some(row) = node.next() {
            println!("-- {}", row);
        }
    }
}
