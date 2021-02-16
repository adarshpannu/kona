#![allow(dead_code, unused_imports, unused_variables, warnings)]

// OLD CODE - OBSOLETE

use crate::expr::{Expr::*, *};
use crate::row::*;
use fmt::Display;
use std::collections::hash_map::{Iter, Values};
use std::io::{self, BufRead};
use std::path::Path;
use std::rc::Rc;
use std::{collections::HashMap, fmt};
use std::{fs::File, hash::Hash};

/***************************************************************************************************/
trait Node {
    //fn children(&self) -> Option<Vec<Rc<RefCell<Node>>>>;
    fn project(self, cols: Vec<usize>) -> ProjectNode<Self>
    where
        Self: Sized,
    {
        ProjectNode::new(self, cols)
    }

    fn filter(self, expr: Expr) -> FilterNode<Self>
    where
        Self: Sized,
    {
        FilterNode::new(self, expr)
    }

    fn agg<'b>(
        self, keycolids: Vec<usize>, aggcolids: Vec<(AggType, usize)>, htable: &'b mut HashMap<Row, Row>,
    ) -> AggNode<'b, Self>
    where
        Self: Sized,
    {
        AggNode::new(self, keycolids, aggcolids, htable)
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
struct ProjectNode<T> {
    colids: Vec<usize>,
    child: T,
}

impl<T> ProjectNode<T> {
    fn new(child: T, cols: Vec<usize>) -> ProjectNode<T> {
        println!("New cols {:?}", cols);

        ProjectNode {
            child,
            colids: cols,
        }
    }
}

impl<T> Node for ProjectNode<T>
where
    T: Node,
{
    fn next(&mut self) -> Option<Row> {
        if let Some(row) = self.child.next() {
            return Some(row.project(&self.colids));
        } else {
            return None;
        }
    }
}

impl<T> fmt::Display for ProjectNode<T>
where
    T: Display,
{
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProjectNode({:?}, {})", &self.colids, &self.child)
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
#[derive(Debug, Clone, Copy)]
enum AggType {
    COUNT,
    MIN,
    MAX,
    //AVG,
    //SUM,
}

#[derive(Debug)]
struct AggNode<'a, T> {
    keycolids: Vec<usize>,
    aggcolids: Vec<(AggType, usize)>,
    child: T,
    run_agg: bool,
    htable: &'a mut HashMap<Row, Row>,
    agg_iter: Option<Iter<'a, Row, Row>>,
}

impl<'a, T> AggNode<'a, T>
where
    T: Node,
{
    fn new<'b>(
        child: T, keycols: Vec<usize>, aggcols: Vec<(AggType, usize)>, htable: &'b mut HashMap<Row, Row>,
    ) -> AggNode<'b, T> {
        AggNode {
            child,
            keycolids: keycols,
            aggcolids: aggcols,
            run_agg: false,
            htable: &mut htable,
            agg_iter: None,
        }
    }

    fn run_one_agg(&mut self, acc: &mut Row, currow: &Row) {
        for ix in 0..acc.len() {
            match self.aggcolids[ix].0 {
                AggType::COUNT => {
                    let mut col = acc.get_column_mut(ix);
                    *col = Datum::INT(99);
                }
                _ => {}
            }
        }
    }

    fn run_agg(&mut self) {
        while let Some(mut currow) = self.child.next() {
            // build key
            let key = currow.project(&self.keycolids);
            println!("-- key = {}", key);

            let acc = self.htable.entry(key).or_insert_with(|| {
                let acc_cols: Vec<Datum> = self
                    .aggcolids
                    .iter()
                    .map(|&(aggtype, ix)| {
                        // Build an empty accumumator Row
                        match aggtype {
                            AggType::COUNT => Datum::INT(0),
                            _ => currow.get_column(ix).clone(),
                        }
                    })
                    .collect();
                Row::from(acc_cols)
            });
            println!("-- acc = {}", acc);
            AggNode::run_one_agg(self, acc, &currow)
        }
        self.agg_iter = Some(self.htable.iter()); // DOES NOT COMPILE!! Ugh!
    }
}

impl<'a, T> Node for AggNode<'a, T>
where
    T: Node,
{
    fn next(&mut self) -> Option<Row> {
        if !self.run_agg {
            self.run_agg();
            self.run_agg = true;
        }
        /*
        if let Some(row) = self.child.next() {
            return Some(row.project(&self.keycolids));
        } else {
            return None;
        }
        */
        None
    }
}

impl<'a, T> fmt::Display for AggNode<'a, T>
where
    T: Display,
{
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggNode({:?}, {})", &self.keycolids, &self.child)
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
        let mut htable: HashMap<Row, Row> = HashMap::new();

        let mut node = CSVScanNode::new(filename)
            .filter(expr)
            .project(vec![2, 1, 0])
            .agg(vec![0], vec![(AggType::COUNT, 1)], &mut htable);

        println!("{}", node);

        //let mut node = CSVScanNode::new(filename);

        while let Some(row) = node.next() {
            println!("-- {}", row);
        }
    }
}
