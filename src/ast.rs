use crate::includes::*;

use crate::expr::{Expr, Expr::*};
use crate::metadata::TableDesc;
use crate::sqlparser;

use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::rc::Rc;

use std::process::Command;

pub struct ParserState {
    next_id: RefCell<usize>,
    pub subqueries: Vec<Rc<QueryBlock>>
}

impl ParserState {
    pub fn new() -> Self {
        ParserState {
            next_id: RefCell::new(0), subqueries: vec![]
        }
    }

    pub fn add_subquery(&mut self, mut qblock: Rc<QueryBlock>) {
        self.subqueries.push(qblock);
    }

    pub fn next_id(&self) -> usize {
        let mut id = self.next_id.borrow_mut();
        let retval = *id;
        *id = *id + 1;
        retval
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
    QGM(QGM),
}

#[derive(Debug)]
pub struct QGM {
    pub qblock: QueryBlock,
}

#[derive(Debug)]
pub struct NamedExpr {
    pub name: Option<String>,
    pub expr: ExprLink,
}

#[derive(Debug, PartialEq)]
pub enum QueryBlockType {
    Unassigned,
    Main,
    CTE,
    InlineView,
    Subquery,
}

pub type QueryBlock0 = (Vec<NamedExpr>, Vec<Quantifier>, Vec<ExprLink>);

#[derive(Debug)]
pub struct Quantifier {
    id: usize,
    name: Option<String>,
    alias: Option<String>,
    qblock: Option<Rc<QueryBlock>>,
}

impl Quantifier {
    pub fn new(id: usize, name: Option<String>, alias: Option<String>, qblock: Option<Rc<QueryBlock>>) -> Self {
        Quantifier {
            id,
            name,
            alias,
            qblock,
        }
    }

    pub fn display(&self) -> String {
        format!(
            "{}/{}",
            self.name.as_ref().unwrap_or(&"".to_string()),
            self.alias.as_ref().unwrap_or(&"".to_string())
        )
    }

    pub fn name(&self) -> String {
        format!("QUN_{}", self.id)
    }
}

#[derive(Debug)]
pub struct QueryBlock {
    id: usize,
    pub name: Option<String>,
    pub qbtype: QueryBlockType,
    pub select_list: Vec<NamedExpr>,
    pub quns: Vec<Quantifier>,
    pub pred_list: Vec<ExprLink>,
    pub qblocks: Vec<Rc<QueryBlock>>,
}

impl QueryBlock {
    pub fn new(
        id: usize, name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>,
        pred_list: Vec<ExprLink>, qblocks: Vec<Rc<QueryBlock>>,
    ) -> Self {
        QueryBlock {
            id,
            name,
            qbtype,
            select_list,
            quns,
            pred_list,
            qblocks,
        }
    }

    pub fn name(&self) -> String {
        if self.qbtype == QueryBlockType::Main { 
            format!("QB_main")
        } else {
            format!("QB_{}", self.id)
        }
    }
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

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

impl QueryBlock {
    pub(crate) fn write_qblock(&self, file: &mut File) -> std::io::Result<()> {
        for qblock in self.qblocks.iter() {
            qblock.write_qblock(file);
        }

        fprint!(file, "  subgraph cluster_{} {{\n", self.name());
        fprint!(file, "    \"{}_pt\"[shape=point, color=white];\n", self.name());
        fprint!(file, "    label = \"{}\";\n", self.name());

        for qun in self.quns.iter() {
            fprint!(
                file,
                "    \"{}\"[label=\"{}\", color=red]\n",
                qun.name(),
                qun.display()
            );
            if let Some(qblock) = &qun.qblock {
                fprint!(file, "    \"{}\" -> \"{}_pt\";\n", qun.name(), qblock.name());
                qblock.write_qblock(file)?
            }
        }

        if self.pred_list.len() > 0 {
            QGM::write_expr_to_graphvis(&self.pred_list[0], file);
        }
        fprint!(file, "}}\n");
        Ok(())
    }
}

impl QGM {
    pub(crate) fn write_to_graphviz(&self, filename: &str, open_jpg: bool) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        //fprint!(file, "    node [style=filled,color=white];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        //fprint!(file, "    splines=polyline;\n");
        //fprint!(file, "    style=filled;\n");
        //fprint!(file, "    color=lightgrey;\n");
        //fprint!(file, "    node [style=filled,color=white];\n");

        self.qblock.write_qblock(&mut file);
        fprint!(file, "}}\n");

        drop(file);

        let ofilename = format!("{}.jpg", filename);
        let oflag = format!("-o{}.jpg", filename);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(filename)
            .status()
            .expect("failed to execute process");

        if open_jpg {
            let _cmd = Command::new("open")
                .arg(ofilename)
                .status()
                .expect("failed to execute process");
        }
        Ok(())
    }

    fn write_expr_to_graphvis(expr: &ExprLink, file: &mut File) -> std::io::Result<()> {
        let expr = expr.borrow();
        let addr = &*expr as *const Expr;
        fprint!(file, "    exprnode{:?}[label=\"{}\"];\n", addr, expr.name());

        println!("--- {:?} {:?}", addr, &*expr);

        match &*expr {
            RelExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?
            }

            LogExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?
            }

            BinaryExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?
            }

            _ => {}
        }
        Ok(())
    }
}

/*
trait GraphVizNode<T> {
    fn name(&self) -> String;
    fn id(&self) -> usize;
    fn children(&self) -> Vec<Box<T>>;
}
*/
