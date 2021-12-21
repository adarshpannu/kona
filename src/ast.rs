use crate::includes::*;

use crate::expr::{Expr, Expr::*};
use crate::metadata::TableDesc;
use crate::sqlparser;

use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::rc::Rc;

use std::process::Command;

type Quantifier = String;

pub struct ParserState {
    pub qblocks: Vec<QueryBlock>,
}

impl ParserState {
    pub fn new() -> Self {
        ParserState { qblocks: vec![] }
    }

    pub fn add_qblock(&mut self, mut qblock: QueryBlock) -> String {
        if qblock.name.is_none() {
            qblock.name = Some(format!("$QB-{}", self.qblocks.len()).to_string());
        }
        let retval = qblock.name.clone().unwrap(); // hack
        self.qblocks.push(qblock);
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
    pub qblocks: Vec<QueryBlock>,
}

#[derive(Debug)]
pub struct NamedExpr {
    pub name: Option<String>,
    pub expr: ExprLink,
}

#[derive(Debug)]
pub enum QueryBlockType {
    Unassigned,
    Main,
    CTE,
    InlineView,
    Subquery
}

pub type QueryBlock0 = (Vec<NamedExpr>, Vec<Quantifier>, Vec<ExprLink>);

#[derive(Debug)]
pub struct QueryBlock {
    pub name: Option<String>,
    pub qbtype: QueryBlockType,
    pub select_list: Vec<NamedExpr>,
    pub quns: Vec<Quantifier>,
    pub pred_list: Vec<ExprLink>,
}

impl QueryBlock {
    pub fn new(
        name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>, pred_list: Vec<ExprLink>,
    ) -> Self {
        QueryBlock {
            name,
            qbtype,
            select_list,
            quns,
            pred_list,
        }
    }
}

pub enum TableReference {
    Identifier(String),
    QueryBlock(QueryBlock),
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

impl QGM {
    pub(crate) fn write_to_graphviz(&self, filename: &str, open_jpg: bool) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [style=filled,color=white];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    splines=polyline;\n");
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    style=filled;\n");
        fprint!(file, "    color=lightgrey;\n");
        //fprint!(file, "    node [style=filled,color=white];\n");

        for (ix, qblock) in self.qblocks.iter().enumerate() {
            fprint!(file, "  subgraph cluster_{} {{\n", ix);
            if let Some(ref name) = qblock.name {
                fprint!(file, "    \"{}\"[shape=point, color=black];\n", ix);
                fprint!(file, "    label = \"{}\";\n", name);
            }

            for qun in qblock.quns.iter() {
                fprint!(file, "    \"{}\"[color=red]\n", qun);
            }

            if qblock.pred_list.len() > 0 {
                QGM::write_expr_to_graphvis(&qblock.pred_list[0], &mut file);
            }
            fprint!(file, "  }}\n");
        }
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
