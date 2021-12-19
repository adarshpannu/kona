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

    pub fn add_qblock(&mut self, qblock: QueryBlock) {
        self.qblocks.push(qblock);
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
pub struct QueryBlock {
    pub name: Option<String>,
    pub select_list: Vec<NamedExpr>,
    pub quns: Vec<Quantifier>,
    pub pred_list: Vec<ExprLink>,
}

impl QueryBlock {
    pub fn new(
        name: Option<String>, select_list: Vec<NamedExpr>,
        quns: Vec<Quantifier>, pred_list: Vec<ExprLink>,
    ) -> Self {
        QueryBlock {
            name,
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

impl QGM {
    pub(crate) fn write_to_graphviz(
        &self, filename: &str, open_jpg: bool,
    ) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        file.write_all("digraph example1 {\n".as_bytes())?;
        file.write_all("    node [style=filled,color=white];\n".as_bytes())?;
        file.write_all("    rankdir=BT;\n".as_bytes())?; // direction of DAG
        file.write_all("    splines=polyline;\n".as_bytes())?;
        file.write_all("    nodesep=0.5;\n".as_bytes())?;
        file.write_all("    style=filled;\n".as_bytes())?;
        file.write_all("    color=lightgrey;\n".as_bytes())?;
        //file.write_all("    node [style=filled,color=white];\n".as_bytes())?;

        for (ix, qblock) in self.qblocks.iter().enumerate() {
            let nodestr = format!("  subgraph cluster_{} {{\n", ix);
            file.write_all(nodestr.as_bytes())?;

            file.write_all(format!("    label =  \"qblock{}\";\n", ix).as_bytes());

            if qblock.pred_list.len() > 0 {
                QGM::write_expr_to_graphvis(&qblock.pred_list[0], &mut file);
            }
            /*
            for child in node.children().iter() {
                let edge = format!("    Node{} -> Node{};\n", child, node.id());
                file.write_all(edge.as_bytes())?;
            }
            */
            file.write_all("  }\n".as_bytes())?;
        }
        file.write_all("}\n".as_bytes())?;
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

    fn write_expr_to_graphvis(
        expr: &ExprLink, file: &mut File,
    ) -> std::io::Result<()> {
        let expr = expr.borrow();
        let addr = &*expr as *const Expr;
        let nodestr = format!("    exprnode{:?}[label=\"{}\"];\n", addr, expr.name());
        file.write_all(nodestr.as_bytes())?;

        println!("--- {:?} {:?}", addr, &*expr);

        match &*expr {
            CID(cid) => file.write_all(nodestr.as_bytes())?,
            RelExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                let childstr = format!(
                    "    exprnode{:?} -> exprnode{:?};\n",
                    childaddr, addr
                );
                file.write_all(childstr.as_bytes())?;

                let childaddr = &*rhs.borrow() as *const Expr;
                let childstr = format!(
                    "    exprnode{:?} -> exprnode{:?};\n",
                    childaddr, addr
                );

                file.write_all(childstr.as_bytes())?;
                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?
            }

            LogExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                let childstr = format!(
                    "    exprnode{:?} -> exprnode{:?};\n",
                    childaddr, addr
                );
                file.write_all(childstr.as_bytes())?;

                let childaddr = &*rhs.borrow() as *const Expr;
                let childstr = format!(
                    "    exprnode{:?} -> exprnode{:?};\n",
                    childaddr, addr
                );

                file.write_all(childstr.as_bytes())?;
                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?
            }
            _ => {},
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
