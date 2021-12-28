use crate::includes::*;
use crate::sqlparser;
use Expr::*;
use crate::row::{Datum, Row};

use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::rc::Rc;

use core::panic;
use std::fmt;
use std::ops;
use std::process::Command;

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
    pub cte_list: Vec<QueryBlockLink>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamedExpr {
    pub name: Option<String>,
    pub expr: ExprLink,
}

impl NamedExpr {
    pub fn new(name: Option<String>, expr: ExprLink) -> Self {
        let mut name = name;
        if name.is_none() {
            if let Expr::Column { tablename, colname } = &*expr.borrow() {
                name = Some(colname.clone())
            } else if let Expr::Star = &*expr.borrow() {
                name = Some("*".to_string())
            }
        }
        NamedExpr { name, expr }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum QueryBlockType {
    Unassigned,
    Main,
    CTE,
    InlineView,
    Subquery,
}

pub type QueryBlock0 = (Vec<NamedExpr>, Vec<Quantifier>, Vec<ExprLink>);

#[derive(Debug, Serialize, Deserialize)]
pub struct Quantifier {
    id: usize,
    name: Option<String>,
    alias: Option<String>,
    qblock: Option<QueryBlockLink>,
}

impl Quantifier {
    pub fn new(id: usize, name: Option<String>, alias: Option<String>, qblock: Option<QueryBlockLink>) -> Self {
        Quantifier {
            id,
            name,
            alias,
            qblock,
        }
    }

    pub fn display(&self) -> String {
        format!(
            "{}/{} {}",
            self.name.as_ref().unwrap_or(&"".to_string()),
            self.alias.as_ref().unwrap_or(&"".to_string()),
            if self.qblock.is_some() { "subq" } else { "" }
        )
    }

    pub fn name(&self) -> String {
        format!("QUN_{}", self.id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DistinctProperty {
    All,
    Distinct
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryBlock {
    id: usize,
    pub name: Option<String>,
    pub qbtype: QueryBlockType,
    select_list: Vec<NamedExpr>,
    quns: Vec<Quantifier>,
    pub pred_list: Vec<ExprLink>,
    group_by: Option<Vec<ExprLink>>,
    having_clause: Option<ExprLink>,
    order_by: Option<Vec<(ExprLink, Ordering)>>,
    distinct: DistinctProperty,
    topN: Option<usize>
}

impl QueryBlock {
    pub fn new(
        id: usize, name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>,
        pred_list: Vec<ExprLink>, group_by: Option<Vec<ExprLink>>, having_clause: Option<ExprLink>,
        order_by: Option<Vec<(ExprLink, Ordering)>>,
        distinct: DistinctProperty,
        topN: Option<usize>
    ) -> Self {
        QueryBlock {
            id,
            name,
            qbtype,
            select_list,
            quns,
            pred_list,
            group_by,
            having_clause,
            order_by,
            distinct,
            topN
        }
    }

    pub fn name(&self) -> String {
        if self.name.is_some() {
            format!("{}", self.name.as_ref().unwrap())
        } else if self.qbtype == QueryBlockType::Main {
            format!("QB_main")
        } else {
            format!("QB_{}", self.id)
        }
    }

    pub fn set_name(&mut self, name: Option<String>) {
        self.name = name
    }
}

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

impl QueryBlock {
    pub(crate) fn write_to_graphviz(&self, file: &mut File) -> std::io::Result<()> {
        // Write current query block first
        let s = "".to_string();
        let select_names: Vec<&String> = self.select_list.iter().map(|e| e.name.as_ref().unwrap_or(&s)).collect();
        let select_names = format!("{:?}", select_names).replace("\"", "");

        fprint!(file, "  subgraph cluster_{} {{\n", self.name());
        fprint!(file, "    label = \"{} {}\";\n", self.name(), select_names);
        fprint!(file, "    \"{}_pt\"[shape=point, color=white];\n", self.name());

        /*
        for nexpr in self.select_list.iter() {
            let expr = &nexpr.expr;
            let childaddr = &*expr.borrow() as *const Expr;
            fprint!(file, "    exprnode{:?} -> \"{}_pt\";\n", childaddr, self.name());
            QGM::write_expr_to_graphvis(expr, file)?;
        }
        */

        for qun in self.quns.iter().rev() {
            fprint!(file, "    \"{}\"[label=\"{}\", color=red]\n", qun.name(), qun.display());
            if let Some(qblock) = &qun.qblock {
                let qblock = &*qblock.borrow();
                fprint!(file, "    \"{}\" -> \"{}_pt\";\n", qun.name(), qblock.name());
                qblock.write_to_graphviz(file)?
            }
        }

        if self.pred_list.len() > 0 {
            QGM::write_expr_to_graphvis(&self.pred_list[0], file);
        }

        fprint!(file, "}}\n");

        Ok(())
    }
}

pub struct ParserState {
    next_id: usize,
}

impl ParserState {
    pub fn new() -> Self {
        ParserState {
            next_id: 0,
        }
    }

    pub fn next_id(&mut self) -> usize {
        let retval = self.next_id;
        self.next_id += 1;
        retval
    }
}

impl QGM {
    pub(crate) fn write_to_graphviz(&self, filename: &str, open_jpg: bool) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        //fprint!(file, "    node [style=filled,color=white];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        //fprint!(file, "    splines=polyline;\n");
        //fprint!(file, "    style=filled;\n");
        //fprint!(file, "    color=lightgrey;\n");
        //fprint!(file, "    node [style=filled,color=white];\n");

        self.qblock.write_to_graphviz(&mut file);

        // Write subqueries (CTEs)
        for qblock in self.cte_list.iter() {
            let qblock = &*qblock.borrow();
            qblock.write_to_graphviz(&mut file);
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

        //println!("--- {:?} {:?}", addr, &*expr);

        match &*expr {
            RelExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?;
            }

            BetweenExpr(e, lhs, rhs) => {
                let childaddr = &*e.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&e, file)?;
                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?;
            }

            LogExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                if let Some(rhs) = rhs {
                    let childaddr = &*rhs.borrow() as *const Expr;
                    fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                }
                Self::write_expr_to_graphvis(&lhs, file)?;
                if let Some(rhs) = rhs {
                    Self::write_expr_to_graphvis(&rhs, file)?;
                }
            }

            BinaryExpr(lhs, op, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?;
            }

            NegatedExpr(expr) => {
                let childaddr = &*expr.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                Self::write_expr_to_graphvis(&expr, file)?;
            }

            ScalarFunction(name, args) => {
                for arg in args.iter() {
                    let childaddr = &*arg.borrow() as *const Expr;
                    fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                    Self::write_expr_to_graphvis(&arg, file)?;
                }
            }

            AggFunction(aggtype, arg) => {
                let childaddr = &*arg.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                Self::write_expr_to_graphvis(&arg, file)?;
            }

            Subquery(subq) => {
                let subq = &*subq.borrow();
                fprint!(file, "    exprnode{:?} -> \"{}_pt\";\n", addr, subq.name());
                subq.write_to_graphviz(file);
            }

            InSubqExpr(lhs, rhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                let childaddr = &*rhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                Self::write_expr_to_graphvis(&lhs, file)?;
                Self::write_expr_to_graphvis(&rhs, file)?;
            }

            InListExpr(lhs, args) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);

                for arg in args.iter() {
                    let childaddr = &*arg.borrow() as *const Expr;
                    fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                    Self::write_expr_to_graphvis(&arg, file)?;
                }
                Self::write_expr_to_graphvis(&lhs, file)?;
            }

            ExistsExpr(lhs) => {
                let childaddr = &*lhs.borrow() as *const Expr;
                fprint!(file, "    exprnode{:?} -> exprnode{:?};\n", childaddr, addr);
                Self::write_expr_to_graphvis(&lhs, file)?;
            }

            _ => {}
        }
        Ok(())
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl fmt::Display for ArithOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            ArithOp::Add => '+',
            ArithOp::Sub => '-',
            ArithOp::Mul => '*',
            ArithOp::Div => '/',
        };
        write!(f, "{}", display_str)
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum LogOp {
    And,
    Or,
    Not,
}

impl fmt::Display for LogOp {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            LogOp::And => "&&",
            LogOp::Or => "||",
            LogOp::Not => "!",
        };
        write!(f, "{}", display_str)
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum RelOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Like,
}

impl fmt::Display for RelOp {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            RelOp::Eq => "=",
            RelOp::Ne => "!=",
            RelOp::Gt => ">",
            RelOp::Ge => ">=",
            RelOp::Lt => "<",
            RelOp::Le => "<=",
            RelOp::Like => "LIKE",
        };
        write!(f, "{}", display_str)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AggType {
    COUNT,
    COUNT_DISTINCT,
    MIN,
    MAX,
    SUM,
    AVG,
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum Expr {
    CID(usize),
    Column { tablename: Option<String>, colname: String },
    Star,
    Literal(Datum),
    NegatedExpr(ExprLink),
    BinaryExpr(ExprLink, ArithOp, ExprLink),
    RelExpr(ExprLink, RelOp, ExprLink),
    BetweenExpr(ExprLink, ExprLink, ExprLink),
    InListExpr(ExprLink, Vec<ExprLink>),
    InSubqExpr(ExprLink, ExprLink),
    ExistsExpr(ExprLink),
    LogExpr(ExprLink, LogOp, Option<ExprLink>),
    Subquery(QueryBlockLink),
    AggFunction(AggType, ExprLink),
    ScalarFunction(String, Vec<ExprLink>)
}

impl Expr {
    pub fn newlink(expr: Expr) -> ExprLink {
        Rc::new(RefCell::new(expr))
    }

    pub fn name(&self) -> String {
        match self {
            CID(cid) => format!("CID: {}", cid),
            Column { tablename, colname } => {
                if let Some(tablename) = tablename {
                    format!("{}.{}", tablename, colname)
                } else {
                    format!("{}", colname)
                }
            }
            Star => format!("*"),
            Literal(v) => format!("{}", v).replace(r#"""#, r#"\""#),
            BinaryExpr(lhs, op, rhs) => format!("{:?}", op),
            NegatedExpr(lhs) => "-".to_string(),
            RelExpr(lhs, op, rhs) => format!("{}", op),
            BetweenExpr(e, l, r) => format!("BETWEEEN"),
            InListExpr(_, _) => format!("IN"),
            InSubqExpr(_, _) => format!("IN_SUBQ"),
            ExistsExpr(_) => format!("EXISTS"),
            LogExpr(lhs, op, rhs) => format!("{:?}", op),
            Subquery(qblock) => format!("(subquery)"),
            AggFunction (aggtype, arg ) => format!("{:?}", aggtype),
            ScalarFunction(name, args ) => format!("{}()", name),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CID(cid) => write!(f, "${}", cid),
            Column { tablename, colname } => {
                write!(f, "{:?}.{}", tablename, colname)
            }
            Star => write!(f, "*"),
            Literal(v) => write!(f, "{}", v),
            BinaryExpr(lhs, op, rhs) => {
                write!(f, "({} {} {})", lhs.borrow(), op, rhs.borrow())
            }
            NegatedExpr(lhs) => write!(f, "-({})", lhs.borrow()),
            ExistsExpr(lhs) => unimplemented!(),
            RelExpr(lhs, op, rhs) => {
                write!(f, "({} {} {})", lhs.borrow(), op, rhs.borrow())
            }
            BetweenExpr(e, lhs, rhs) => {
                write!(f, "({} {} {})", e.borrow(), lhs.borrow(), rhs.borrow())
            }
            InListExpr(_, _) => unimplemented!(),
            InSubqExpr(_, _) => unimplemented!(),
            LogExpr(lhs, op, rhs) => {
                if let Some(rhs) = rhs {
                    write!(f, "({} {} {})", lhs.borrow(), op, rhs.borrow())
                } else {
                    write!(f, "({} {})", lhs.borrow(), op)
                }
            }
            Subquery(qblock) => {
                write!(f, "(subq)")
            }
            AggFunction (aggtype, arg) => write!(f, "{:?} {}", aggtype, arg.borrow()),
            ScalarFunction ( name, args) => write!(f, "{}({:?})", name, args),
        }
    }
}

/***************************************************************************************************/
impl Expr {
    pub fn eval<'a>(&'a self, row: &'a Row) -> Datum {
        match self {
            CID(cid) => row.get_column(*cid).clone(),
            Literal(lit) => lit.clone(),
            BinaryExpr(b1, op, b2) => {
                let b1 = b1.borrow().eval(row);
                let b2 = b2.borrow().eval(row);
                let res = match (b1, op, b2) {
                    (Datum::INT(i1), ArithOp::Add, Datum::INT(i2)) => i1 + i2,
                    (Datum::INT(i1), ArithOp::Sub, Datum::INT(i2)) => i1 - i2,
                    (Datum::INT(i1), ArithOp::Mul, Datum::INT(i2)) => i1 * i2,
                    (Datum::INT(i1), ArithOp::Div, Datum::INT(i2)) => i1 / i2,
                    _ => panic!("Internal error: Operands of ArithOp not resolved yet."),
                };
                Datum::INT(res)
            }
            RelExpr(b1, op, b2) => {
                let b1 = b1.borrow().eval(row);
                let b2 = b2.borrow().eval(row);
                let res = match (b1, op, b2) {
                    (Datum::INT(i1), RelOp::Eq, Datum::INT(i2)) => i1 == i2,
                    (Datum::INT(i1), RelOp::Ne, Datum::INT(i2)) => i1 != i2,
                    (Datum::INT(i1), RelOp::Le, Datum::INT(i2)) => i1 <= i2,
                    (Datum::INT(i1), RelOp::Lt, Datum::INT(i2)) => i1 < i2,
                    (Datum::INT(i1), RelOp::Ge, Datum::INT(i2)) => i1 >= i2,
                    (Datum::INT(i1), RelOp::Gt, Datum::INT(i2)) => i1 > i2,
                    _ => panic!("Internal error: Operands of RelOp not resolved yet."),
                };
                Datum::BOOL(res)
            }
            _ => unimplemented!(),
        }
    }
}
