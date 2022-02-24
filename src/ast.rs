use crate::graph::{Graph, NodeId};
use crate::includes::*;
use crate::metadata::TableDesc;
use crate::row::{Datum, Row};
use crate::sqlparser;
use Expr::*;
use std::collections::HashMap;

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
    SetOption { name: String, value: String }
}

#[derive(Debug)]
pub struct QGM {
    pub qblock: QueryBlock,
    pub cte_list: Vec<QueryBlockLink>,
    pub graph: Graph<Expr>,  // arena allocator
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamedExpr {
    pub alias: Option<String>,
    pub expr_id: NodeId,
}

impl NamedExpr {
    pub fn new(alias: Option<String>, expr_id: NodeId, graph: &Graph<Expr>) -> Self {
        let expr = &graph.get_node(expr_id).inner;
        let mut alias = alias;
        if alias.is_none() {
            if let Expr::Column {
                prefix: tablename,
                colname,
            } = expr
            {
                alias = Some(colname.clone())
            } else if let Expr::Star = expr {
                alias = Some("*".to_string())
            }
        }

        NamedExpr { alias, expr_id }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum QueryBlockType {
    Unassigned,
    Main,
    CTE,
    InlineView,
    Subquery,
    GroupBy
}

pub type QueryBlock0 = (Vec<NamedExpr>, Vec<Quantifier>, Vec<NodeId>);

#[derive(Serialize, Deserialize)]
pub struct Quantifier {
    pub id: usize,
    pub name: Option<String>,
    pub qblock: Option<QueryBlockLink>,
    pub alias: Option<String>,

    #[serde(skip)]
    pub tabledesc: Option<Rc<dyn TableDesc>>,

    #[serde(skip)]
    pub column_read_map: RefCell<HashMap<ColId, usize>>
}

impl fmt::Debug for Quantifier {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Quantifier")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("alias", &self.alias)
            .finish()
    }
}

impl Quantifier {
    pub fn new(id: usize, name: Option<String>, qblock: Option<QueryBlockLink>, alias: Option<String>) -> Self {
        // Either we have a named base table, or we have a queryblock (but not both)
        assert!((name.is_some() && qblock.is_none()) || (name.is_none() && qblock.is_some()));

        Quantifier {
            id,
            name,
            qblock,
            alias,
            tabledesc: None,
            column_read_map: RefCell::new(HashMap::new())
        }
    }

    pub fn is_base_table(&self) -> bool {
        self.qblock.is_none()
    }

    pub fn matches_name_or_alias(&self, prefix: &String) -> bool {
        self.name.as_ref().map(|e| e == prefix).unwrap_or(false)
            || self.alias.as_ref().map(|e| e == prefix).unwrap_or(false)
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

    pub fn get_column_map(&self) -> HashMap<ColId, usize>{
        self.column_read_map.borrow().clone()    
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
    Distinct,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryBlock {
    id: usize,
    pub name: Option<String>,
    pub qbtype: QueryBlockType,
    pub select_list: Vec<NamedExpr>,
    pub quns: Vec<Quantifier>,
    pub pred_list: Option<NodeId>,
    pub group_by: Option<Vec<NodeId>>,
    pub having_clause: Option<NodeId>,
    pub order_by: Option<Vec<(NodeId, Ordering)>>,
    pub distinct: DistinctProperty,
    pub topN: Option<usize>,
}

impl QueryBlock {
    pub fn new(
        id: usize, name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>,
        pred_list: Option<NodeId>, group_by: Option<Vec<NodeId>>, having_clause: Option<NodeId>,
        order_by: Option<Vec<(NodeId, Ordering)>>, distinct: DistinctProperty, topN: Option<usize>,
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
            topN,
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
    pub(crate) fn write_qblock_to_graphviz(&self, qgm: &QGM, file: &mut File) -> std::io::Result<()> {
        // Write current query block first
        let s = "".to_string();
        let select_names: Vec<&String> = self
            .select_list
            .iter()
            .map(|e| e.alias.as_ref().unwrap_or(&s))
            .collect();
        let select_names = format!("{:?}", select_names).replace("\"", "");

        fprint!(file, "  subgraph cluster_{} {{\n", self.name());
        fprint!(file, "    label = \"{} {}\";\n", self.name(), select_names);
        fprint!(file, "    \"{}_pt\"[shape=point, color=black];\n", self.name());

        // Write select_list
        for nexpr in self.select_list.iter() {
            let expr_id = nexpr.expr_id;
            QGM::write_expr_to_graphvis(qgm, expr_id, file);
            let childid_name = QGM::nodeid_to_str(&expr_id);
            fprint!(file, "    exprnode{} -> \"{}_pt\";\n", childid_name, self.name());

        }
    
        for qun in self.quns.iter().rev() {
            fprint!(file, "    \"{}\"[label=\"{}\", color=red]\n", qun.name(), qun.display());
            if let Some(qblock) = &qun.qblock {
                let qblock = &*qblock.borrow();
                fprint!(file, "    \"{}\" -> \"{}_pt\";\n", qun.name(), qblock.name());
                qblock.write_qblock_to_graphviz(qgm, file)?
            }
        }

        if self.pred_list.is_some() {
            QGM::write_expr_to_graphvis(qgm, self.pred_list.unwrap(), file);
        }

        fprint!(file, "}}\n");

        Ok(())
    }
}

pub struct ParserState {
    next_id: usize,
    pub graph: Graph<Expr>,
}

impl ParserState {
    pub fn new() -> Self {
        ParserState {
            next_id: 0,
            graph: Graph::new(),
        }
    }

    pub fn next_id(&mut self) -> usize {
        let retval = self.next_id;
        self.next_id += 1;
        retval
    }
}

impl QGM {
    pub(crate) fn write_qgm_to_graphviz(&self, filename: &str, open_jpg: bool) -> std::io::Result<()> {
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

        self.qblock.write_qblock_to_graphviz(self, &mut file);

        // Write subqueries (CTEs)
        for qblock in self.cte_list.iter() {
            let qblock = &*qblock.borrow();
            qblock.write_qblock_to_graphviz(self, &mut file);
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

    fn nodeid_to_str(nodeid: &NodeId) -> String {
        format!("{:?}", nodeid).replace("(", "").replace(")", "")
    }

    fn write_expr_to_graphvis(qgm: &QGM, expr: NodeId, file: &mut File) -> std::io::Result<()> {
        let id = Self::nodeid_to_str(&expr);
        let (expr, children) = qgm.graph.get_node_with_children(expr);
        fprint!(file, "    exprnode{}[label=\"{}\"];\n", id, expr.name());
        if let Some(children) = children {
            for &childid in children {
                let childid_name = Self::nodeid_to_str(&childid);

                fprint!(file, "    exprnode{} -> exprnode{};\n", childid_name, id);
                Self::write_expr_to_graphvis(qgm, childid, file)?;
            }
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
    Is,
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
            RelOp::Is => "IS",
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
    Column { prefix: Option<String>, colname: String },
    Star,
    Literal(Datum),
    NegatedExpr,
    BinaryExpr(ArithOp),
    RelExpr(RelOp),
    BetweenExpr,
    InListExpr,
    InSubqExpr,
    ExistsExpr,
    LogExpr(LogOp),
    Subquery(QueryBlockLink),
    AggFunction(AggType),
    ScalarFunction(String),
}

impl Expr {
    pub fn name(&self) -> String {
        match self {
            CID(offset) => format!("#{}", *offset),
            Column {
                prefix: tablename,
                colname,
            } => {
                if let Some(tablename) = tablename {
                    format!("{}.{}", tablename, colname)
                } else {
                    format!("{}", colname)
                }
            }
            Star => format!("*"),
            Literal(v) => format!("{}", v).replace(r#"""#, r#"\""#),
            BinaryExpr(op) => format!("{:?}", op),
            NegatedExpr => "-".to_string(),
            RelExpr(op) => format!("{}", op),
            BetweenExpr => format!("BETWEEEN"),
            InListExpr => format!("IN"),
            InSubqExpr => format!("IN_SUBQ"),
            ExistsExpr => format!("EXISTS"),
            LogExpr(op) => format!("{:?}", op),
            Subquery(qblock) => format!("(subquery)"),
            AggFunction(aggtype) => format!("{:?}", aggtype),
            ScalarFunction(name) => format!("{}()", name),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!();
        /*
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
        */
        Ok(())
    }
}

enum E {
    A,
    B(String, String),
}

#[test]
fn foo() {
    println!("sizeof Expr: {}", std::mem::size_of::<Expr>());
    println!("sizeof QueryBlock: {}", std::mem::size_of::<QueryBlock>());
    println!("sizeof E: {}", std::mem::size_of::<E>());
}
