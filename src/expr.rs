// expr

use crate::graph::*;
use crate::includes::*;
use crate::row::*;
use regex::Regex;

use std::fmt;
use Expr::*;

pub type ExprGraph = Graph<ExprKey, Expr, ExprProp>;

/***************************************************************************************************/
#[derive(Debug, Hash, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone, Hash)]
pub enum LogOp {
    And,
    Or,
    Not,
}

impl fmt::Display for LogOp {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display_str = match self {
            LogOp::And => "AND",
            LogOp::Or => "OR",
            LogOp::Not => "NOT",
        };
        write!(f, "{}", display_str)
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone, Hash)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggType {
    COUNT,
    MIN,
    MAX,
    SUM,
    AVG,
}

/***************************************************************************************************/
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ExprProp {
    pub datatype: DataType,
}

impl std::default::Default for ExprProp {
    fn default() -> Self {
        ExprProp {
            datatype: DataType::UNASSIGNED,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Hash)]
pub enum Expr {
    CID(QunId, ColId),
    Column {
        prefix: Option<String>,
        colname: String,
        qunid: QunId,
        colid: ColId,
    },
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
    Subquery(QueryBlockKey),
    AggFunction(AggType, bool),
    ScalarFunction(String),
}

impl Expr {
    pub fn name(&self) -> String {
        match self {
            CID(qunid, colid) => {
                format!("${}.{}", *qunid, *colid)
            }
            Column { prefix, colname, qunid, colid } => {
                if let Some(prefix) = prefix {
                    format!("{}.{} (${}.{})", prefix, colname, *qunid, *colid)
                } else {
                    format!("{} (${}.{})", colname, *qunid, *colid)
                }
            }
            Star => format!("*"),
            Literal(v) => format!("{}", v).replace(r#"""#, r#"\""#),
            BinaryExpr(op) => {
                format!("{}", op)
            }
            NegatedExpr => "-".to_string(),
            RelExpr(op) => {
                format!("{}", op)
            }
            BetweenExpr => {
                format!("BETWEEEN")
            }
            InListExpr => format!("IN"),
            InSubqExpr => {
                format!("IN_SUBQ")
            }
            ExistsExpr => {
                format!("EXISTS")
            }
            LogExpr(op) => {
                format!("{:?}", op)
            }
            Subquery(_) => {
                format!("(subquery)")
            }
            AggFunction(aggtype, ..) => {
                format!("{:?}", aggtype)
            }
            ScalarFunction(name) => {
                format!("{}()", name)
            }
        }
    }

    pub fn isomorphic(graph: &ExprGraph, expr_key1: ExprKey, expr_key2: ExprKey) -> bool {
        let mut iter1 = graph.iter(expr_key1);
        let mut iter2 = graph.iter(expr_key2);

        loop {
            if let Some(expr_key1) = iter1.next() {
                if let Some(expr_key2) = iter2.next() {
                    let expr1 = &graph.get(expr_key1).value;
                    let expr2 = &graph.get(expr_key2).value;
                    if expr1.equals(expr2) == false {
                        return false;
                    }
                } else {
                    // expr_key1 != None, expr_key2 == None
                    return false;
                }
            } else {
                // expr_key1 == None
                return iter2.next().is_none();
            }
        }
    }

    pub fn equals(&self, other: &Expr) -> bool {
        let cmp_status = match (self, other) {
            (CID(qunid1, colid1), CID(qunid2, colid2)) => qunid1 == qunid2 && colid1 == colid2,
            (BinaryExpr(c1), BinaryExpr(c2)) => *c1 == *c2,
            (RelExpr(c1), RelExpr(c2)) => *c1 == *c2,
            (LogExpr(c1), LogExpr(c2)) => *c1 == *c2,
            (
                Column {
                    prefix: p1,
                    colname: n1,
                    qunid: _,
                    colid: _,
                },
                Column {
                    prefix: p2,
                    colname: n2,
                    qunid: _,
                    colid: _,
                },
            ) => p1 == p2 && n1 == n2,
            (Literal(c1), Literal(c2)) => *c1 == *c2,
            (NegatedExpr, NegatedExpr) => true,
            (BetweenExpr, BetweenExpr) => true,
            (InListExpr, InListExpr) => true,
            _ => false,
        };
        cmp_status
    }
}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

impl ExprKey {
    pub fn hash(&self, expr_graph: &ExprGraph) -> u64 {
        let iter = expr_graph.iter(*self);
        let mut state = DefaultHasher::new();

        for exprkey in iter {
            let expr = &expr_graph.get(exprkey).value;
            match expr {
                Expr::Column { colname, qunid, colid, .. } => {
                    colname.hash(&mut state);
                    qunid.hash(&mut state);
                    colid.hash(&mut state);
                }
                Expr::Subquery(_) => panic!("Cannot hash subqueries"),
                _ => expr.hash(&mut state),
            }
        }
        state.finish()
    }

    pub fn get_boolean_factors(self, expr_graph: &ExprGraph, boolean_factors: &mut Vec<ExprKey>) {
        let (expr, _, children) = expr_graph.get3(self);
        if let LogExpr(crate::expr::LogOp::And) = expr {
            let children = children.unwrap();
            let lhs = children[0];
            let rhs = children[1];
            lhs.get_boolean_factors(expr_graph, boolean_factors);
            rhs.get_boolean_factors(expr_graph, boolean_factors);
        } else {
            boolean_factors.push(self)
        }
    }

    pub fn to_string(self: &ExprKey) -> String {
        format!("{:?}", *self).replace("(", "").replace(")", "")
    }

    pub fn printable(&self, graph: &ExprGraph, do_escape: bool) -> String {
        let (expr, _, children) = graph.get3(*self);
        let retval = match expr {
            CID(qunid, colid) => {
                format!("${}.{}", *qunid, *colid)
            }
            Column { prefix, colname, .. } => {
                if let Some(prefix) = prefix {
                    format!("{}.{}", prefix, colname)
                } else {
                    format!("{}", colname)
                }
            }
            Star => format!("*"),
            Literal(v) => format!("{}", v).replace(r#"""#, r#"\""#),
            BinaryExpr(op) => {
                let (lhs_key, rhs_key) = (children.unwrap()[0], children.unwrap()[1]);
                format!("{} {} {}", lhs_key.printable(graph, false), op, rhs_key.printable(graph, false),)
            }
            NegatedExpr => {
                let lhs_key = children.unwrap()[0];
                format!("-{}", lhs_key.printable(graph, false))
            }
            RelExpr(op) => {
                let (lhs_key, rhs_key) = (children.unwrap()[0], children.unwrap()[1]);
                format!("{} {} {}", lhs_key.printable(graph, false), op, rhs_key.printable(graph, false),)
            }
            LogExpr(op) => {
                let (lhs_key, rhs_key) = (children.unwrap()[0], children.unwrap()[1]);
                format!("{} {} {}", lhs_key.printable(graph, false), op, rhs_key.printable(graph, false),)
            }
            BetweenExpr => {
                format!("BETWEEEN")
            }
            InListExpr => format!("IN"),
            InSubqExpr => {
                format!("IN_SUBQ")
            }
            ExistsExpr => {
                format!("EXISTS")
            }
            Subquery(_) => {
                format!("(subquery)")
            }
            AggFunction(aggtype, _) => {
                let child_id = children.unwrap()[0];
                format!("{:?}({})", aggtype, child_id.printable(graph, false))
            }
            ScalarFunction(name) => {
                format!("{}()", name)
            }
        };
        if do_escape {
            let re = Regex::new(r"([><])").unwrap();
            re.replace_all(&retval[..], "\\$1").to_string()
        } else {
            retval
        }
    }
}
