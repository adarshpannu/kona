use crate::includes::*;
use crate::graph::*;
use crate::row::*;
use regex::Regex;

use Expr::*;
use std::fmt;

pub type ExprGraph = Graph<ExprId, Expr, ExprProp>;

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
            datatype: DataType::UNKNOWN,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
    Subquery(QueryBlockLink),
    AggFunction(AggType, bool),
    ScalarFunction(String),
}

impl Expr {
    pub fn name(&self) -> String {
        match self {
            CID(qunid, colid) => format!("CID({},{})", *qunid, *colid),
            Column {
                prefix,
                colname,
                qunid,
                colid,
            } => {
                if let Some(prefix) = prefix {
                    format!("{}.{} ({}.{})", prefix, colname, *qunid, *colid)
                } else {
                    format!("{} ({}.{})", colname, *qunid, *colid)
                }
            }
            Star => format!("*"),
            Literal(v) => format!("{}", v).replace(r#"""#, r#"\""#),
            BinaryExpr(op) => format!("{}", op),
            NegatedExpr => "-".to_string(),
            RelExpr(op) => format!("{}", op),
            BetweenExpr => format!("BETWEEEN"),
            InListExpr => format!("IN"),
            InSubqExpr => format!("IN_SUBQ"),
            ExistsExpr => format!("EXISTS"),
            LogExpr(op) => format!("{:?}", op),
            Subquery(qblock) => format!("(subquery)"),
            AggFunction(aggtype, is_distinct) => format!("{:?}", aggtype),
            ScalarFunction(name) => format!("{}()", name),
        }
    }

    pub fn isomorphic(graph: &ExprGraph, expr_id1: ExprId, expr_id2: ExprId) -> bool {
        let (expr1, _, children1) = graph.get3(expr_id1);
        let (expr2, _, children2) = graph.get3(expr_id2);
        let shallow_matched = match (expr1, expr2) {
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
        if shallow_matched {
            if children1.is_some() != children2.is_some() {
                return false;
            }
            if children1.is_some() && children2.is_some() {
                let children1 = children1.unwrap();
                let children2 = children2.unwrap();
                if children1.len() == children2.len() {
                    for (&child1, &child2) in children1.iter().zip(children2.iter()) {
                        if !Self::isomorphic(graph, child1, child2) {
                            return false;
                        }
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    pub fn to_string(expr_id: ExprId, graph: &ExprGraph, do_escape: bool) -> String {
        let (expr, _, children) = graph.get3(expr_id);
        let retval = match expr {
            CID(qunid, colid) => format!("CID({},{})", *qunid, *colid),
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
                let (lhs_id, rhs_id) = (children.unwrap()[0], children.unwrap()[1]);
                format!(
                    "{} {} {}",
                    Self::to_string(lhs_id, graph, false),
                    op,
                    Self::to_string(rhs_id, graph, false)
                )
            }
            NegatedExpr => "-".to_string(),
            RelExpr(op) => {
                let (lhs_id, rhs_id) = (children.unwrap()[0], children.unwrap()[1]);
                format!(
                    "{} {} {}",
                    Self::to_string(lhs_id, graph, false),
                    op,
                    Self::to_string(rhs_id, graph, false)
                )
            }
            LogExpr(op) => {
                let (lhs_id, rhs_id) = (children.unwrap()[0], children.unwrap()[1]);
                format!(
                    "{} {} {}",
                    Self::to_string(lhs_id, graph, false),
                    op,
                    Self::to_string(rhs_id, graph, false)
                )
            }
            BetweenExpr => format!("BETWEEEN"),
            InListExpr => format!("IN"),
            InSubqExpr => format!("IN_SUBQ"),
            ExistsExpr => format!("EXISTS"),
            Subquery(qblock) => format!("(subquery)"),
            AggFunction(aggtype, is_distinct) => {
                let child_id = children.unwrap()[0];
                format!("{:?}({})", aggtype, Self::to_string(child_id, graph, false))
            },
            ScalarFunction(name) => format!("{}()", name),
            _ => {
                debug!("todo - {:?}", expr);
                todo!()
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
