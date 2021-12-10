use crate::includes::*;

use crate::expr::Expr;
use crate::metadata::TableDesc;

use std::rc::Rc;
use std::cell::RefCell;

#[derive(Debug)]
pub enum AST<'a> {
    CatalogTable { name: &'a str, options: Vec<(&'a str, &'a str)>},
    Query { select_list: Vec<ExprLink>, from_list: Vec<&'a str>, where_clause: ExprLink }
}

enum DistinctProperty {
    Enforce,
    Permit
}

struct QGM<'a> {
    tables: Vec<&'a TableDesc>,
    qblocks: Vec<&'a QueryBlock>
}

struct QueryBlock {
    head: Vec<(String, ExprLink)>,
    distinct: DistinctProperty
}

enum Quantifier {
    Table(TableDesc),
    QueryBlock(QueryBlock) 
}
