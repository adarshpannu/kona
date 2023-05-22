// ast: abstract syntax tree definitions

use crate::{datum::Datum, qgm::QGM};

#[derive(Debug)]
pub enum AST {
    CatalogTable { name: String, options: Vec<(String, Datum)> },
    DescribeTable { name: String },
    QGM(QGM),
    SetOption { name: String, value: Datum },
}
