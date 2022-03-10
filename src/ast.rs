
use crate::qgm::*;


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
    SetOption {
        name: String,
        value: String,
    },
}
