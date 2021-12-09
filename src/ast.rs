use crate::includes::*;

#[derive(Debug)]
pub enum AST {
    CatalogTable { name: String, from: String, opts: Vec<(String, String)>}
}

