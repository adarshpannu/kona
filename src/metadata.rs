#![allow(warnings)]
#![allow(warnings)]

use std::collections::HashMap;
use crate::expr::{Expr::*, *};
use crate::includes::*;

pub struct CSVDesc {
    path: String,
    header: bool,
    separator: char,
}

pub enum TableDesc {
    CSVDesc(CSVDesc),
}

pub struct Metadata {
    tables: HashMap<String, TableDesc>,
}

impl Metadata {
    pub fn new() -> Metadata {
        Metadata { tables: HashMap::new() }
    }

    pub fn register_table(name: String, options: Vec<(String, String)>) {
        let hm: HashMap<String, String> = options.into_iter().collect();
        /*
        match hm.get("TYPE") {
            Some("CSV") => {}
        }
        */
        unimplemented!()
    }
}

#[test]
fn test() {
    println!("Hello");
}
