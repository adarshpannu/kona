#![allow(warnings)]
#![allow(warnings)]

use crate::expr::{Expr::*, *};
use crate::includes::*;
use std::collections::HashMap;

#[derive(Debug)]
pub struct CSVDesc {
    path: String,
    header: bool,
    separator: char,
}

#[derive(Debug)]
pub enum TableDesc {
    CSVDesc(CSVDesc),
}

#[derive(Debug)]
pub struct Metadata {
    tables: HashMap<String, TableDesc>,
}

impl Metadata {
    pub fn new() -> Metadata {
        Metadata {
            tables: HashMap::new(),
        }
    }

    pub fn register_table(&mut self, name: &str, options: Vec<(&str, &str)>) {
        let hm: HashMap<&str, &str> = options.into_iter().collect();        
        match hm.get("TYPE") {
            Some(&"CSV") => {
                // PATH, HEADER, SEPARATOR
                let path =
                    hm.get("PATH").expect("PATH not specified").to_string();
                let header = match hm.get("HEADER") {
                    Some(&"Y") => true,
                    _ => false,
                };
                let separator = match hm.get("SEPARATOR") {
                    Some(sep) => {
                        if sep.len() != 1 {
                            ','
                        } else {
                            sep.chars().next().unwrap()
                        }
                    }
                    _ => ',',
                };
                let csvdesc = CSVDesc {
                    path,
                    header,
                    separator,
                };
                let tabledesc = TableDesc::CSVDesc(csvdesc);
                self.tables.insert(name.to_string(), tabledesc);
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

#[test]
fn test() {
    println!("Hello");
}
