#![allow(warnings)]

use crate::error::{FlareError, FlareErrorCode, FlareErrorCode::*};
use crate::expr::{Expr::*, *};
use crate::{csv::*, expr::Expr::*, expr::*, includes::*, row::*, task::*};

use crate::includes::*;
use std::collections::HashMap;

use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[derive(Debug)]
pub struct CSVDesc {
    path: String,
    header: bool,
    separator: char,
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
}

impl CSVDesc {
    pub fn new(path: String, header: bool, separator: char) -> Self {
        let (colnames, coltypes) = Self::infer_metadata(&path);
        CSVDesc {
            path,
            header,
            separator,
            colnames,
            coltypes,
        }
    }

    fn infer_datatype(str: &String) -> DataType {
        let res = str.parse::<i32>();
        if res.is_ok() {
            DataType::INT
        } else if str.eq("true") || str.eq("false") {
            DataType::BOOL
        } else {
            DataType::STR
        }
    }

    pub fn colnames(&self) -> &Vec<String> {
        &self.colnames
    }

    pub fn coltypes(&self) -> &Vec<DataType> {
        &self.coltypes
    }

    pub fn infer_metadata(filename: &str) -> (Vec<String>, Vec<DataType>) {
        let mut iter = read_lines(&filename).unwrap();
        let mut colnames: Vec<String> = vec![];
        let mut coltypes: Vec<DataType> = vec![];
        let mut first_row = true;

        while let Some(line) = iter.next() {
            let cols: Vec<String> =
                line.unwrap().split('|').map(|e| e.to_owned()).collect();
            if colnames.len() == 0 {
                colnames = cols;
            } else {
                for (ix, col) in cols.iter().enumerate() {
                    let datatype = CSVDesc::infer_datatype(col);
                    if first_row {
                        coltypes.push(datatype)
                    } else if coltypes[ix] != DataType::STR {
                        coltypes[ix] = datatype;
                    } else {
                        coltypes[ix] = DataType::STR;
                    }
                }
                first_row = false;
            }
        }
        (colnames, coltypes)
    }
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

    pub fn register_table(
        &mut self, name: &str, options: Vec<(&str, &str)>,
    ) -> Result<(), FlareError> {
        if self.tables.contains_key(name) {
            error!(
                "{}",
                format!("Table {} cannot be cataloged more than once.", name)
            );
            return Err(FlareError::new(
                TableAlreadyCataloged,
                format!("{}", name),
            ));
        }
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
                let csvdesc = CSVDesc::new(path, header, separator);
                let tabledesc = TableDesc::CSVDesc(csvdesc);
                self.tables.insert(name.to_string(), tabledesc);
            }
            _ => {
                unimplemented!()
            }
        }
        Ok(())
    }
}

#[test]
fn test() {
    println!("Hello");
}
