#![allow(warnings)]

use crate::{ast::Expr::*, ast::*, csv::*, includes::*, row::*};

use crate::includes::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::rc::Rc;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[derive(Debug)]
pub struct CSVDesc {
    filename: String,
    header: bool,
    separator: char,
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
}

impl CSVDesc {
    pub fn new(filename: String, separator: char, header: bool) -> Self {
        let (colnames, coltypes) = Self::infer_metadata(&filename, separator, header);
        CSVDesc {
            filename,
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

    pub fn infer_metadata(filename: &str, separator: char, header: bool) -> (Vec<String>, Vec<DataType>) {
        let mut iter = read_lines(&filename).unwrap();
        let mut colnames: Vec<String> = vec![];
        let mut coltypes: Vec<DataType> = vec![];
        let mut first_row = true;

        while let Some(line) = iter.next() {
            let cols: Vec<String> = line.unwrap().split(separator).map(|e| e.to_owned().to_uppercase()).collect();
            if colnames.len() == 0 {
                if header {
                    colnames = cols
                } else {
                    // Default column names
                    colnames = (0..cols.len()).map(|ix| format!("COL_{}", ix)).collect();
                }
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

pub trait TableDesc {
    fn filename(&self) -> &String;
    fn colnames(&self) -> &Vec<String>;
    fn coltypes(&self) -> &Vec<DataType>;
    fn header(&self) -> bool;
    fn separator(&self) -> char;
    fn describe(&self) -> String {
        String::from("")
    }
    fn get_coldesc(&self, colname: &String) -> Option<(ColId, DataType)>;
}

impl TableDesc for CSVDesc {
    fn colnames(&self) -> &Vec<String> {
        &self.colnames
    }

    fn coltypes(&self) -> &Vec<DataType> {
        &self.coltypes
    }

    fn filename(&self) -> &String {
        &self.filename
    }

    fn describe(&self) -> String {
        format!("Type: CSV, {:?}", self)
    }

    fn get_coldesc(&self, colname: &String) -> Option<(ColId, DataType)> {
        let ix = self.colnames.iter().position(|cn| cn == colname);
        ix.map(|ix| (ix, self.coltypes[ix]))
    }

    fn header(&self) -> bool {
        self.header
    }

    fn separator(&self) -> char {
        self.separator
    }
}

pub struct Metadata {
    tables: HashMap<String, Rc<dyn TableDesc>>,
}

impl Metadata {
    pub fn new() -> Metadata {
        Metadata { tables: HashMap::new() }
    }

    pub fn catalog_table(&mut self, name: String, options: Vec<(String, String)>) -> Result<(), String> {
        let mut name = name.to_uppercase();
        if self.tables.contains_key(&name) {
            return Err(format!("Table {} cannot be cataloged more than once.", name));
        }
        let hm: HashMap<String, String> = options.into_iter().collect();
        match hm.get("TYPE").map(|e| &e[..]) {
            Some("CSV") => {
                // PATH, HEADER, SEPARATOR
                let path = hm.get("PATH").expect("PATH not specified").to_string();
                let header = match hm.get("HEADER").map(|e| &e[..]) {
                    Some("Y") | Some("YES") => true,
                    _ => false,
                };
                let separator = match hm.get("SEPARATOR").map(|e| &e[..]) {
                    Some(sep) => {
                        if sep.len() != 1 {
                            ','
                        } else {
                            sep.chars().next().unwrap()
                        }
                    }
                    _ => ',',
                };
                let csvdesc = Rc::new(CSVDesc::new(path, separator, header));
                self.tables.insert(name.to_string(), csvdesc);
                info!("Cataloged table {}", &name);
            }
            _ => {
                unimplemented!()
            }
        }
        Ok(())
    }

    pub fn describe_table(&self, name: String) -> Result<(), String> {
        let name = name.to_uppercase();
        let tbldesc = self.tables.get(&name);
        if tbldesc.is_none() {
            return Err(format!("Table {} does not exist.", name));
        }
        let tbldesc = tbldesc.unwrap();
        info!("Table {}", name);
        info!("  FILENAME = \"{}\"", tbldesc.filename());
        info!("  HEADER = {}", tbldesc.header());
        info!("  SEPARATOR = '{}'", tbldesc.separator());
        info!("  {} COLUMNS", tbldesc.colnames().len());
        for (colname, coltype) in tbldesc.colnames().iter().zip(tbldesc.coltypes()) {
            info!("      {} {:?}", colname, coltype);
        }

        /*
                fn filename(&self) -> &String;
        fn colnames(&self) -> &Vec<String>;
        fn coltypes(&self) -> &Vec<DataType>;
        fn header(&self) -> bool;
        fn separator(&self) -> char;
        fn describe(&self) -> String {
            String::from("")
        }
        fn get_coldesc(&self, colname: &String) -> Option<(ColId, DataType)>;

            */
        Ok(())
    }

    pub fn get_tabledesc(&self, name: &String) -> Option<Rc<dyn TableDesc>> {
        let val = self.tables.get(&name.to_uppercase());
        val.map(|e| e.clone())
    }
}

#[test]
fn test() {
    println!("Hello");
}
