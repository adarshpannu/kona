use crate::includes::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::rc::Rc;

use crate::{csv::*, expr::Expr::*, expr::*, graph::*, includes::*, qgm::*, row::*};

#[derive(Debug, Clone)]
pub enum PartType {
    RAW,
    HASHCOL(Vec<ColId>),
    HASHEXPR(Vec<ExprKey>),
}

#[derive(Debug, Clone)]
pub struct PartDesc {
    pub npartitions: usize,
    pub part_type: PartType,
}

impl PartDesc {
    pub fn printable(&self, expr_graph: &ExprGraph, do_escape: bool) -> String {
        let part_type_str = match &self.part_type {
            PartType::RAW => format!("{}", "RAW"),
            PartType::HASHCOL(cols) => format!("{} {:?})", "HASHCOL", cols),
            PartType::HASHEXPR(exprs) => {
                let mut exprstr = String::from("");
                for (ix, expr_key) in exprs.iter().enumerate() {
                    if ix > 0 {
                        exprstr.push_str(", ")
                    }
                    exprstr.push_str(&expr_key.printable(&expr_graph, do_escape));
                }
                format!("{}", exprstr)
            }
        };
        format!("p={}, ptype={}", self.npartitions, part_type_str)
    }
}

#[derive(Debug)]
pub struct TableStats {
    nrows: usize,
    avg_row_size: usize,
}

pub trait TableDesc {
    fn filename(&self) -> &String;
    fn columns(&self) -> &Vec<ColDesc>;
    fn header(&self) -> bool;
    fn separator(&self) -> char;
    fn describe(&self) -> String {
        String::from("")
    }
    fn get_part_desc(&self) -> &PartDesc;
    fn get_column(&self, colname: &String) -> Option<&ColDesc>;

    fn get_stats(&self) -> &TableStats;
}

#[derive(Debug)]
pub struct ColDesc {
    pub colid: ColId,
    pub name: String,
    pub datatype: DataType,
}

impl ColDesc {
    pub fn new(colid: ColId, name: String, datatype: DataType) -> ColDesc {
        ColDesc { colid, name, datatype }
    }
}

#[derive(Debug)]
pub struct CSVDesc {
    filename: Rc<String>,
    header: bool,
    separator: char,
    columns: Vec<ColDesc>,
    part_desc: PartDesc,
    table_stats: TableStats,
}

impl CSVDesc {
    pub fn new(
        filename: Rc<String>, separator: char, header: bool, part_desc: PartDesc, table_stats: TableStats,
    ) -> Self {
        let columns = Self::infer_metadata(&filename, separator, header);
        CSVDesc {
            filename,
            header,
            separator,
            columns,
            part_desc,
            table_stats,
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

    pub fn infer_metadata(filename: &str, separator: char, header: bool) -> Vec<ColDesc> {
        let mut iter = read_lines(&filename).unwrap();
        let mut colnames: Vec<String> = vec![];
        let mut coltypes: Vec<DataType> = vec![];
        let mut first_row = true;

        while let Some(line) = iter.next() {
            let cols: Vec<String> = line
                .unwrap()
                .split(separator)
                .map(|e| e.to_owned().to_uppercase())
                .collect();
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
        colnames
            .into_iter()
            .zip(coltypes)
            .enumerate()
            .map(|(id, (name, datatype))| ColDesc {
                colid: id,
                name,
                datatype,
            })
            .collect()
    }
}

impl TableDesc for CSVDesc {
    fn columns(&self) -> &Vec<ColDesc> {
        &self.columns
    }

    fn filename(&self) -> &String {
        &self.filename
    }

    fn describe(&self) -> String {
        format!("Type: CSV, {:?}", self)
    }

    fn get_column(&self, colname: &String) -> Option<&ColDesc> {
        self.columns.iter().find(|&cd| cd.name == *colname)
    }

    fn get_part_desc(&self) -> &PartDesc {
        &self.part_desc
    }

    fn header(&self) -> bool {
        self.header
    }

    fn separator(&self) -> char {
        self.separator
    }

    fn get_stats(&self) -> &TableStats {
        &self.table_stats
    }
}

pub struct Metadata {
    tables: HashMap<String, Rc<dyn TableDesc>>,
}

impl Metadata {
    pub fn new() -> Metadata {
        Metadata { tables: HashMap::new() }
    }

    pub fn catalog_table(&mut self, name: String, options: Vec<(String, Datum)>) -> Result<(), String> {
        let mut name = name.to_uppercase();
        if self.tables.contains_key(&name) {
            return Err(f!("Table {name} cannot be cataloged more than once."));
        }
        let hm: HashMap<String, Datum> = options.into_iter().collect();

        let tp = hm
            .get("TYPE")
            .ok_or(f!("Table {name} does not specify a TYPE."))?
            .as_str(&f!("Table {name} has invalid TYPE."))?;

        match &tp[..] {
            "CSV" => {
                // PATH, HEADER, SEPARATOR
                let path = hm
                    .get("PATH")
                    .ok_or("Table {name} does not specify a PATH")?
                    .as_str(&f!("PATH does not hold a string for table {name}"))?;

                let header = hm.get("HEADER");
                let header = match header {
                    Some(Datum::STR(header)) => {
                        let header = &*header;
                        yes_or_no(&*header).ok_or(f!("Invalid value for option HEADER: '{header}'"))?
                    }
                    None => true,
                    _ => return Err(f!("Invalid value for option HEADER: '{header:?}'")),
                };

                let separator = match hm.get("SEPARATOR") {
                    Some(Datum::STR(sep)) => {
                        let sep = &**sep;
                        if sep.len() != 1 {
                            return Err(f!("Invalid value for option SEPARATOR: '{sep}'"));
                        } else {
                            sep.chars().next().unwrap()
                        }
                    }
                    _ => ',',
                };

                let npartitions = match hm.get("PARTITIONS") {
                    Some(Datum::INT(npartitions)) => {
                        if *npartitions > 0 {
                            *npartitions as usize
                        } else {
                            return Err(format!("Invalid value for option PARTITIONS"));
                        }
                    }
                    None => 1usize,
                    _ => return Err(format!("Invalid value for option PARTITIONS")),
                };

                let part_desc = PartDesc {
                    npartitions,
                    part_type: PartType::RAW,
                };

                let nrows = match hm.get("NROWS") {
                    Some(Datum::INT(nrows)) => {
                        if *nrows > 0 {
                            *nrows as usize
                        } else {
                            return Err(format!("Invalid value for option NROWS"));
                        }
                    }
                    None => 1usize,
                    _ => return Err(format!("Invalid value for option NROWS")),
                };

                let avg_row_size = match hm.get("AVG_ROW_SIZE") {
                    Some(Datum::INT(avg_row_size)) => {
                        if *avg_row_size > 0 {
                            *avg_row_size as usize
                        } else {
                            return Err(format!("Invalid value for option AVG_ROW_SIZE"));
                        }
                    }
                    None => 1usize,
                    _ => return Err(format!("Invalid value for option AVG_ROW_SIZE")),
                };

                let table_stats = TableStats { nrows, avg_row_size };

                let csvdesc = Rc::new(CSVDesc::new(path, separator, header, part_desc, table_stats));
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
        info!("  PARTITIONS = {:?}", tbldesc.get_part_desc());
        info!("  STATS = {:?}", tbldesc.get_stats());
        info!("  {} COLUMNS", tbldesc.columns().len());
        for cd in tbldesc.columns() {
            info!("      {} {:?}", cd.name, cd.datatype);
        }
        Ok(())
    }

    pub fn get_tabledesc(&self, name: &String) -> Option<Rc<dyn TableDesc>> {
        let val = self.tables.get(&name.to_uppercase());
        val.map(|e| e.clone())
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
