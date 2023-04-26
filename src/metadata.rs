// metadata

use crate::includes::*;
use crate::{expr::*, graph::*, row::*};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone, Copy)]
pub enum TableType {
    CSV,
    CSVDIR,
}

#[derive(Debug, Clone)]
pub enum PartType {
    RAW,
    HASHEXPR(Vec<ExprKey>),
}

#[derive(Debug, Clone)]
pub struct PartDesc {
    pub npartitions: usize,
    pub part_type: PartType,
}

impl PartDesc {
    pub fn new(npartitions: usize, part_type: PartType) -> Self {
        PartDesc { npartitions, part_type }
    }

    pub fn printable(&self, expr_graph: &ExprGraph, do_escape: bool) -> String {
        let part_type_str = match &self.part_type {
            PartType::RAW => format!("{}", "RAW"),
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
        format!("p = {} ({})", self.npartitions, part_type_str)
    }
}

#[allow(dead_code)] // FIXME
#[derive(Debug)]
pub struct TableStats {
    nrows: usize,
    avg_row_size: usize,
}

pub trait TableDesc {
    fn get_type(&self) -> TableType;
    fn pathname(&self) -> &String;
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
    tp: TableType,
    pathname: Rc<String>,
    header: bool,
    separator: char,
    columns: Vec<ColDesc>,
    part_desc: PartDesc,
    table_stats: TableStats,
}

impl CSVDesc {
    pub fn new(
        tp: TableType, pathname: Rc<String>, columns: Vec<ColDesc>, separator: char, header: bool, part_desc: PartDesc, table_stats: TableStats,
    ) -> Result<Self, String> {
        let csvdesc = CSVDesc {
            tp,
            pathname,
            header,
            separator,
            columns,
            part_desc,
            table_stats,
        };
        Ok(csvdesc)
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

    pub fn infer_metadata(pathname: &str, separator: char, header: bool) -> Vec<ColDesc> {
        let mut iter = read_lines(&pathname).unwrap();
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
        colnames
            .into_iter()
            .zip(coltypes)
            .enumerate()
            .map(|(id, (name, datatype))| ColDesc { colid: id, name, datatype })
            .collect()
    }
}

impl TableDesc for CSVDesc {
    fn get_type(&self) -> TableType {
        self.tp
    }

    fn columns(&self) -> &Vec<ColDesc> {
        &self.columns
    }

    fn pathname(&self) -> &String {
        &self.pathname
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

    pub fn parse_columns(hm: &HashMap<String, Datum>) -> Result<Vec<ColDesc>, String> {
        // Parse: COLUMNS = "name=STRING,age=INT,emp_dept_id=INT"

        let colstr = hm.get("COLUMNS");
        let colstr = match colstr {
            Some(Datum::STR(coldescstr)) => {
                let coldescstr = &**coldescstr;
                coldescstr
            }
            None => return Err(f!("CSVDIRs need a COLUMNS specification")),
            _ => return Err(f!("Invalid value for option COLUMNS: '{colstr:?}'")),
        };

        let mut coldescs = vec![];
        for (colid, part) in colstr.split(",").enumerate() {
            let mut colname_and_type = part.split("=");
            let err = "Cannot parse COLUMN specification".to_string();
            let (name, datatype) = (
                colname_and_type.next().ok_or(err.clone())?.to_string().to_uppercase(),
                colname_and_type.next().ok_or(err)?,
            );
            let datatype = match datatype {
                "STRING" => DataType::STR,
                "INT" => DataType::INT,
                _ => return Err(f!("Invalid datatype {datatype} in COLUMN specification")),
            };
            let coldesc = ColDesc { colid, name, datatype };
            coldescs.push(coldesc)
        }
        Ok(coldescs)
    }

    fn get_table_type(hm: &HashMap<String, Datum>, name: &String) -> Result<TableType, String> {
        let tp = hm
            .get("TYPE")
            .ok_or(f!("Table {name} does not specify a TYPE."))?
            .as_str(&f!("Table {name} has invalid TYPE."))?;

        let tp = match &tp[..] {
            "CSV" => TableType::CSV,
            "CSVDIR" => TableType::CSVDIR,
            _ => return Err(f!("Table {name} has invalid TYPE.")),
        };
        Ok(tp)
    }

    fn get_header_parm(hm: &HashMap<String, Datum>) -> Result<bool, String> {
        let header = hm.get("HEADER");
        let header = match header {
            Some(Datum::STR(header)) => {
                let header = &*header;
                yes_or_no(&*header).ok_or(f!("Invalid value for option HEADER: '{header}'"))?
            }
            None => true,
            _ => return Err(f!("Invalid value for option HEADER: '{header:?}'")),
        };
        Ok(header)
    }

    fn get_separator_parm(hm: &HashMap<String, Datum>) -> Result<char, String> {
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
        Ok(separator)
    }

    fn get_table_stats(hm: &HashMap<String, Datum>) -> Result<TableStats, String> {
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
        Ok(table_stats)
    }

    fn get_part_desc(hm: &HashMap<String, Datum>) -> Result<PartDesc, String> {
        // CSVDIRs cannot specify PARTITIONS. Only CSVs can.
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
        Ok(part_desc)
    }

    pub fn catalog_table(&mut self, name: String, options: Vec<(String, Datum)>) -> Result<(), String> {
        let name = name.to_uppercase();
        if self.tables.contains_key(&name) {
            return Err(f!("Table {name} cannot be cataloged more than once."));
        }
        let hm: HashMap<String, Datum> = options.into_iter().collect();

        let tp = Self::get_table_type(&hm, &name)?;

        match tp {
            TableType::CSV | TableType::CSVDIR => {
                // PATH, HEADER, SEPARATOR
                let path = hm
                    .get("PATH")
                    .ok_or("Table {name} does not specify a PATH")?
                    .as_str(&f!("PATH does not hold a string for table {name}"))?;

                let header = Self::get_header_parm(&hm)?;
                let separator = Self::get_separator_parm(&hm)?;
                let part_desc = Self::get_part_desc(&hm)?;
                let table_stats = Self::get_table_stats(&hm)?;

                let columns = if hm.get("COLUMNS").is_some() {
                    Self::parse_columns(&hm)?
                } else if matches!(tp, TableType::CSV) {
                    CSVDesc::infer_metadata(&path, separator, header)
                } else {
                    unimplemented!()
                };

                let csvdesc = Rc::new(CSVDesc::new(tp, path, columns, separator, header, part_desc, table_stats)?);
                self.tables.insert(name.to_string(), csvdesc);
                info!("Cataloged table {}", &name);
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
        info!("  pathname = \"{}\"", tbldesc.pathname());
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
