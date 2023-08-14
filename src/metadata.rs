// metadata

use std::{collections::HashMap, fs::File, rc::Rc};

use arrow2::io::csv::read;

use crate::{expr::ExprGraph, graph::ExprKey, includes::*, Datum};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    CSV,
    Parquet,
    Query,
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

    pub fn describe(&self, expr_graph: &ExprGraph, do_escape: bool) -> String {
        let part_type_str = match &self.part_type {
            PartType::RAW => String::from("RAW"),
            PartType::HASHEXPR(exprs) => {
                let mut exprstr = String::from("");
                for (ix, expr_key) in exprs.iter().enumerate() {
                    if ix > 0 {
                        exprstr.push_str(", ")
                    }
                    exprstr.push_str(&expr_key.describe(expr_graph, do_escape));
                }
                exprstr.to_string()
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
    fn fields(&self) -> &Vec<Field>;
    fn header(&self) -> bool;
    fn separator(&self) -> char;
    fn describe(&self) -> String {
        String::from("")
    }
    fn get_column(&self, colname: &str) -> Option<(usize, &Field)>;
    fn get_part_desc(&self) -> Option<&PartDesc>;
    fn get_stats(&self) -> Option<&TableStats>;
}

#[derive(Debug)]
pub struct CSVDesc {
    typ: TableType,
    pathname: Rc<String>,
    header: bool,
    separator: char,
    columns: Vec<Field>,
    part_desc: PartDesc,
    table_stats: TableStats,
}

impl CSVDesc {
    pub fn new(
        typ: TableType, pathname: Rc<String>, columns: Vec<Field>, separator: char, header: bool, part_desc: PartDesc, table_stats: TableStats,
    ) -> Result<Self, String> {
        let csvdesc = CSVDesc { typ, pathname, header, separator, columns, part_desc, table_stats };
        Ok(csvdesc)
    }

    pub fn infer_metadata(pathname: &str, separator: char, header: bool) -> Result<Vec<Field>, String> {
        // Create a CSV reader. This is typically created on the thread that reads the file and
        // thus owns the read head.
        let mut reader = read::ReaderBuilder::new().has_headers(header).delimiter(separator as u8).from_path(pathname).map_err(|err| stringify1(err, pathname))?;

        // Infers the fields using the default inferer. The inferer is just a function that maps bytes
        // to a `DataType`.
        let (fields, _) = read::infer_schema(&mut reader, None, true, &read::infer).map_err(|err| stringify1(err, pathname))?;

        let fields = fields.iter().map(|f| Field::new(f.name.to_uppercase(), f.data_type.clone(), f.is_nullable)).collect();

        Ok(fields)
    }
}

impl TableDesc for CSVDesc {
    fn get_type(&self) -> TableType {
        self.typ
    }

    fn fields(&self) -> &Vec<Field> {
        &self.columns
    }

    fn pathname(&self) -> &String {
        &self.pathname
    }

    fn describe(&self) -> String {
        format!("Type: CSV, {:?}", self)
    }

    fn get_column(&self, colname: &str) -> Option<(usize, &Field)> {
        self.columns.iter().enumerate().find(|(_, cd)| cd.name == *colname)
    }

    fn get_part_desc(&self) -> Option<&PartDesc> {
        Some(&self.part_desc)
    }

    fn header(&self) -> bool {
        self.header
    }

    fn separator(&self) -> char {
        self.separator
    }

    fn get_stats(&self) -> Option<&TableStats> {
        Some(&self.table_stats)
    }
}

#[derive(Default)]
pub struct Metadata {
    tables: HashMap<String, Rc<dyn TableDesc>>,
}

impl Metadata {
    pub fn parse_columns(hm: &HashMap<String, Datum>) -> Result<Vec<Field>, String> {
        // Parse: COLUMNS = "name=STRING,age=INT,emp_dept_id=INT"

        let colstr = hm.get("COLUMNS");
        let colstr = match colstr {
            Some(Utf8(fieldstr)) => fieldstr,
            _ => return Err(f!("Invalid value for option COLUMNS: '{colstr:?}'")),
        };

        let mut fields = vec![];
        for part in colstr.split(',') {
            let mut colname_and_type = part.split('=');
            let err = "Cannot parse COLUMN specification".to_string();
            let (name, datatype) = (colname_and_type.next().ok_or(err.clone())?.to_string().to_uppercase(), colname_and_type.next().ok_or(err)?);
            let datatype = match datatype {
                "STRING" => DataType::Utf8,
                "INT" => DataType::Int64,
                _ => return Err(f!("Invalid datatype {datatype} in COLUMN specification")),
            };
            let field = Field::new(name, datatype, false);
            fields.push(field)
        }
        Ok(fields)
    }

    fn get_table_type(hm: &HashMap<String, Datum>, name: &String) -> Result<TableType, String> {
        let typ = hm.get("TYPE").ok_or(f!("Table {name} does not specify a TYPE."))?;
        let typ = typ.try_as_str().ok_or(f!("Table {name} has invalid TYPE."))?;

        let typ = typ.to_uppercase();
        let typ = typ.as_str();
        let typ = match typ {
            "CSV" => TableType::CSV,
            "PARQUET" => TableType::Parquet,
            _ => return Err(f!("Table {name} has invalid TYPE.")),
        };
        Ok(typ)
    }

    fn get_header_parm(hm: &HashMap<String, Datum>) -> Result<bool, String> {
        let header = hm.get("HEADER");
        let header = match header {
            Some(Utf8(header)) => yes_or_no(header).ok_or(f!("Invalid value for option HEADER: '{header}'"))?,
            None => true,
            _ => return Err(f!("Invalid value for option HEADER: '{header:?}'")),
        };
        Ok(header)
    }

    fn get_separator_parm(hm: &HashMap<String, Datum>) -> Result<char, String> {
        let separator = match hm.get("SEPARATOR") {
            Some(Utf8(sep)) => {
                let sep = sep.as_str();
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
            Some(Int64(nrows)) => {
                if *nrows > 0 {
                    *nrows as usize
                } else {
                    return Err(String::from("Invalid value for option NROWS"));
                }
            }
            None => 1usize,
            _ => return Err(String::from("Invalid value for option NROWS")),
        };

        let avg_row_size = match hm.get("AVG_ROW_SIZE") {
            Some(Int64(avg_row_size)) => {
                if *avg_row_size > 0 {
                    *avg_row_size as usize
                } else {
                    return Err(String::from("Invalid value for option AVG_ROW_SIZE"));
                }
            }
            None => 1usize,
            _ => return Err(String::from("Invalid value for option AVG_ROW_SIZE")),
        };
        let table_stats = TableStats { nrows, avg_row_size };
        Ok(table_stats)
    }

    fn get_part_desc(hm: &HashMap<String, Datum>) -> Result<PartDesc, String> {
        let npartitions = match hm.get("PARTITIONS") {
            Some(Int64(npartitions)) => {
                if *npartitions > 0 {
                    *npartitions as usize
                } else {
                    return Err(String::from("Invalid value for option PARTITIONS"));
                }
            }
            None => 1usize,
            _ => return Err(String::from("Invalid value for option PARTITIONS")),
        };

        let part_desc = PartDesc { npartitions, part_type: PartType::RAW };
        Ok(part_desc)
    }

    pub fn catalog_table(&mut self, name: String, options: Vec<(String, Datum)>) -> Result<(), String> {
        let name = name.to_uppercase();
        if self.tables.contains_key(&name) {
            return Err(f!("Table {name} cannot be cataloged more than once."));
        }
        let hm: HashMap<String, Datum> = options.into_iter().collect();

        let typ = Self::get_table_type(&hm, &name)?;

        match typ {
            TableType::CSV => {
                // PATH, HEADER, SEPARATOR
                let path = hm.get("PATH").ok_or("Table {name} does not specify a PATH")?.try_as_str().ok_or("PATH does not hold a string for table {name}")?;
                let path = Rc::new(String::from(path));

                let header = Self::get_header_parm(&hm)?;
                let separator = Self::get_separator_parm(&hm)?;
                let part_desc = Self::get_part_desc(&hm)?;
                let table_stats = Self::get_table_stats(&hm)?;

                let columns = if hm.get("COLUMNS").is_some() {
                    Self::parse_columns(&hm)?
                } else if matches!(typ, TableType::CSV) {
                    CSVDesc::infer_metadata(&path, separator, header)?
                } else {
                    unimplemented!()
                };

                let csvdesc = Rc::new(CSVDesc::new(typ, path, columns, separator, header, part_desc, table_stats)?);
                self.tables.insert(name.to_string(), csvdesc);
                info!("Cataloged table {}", &name);
            }
            TableType::Parquet => {
                // PATH, HEADER, SEPARATOR
                let path = hm.get("PATH").ok_or("Table {name} does not specify a PATH")?.try_as_str().ok_or("PATH does not hold a string for table {name}")?;
                let path = Rc::new(String::from(path));

                if hm.get("HEADER").is_some() {
                    return Err(f!("HEADER cannot be specified for Parquet files."));
                }
                if hm.get("SEPARATOR").is_some() {
                    return Err(f!("HEADER cannot be specified for Parquet files."));
                }
                if hm.get("COLUMNS").is_some() {
                    return Err(f!("COLUMNS cannot be specified for Parquet files."));
                }

                let part_desc = Self::get_part_desc(&hm)?;
                let table_stats = Self::get_table_stats(&hm)?;

                let columns = ParquetDesc::infer_metadata(&path)?;
                let csvdesc = Rc::new(ParquetDesc::new(typ, path, columns, part_desc, table_stats)?);
                self.tables.insert(name.to_string(), csvdesc);
                info!("Cataloged table {}", &name);
            }
            unexpected => panic!("Unexpected type: {:?}", unexpected),
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
        if tbldesc.get_type() == TableType::CSV {
            info!("  HEADER = {}", tbldesc.header());
            info!("  SEPARATOR = '{}'", tbldesc.separator());
        }
        info!("  PARTITIONS = {:?}", tbldesc.get_part_desc());
        info!("  STATS = {:?}", tbldesc.get_stats());
        info!("  {} COLUMNS", tbldesc.fields().len());
        for cd in tbldesc.fields() {
            info!("      {} {:?}", cd.name, cd.data_type);
        }
        Ok(())
    }

    pub fn get_tabledesc(&self, name: &str) -> Option<Rc<dyn TableDesc>> {
        let val = self.tables.get(&name.to_uppercase());
        val.cloned()
    }
}

#[derive(Debug)]
pub struct ParquetDesc {
    typ: TableType,
    pathname: Rc<String>,
    columns: Vec<Field>,
    part_desc: PartDesc,
    table_stats: TableStats,
}

impl ParquetDesc {
    pub fn new(typ: TableType, pathname: Rc<String>, columns: Vec<Field>, part_desc: PartDesc, table_stats: TableStats) -> Result<Self, String> {
        let csvdesc = ParquetDesc { typ, pathname, columns, part_desc, table_stats };
        Ok(csvdesc)
    }

    pub fn infer_metadata(pathname: &str) -> Result<Vec<Field>, String> {
        use arrow2::io::parquet::read;

        let mut reader = File::open(pathname).map_err(stringify)?;

        let metadata = read::read_metadata(&mut reader).map_err(stringify)?;

        let schema = read::infer_schema(&metadata).map_err(stringify)?;

        let fields = schema.fields.iter().map(|f| Field::new(f.name.to_uppercase(), f.data_type.clone(), f.is_nullable)).collect();

        Ok(fields)
    }
}

impl TableDesc for ParquetDesc {
    fn get_type(&self) -> TableType {
        self.typ
    }

    fn fields(&self) -> &Vec<Field> {
        &self.columns
    }

    fn pathname(&self) -> &String {
        &self.pathname
    }

    fn describe(&self) -> String {
        format!("Type: Parquet, {:?}", self)
    }

    fn get_column(&self, colname: &str) -> Option<(usize, &Field)> {
        self.columns.iter().enumerate().find(|(_, cd)| cd.name == *colname)
    }

    fn get_part_desc(&self) -> Option<&PartDesc> {
        Some(&self.part_desc)
    }

    fn header(&self) -> bool {
        todo!()
    }

    fn separator(&self) -> char {
        todo!()
    }

    fn get_stats(&self) -> Option<&TableStats> {
        Some(&self.table_stats)
    }
}

#[derive(Debug)]
pub struct QueryDesc {
    columns: Vec<Field>,
}

impl QueryDesc {
    pub fn new(columns: Vec<Field>) -> Self {
        QueryDesc { columns }
    }
}

impl TableDesc for QueryDesc {
    fn get_type(&self) -> TableType {
        TableType::Query
    }

    fn fields(&self) -> &Vec<Field> {
        &self.columns
    }

    fn pathname(&self) -> &String {
        panic!("No pathname")
    }

    fn describe(&self) -> String {
        format!("Type: Query, {:?}", self)
    }

    fn get_column(&self, colname: &str) -> Option<(usize, &Field)> {
        self.columns.iter().enumerate().find(|(_, cd)| cd.name == *colname)
    }

    fn get_part_desc(&self) -> Option<&PartDesc> {
        None
    }

    fn header(&self) -> bool {
        todo!()
    }

    fn separator(&self) -> char {
        todo!()
    }

    fn get_stats(&self) -> Option<&TableStats> {
        None
    }
}
