#![allow(warnings)]

use std::fmt;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub enum Datum {
    STR(Rc<String>),
    INT(isize),
    BOOL(bool),
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum DataType {
    STR,
    INT,
    BOOL,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::INT(il) => write!(f, "{}", il),
            Column::STR(sl) => write!(f, "{}", sl),
            Column::BOOL(bl) => write!(f, "{}", bl),
        }
    }
}

type Column = Datum;

#[derive(Debug)]
pub struct Row {
    cols: Vec<Column>,
}

impl Row {
    pub fn from_csv_line(line: &String) -> Row {
        let cols: Vec<Column> = line
            .split(",")
            .map(|e| Column::STR(Rc::new(e.to_owned())))
            .collect();
        Row { cols }
    }
    
    pub fn get_column(&self, ix: usize) -> &Column {
        &self.cols[ix]
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.cols.iter().for_each(|col| {
            write!(f, "{}, ", col);
        });
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::DataType;

    #[test]
    fn test() {
        let d1 = DataType::STR;
        let d2 = DataType::STR;
        println!("{}", d1 > d2);
    }
}
