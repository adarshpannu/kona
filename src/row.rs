#![allow(dead_code)]

use std::rc::Rc;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Row {
    cols: Vec<Column>,
}

impl Row {
    pub fn get_column(&self, ix: usize) -> &Column {
        &self.cols[ix]
    }

    pub fn from(cols: Vec<Datum>) -> Row {
        Row { cols }
    }

    pub fn project(&self, colids: &Vec<usize>) -> Row {
        let cols = colids.iter().map(|&ix| self.cols[ix].clone()).collect::<Vec<Column>>();
        Row::from(cols)
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.cols.iter().for_each(|col| {
            let _ = write!(f, "{}, ", col);
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
