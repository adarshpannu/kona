//#![allow(dead_code)]

use core::panic;
use std::fmt;
use std::sync::Arc;
use crate::includes::*;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, Ord, Serialize)]
pub enum Datum {
    STR(Box<String>),
    INT(isize),
    BOOL(bool),
}

impl Datum {
    pub fn as_int(&self) -> isize {
        if let Datum::INT(val) = self {
            *val
        } else {
            panic!("Datum is not an INT.")
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Serialize)]
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

#[derive(Debug, PartialEq, Eq, Hash, Serialize)]
pub struct Row {
    cols: Vec<Column>,
}

impl Row {
    pub fn get_column_mut(&mut self, ix: usize) -> &mut Column {
        &mut self.cols[ix]
    }

    pub fn set_column(&mut self, ix: usize, newcol: &Column) {
        self.cols[ix] = newcol.clone()
    }

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

    pub fn len(&self) -> usize {
        self.cols.len()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.cols.iter().enumerate().for_each(|(ix, col)| {
            if ix > 0 {
                let _ = write!(f, ", ");
            }
            let _ = write!(f, "{}", col);
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
