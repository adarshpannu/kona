// row: Representation of N-tuples

use std::{fmt, rc::Rc};

use crate::includes::*;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, Ord, Serialize, Deserialize)]
pub enum Datum {
    NULL,
    STR(Rc<String>),
    INT(isize),
    BOOL(bool),
}

impl Datum {
    pub fn as_isize(&self, err_str: &str) -> Result<isize, String> {
        if let Datum::INT(val) = self {
            Ok(*val)
        } else {
            Err(err_str.to_string())
        }
    }

    pub fn as_str(&self, err_str: &str) -> Result<Rc<String>, String> {
        if let Datum::STR(val) = self {
            Ok(Rc::clone(val))
        } else {
            Err(err_str.to_string())
        }
    }

    pub fn as_usize(&self, err_str: &str) -> Result<usize, String> {
        if let Datum::INT(val) = self {
            if *val >= 0 {
                return Ok(*val as usize);
            }
        }
        return Err(err_str.to_string());
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::INT(il) => write!(f, "{}", il),
            Column::STR(sl) => write!(f, "\"{}\"", sl),
            Column::BOOL(bl) => write!(f, "{}", bl),
            Column::NULL => write!(f, "{}", "NULL"),
        }
    }
}

type Column = Datum;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Row {
    cols: Vec<Column>,
}

impl Row {
    pub fn get_column_mut(&mut self, ix: ColId) -> &mut Column {
        &mut self.cols[ix]
    }

    pub fn set_column(&mut self, ix: ColId, newcol: &Column) {
        self.cols[ix] = newcol.clone()
    }

    pub fn get_column(&self, ix: ColId) -> &Column {
        &self.cols[ix]
    }

    pub fn from(cols: Vec<Datum>) -> Row {
        Row { cols }
    }

    pub fn empty(len: usize) -> Row {
        Row::from(vec![Datum::NULL; len])
    }

    pub fn project(&self, colids: &Vec<ColId>) -> Row {
        let cols = colids.iter().map(|&colid| self.cols[colid].clone()).collect::<Vec<Column>>();
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
                let _ = write!(f, ",");
            }
            let _ = write!(f, "{}", col);
        });
        write!(f, "")
    }
}

impl Default for Row {
    fn default() -> Self {
        Row::from(vec![])
    }
}
