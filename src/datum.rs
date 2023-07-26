// row: Representation of N-tuples

use std::{fmt, rc::Rc};

use crate::includes::*;

// TODO! Datum needs to hold arrow2 scalars
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, Ord, Serialize, Deserialize)]
pub enum Datum {
    NULL,
    STR(Rc<String>),
    INT(isize),
    BOOL(bool),
}

impl Datum {
    pub fn try_as_isize(&self, err_str: &str) -> Result<isize, String> {
        if let Datum::INT(val) = self {
            Ok(*val)
        } else {
            Err(err_str.to_string())
        }
    }

    pub fn try_as_str(&self, err_str: &str) -> Result<Rc<String>, String> {
        if let Datum::STR(val) = self {
            Ok(Rc::clone(val))
        } else {
            Err(err_str.to_string())
        }
    }

    pub fn as_isize(&self) -> isize {
        if let Datum::INT(val) = self {
            *val
        } else {
            panic!("Datum::as_isize(): Datum doesn't hold an isize.")
        }
    }

    pub fn as_str(&self) -> Rc<String> {
        if let Datum::STR(val) = self {
            val.clone()
        } else {
            panic!("Datum::as_str(): Datum doesn't hold a string.")
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Datum::INT(il) => write!(f, "{}", il),
            Datum::STR(sl) => write!(f, "\"{}\"", sl),
            Datum::BOOL(bl) => write!(f, "{}", bl),
            Datum::NULL => write!(f, "NULL"),
        }
    }
}
