// row: Representation of N-tuples

use std::{collections::HashMap, fmt};

use lazy_static::lazy_static;

use crate::includes::*;

lazy_static! {
    static ref DATA_TYPE_PAIRS: Vec<(&'static str, DataType)> = vec![("BOOL", DataType::Boolean), ("INT64", DataType::Int64), ("STRING", DataType::Utf8)];
    static ref STR_TO_DATATYPE: HashMap<String, DataType> = {
        let mut m = HashMap::new();
        for (type_str, typ) in DATA_TYPE_PAIRS.iter() {
            m.insert(type_str.to_string(), typ.clone());
        }
        m
    };
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, Ord, Serialize, Deserialize)]
pub enum Datum {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Utf8(String),
    Date32(i32)
    //F64(u64) // Represented as bits
}

impl Datum {
    pub fn try_as_i64(&self) -> Option<i64> {
        if let Int64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn try_as_str(&self) -> Option<&str> {
        if let Utf8(val) = self {
            Some(val)
        } else {
            None
        }
    }

    pub fn try_as_i32(&self) -> Option<i32> {
        match *self {
            Int32(val) => Some(val),
            Date32(val) => Some(val),
            _ => None
        }
    }

    pub fn datatype(&self) -> DataType {
        match self {
            Null => DataType::Null,
            Boolean(_) => DataType::Boolean,
            Int32(_) => DataType::Int32,
            Int64(_) => DataType::Int64,
            Utf8(_) => DataType::UInt64,
            Date32(_) => DataType::Date32
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Null => write!(f, "NULL"),
            Boolean(bl) => write!(f, "{}", bl),
            Int32(il) => write!(f, "{}", il),
            Int64(il) => write!(f, "{}", il),
            Utf8(sl) => write!(f, "\"{}\"", sl),
            Date32(d) => write!(f, "\"{}\"", d),
        }
    }
}
