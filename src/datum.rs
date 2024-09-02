// row: Representation of N-tuples

use std::{collections::HashMap, fmt};

use lazy_static::lazy_static;

use crate::includes::*;

#[allow(dead_code)]
pub struct DataTypeDesc<'a> {
    name: &'a str,
    is_numeric: bool,
    rank: usize,
}

impl<'a> DataTypeDesc<'a> {
    fn new(name: &'a str, is_numeric: bool, rank: usize) -> Self {
        DataTypeDesc { name, is_numeric, rank }
    }
}

lazy_static! {
    pub static ref DATATYPE_PROPS: HashMap<DataType, DataTypeDesc<'static>> = {
        let mut map = HashMap::new(); // datatype -> (name, is_numeric, rank)
        let type_metadata = vec![
            (DataType::Int8, DataTypeDesc::new("INT8", true, 1)),
            (DataType::Int16, DataTypeDesc::new("INT16", true, 2)),
            (DataType::Int32, DataTypeDesc::new("INT32", true, 3)),
            (DataType::Int64, DataTypeDesc::new("INT64", true, 4)),
            (DataType::UInt8, DataTypeDesc::new("UINT8", true, 5)),
            (DataType::UInt16, DataTypeDesc::new("UINT16", true, 6)),
            (DataType::UInt32, DataTypeDesc::new("UINT32", true, 7)),
            (DataType::UInt64, DataTypeDesc::new("UINT64", true, 8)),
            (DataType::Float16, DataTypeDesc::new("FLOAT16", true, 9)),
            (DataType::Float32, DataTypeDesc::new("FLOAT32", true, 10)),
            (DataType::Float64, DataTypeDesc::new("FLOAT64", true, 11)),
            (DataType::Date32, DataTypeDesc::new("DATE32", true, 12)),
            (DataType::Date64, DataTypeDesc::new("DATE64", true, 13)),
            (DataType::Decimal(15, 2), DataTypeDesc::new("DECIMAL(15,2)", true, 13)),
        ];
        for (typ, metadata) in type_metadata.into_iter() {
            map.insert(typ, metadata);
        }
        map
    };

    pub static ref STR_TO_DATATYPE: HashMap<String, DataType> = {
        let mut map = HashMap::new();
        for (typ, type_str) in DATATYPE_PROPS.iter() {
            map.insert(type_str.name.to_string(), typ.clone());
        }
        map
    };
}

pub fn is_numeric(dt: &DataType) -> bool {
    let metadata = DATATYPE_PROPS.get(dt);
    if let Some(metadata) = metadata {
        metadata.is_numeric
    } else {
        panic!("is_numeric() passed invalid datatype")
    }
}

pub fn to_datatype(name: &str) -> Option<&DataType> {
    let name = name.to_uppercase();
    STR_TO_DATATYPE.get(&name)
}

pub fn get_rank(dt: &DataType) -> usize {
    let metadata = DATATYPE_PROPS.get(dt);
    if let Some(metadata) = metadata {
        metadata.rank
    } else {
        panic!("get_rank() passed invalid datatype")
    }
}

// Cast
// Ref: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-type-conversion-database-engine?view=sql-server-ver16
#[derive(Debug, Clone, Copy)]
pub enum CastResult {
    CannotCast,
    Implicit,
    Explicit,
}

pub fn check_castability(from: &DataType, to: &DataType) -> CastResult {
    if *from == *to {
        CastResult::Implicit
    } else if is_numeric(from) && is_numeric(to) {
        CastResult::Explicit
    } else {
        CastResult::CannotCast
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct F64(u64);
impl From<f64> for F64 {
    fn from(value: f64) -> Self {
        F64(value.to_bits())
    }
}

impl From<F64> for f64 {
    fn from(value: F64) -> Self {
        f64::from_bits(value.0)
    }
}

impl fmt::Display for F64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let f64_value = f64::from(*self);
        write!(f, "{}", f64_value)
    }
}

impl fmt::Debug for F64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let f64_value = f64::from(*self);
        write!(f, "{} ({})", f64_value, self.0)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, Ord, Serialize, Deserialize)]
pub enum Datum {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Utf8(String),
    Date32(i32),
    Float64(F64),
}

impl Datum {
    #[inline]
    pub fn try_as_i64(&self) -> Option<i64> {
        if let Int64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn add_i64(&mut self, other: i64) {
        if let Int64(val) = self {
            *val += other
        } else {
            panic!("add_i64: Datum {:?} does not hold Int64", self)
        }
    }

    #[inline]
    pub fn try_as_f64(&self) -> Option<f64> {
        if let Float64(val) = self {
            Some(f64::from(*val))
        } else {
            None
        }
    }

    #[inline]
    pub fn add_f64(&mut self, other: f64) {
        if let Float64(val) = self {
            let newval = f64::from(*val) + other;
            *val = F64::from(newval);
        } else {
            panic!("add_f64: Datum {:?} does not hold F64", self)
        }
    }

    #[inline]
    pub fn try_as_str(&self) -> Option<&str> {
        if let Utf8(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_as_i32(&self) -> Option<i32> {
        match *self {
            Int32(val) => Some(val),
            Date32(val) => Some(val),
            _ => None,
        }
    }

    pub fn datatype(&self) -> DataType {
        match self {
            Null => DataType::Null,
            Boolean(_) => DataType::Boolean,
            Int32(_) => DataType::Int32,
            Int64(_) => DataType::Int64,
            Utf8(_) => DataType::UInt64,
            Date32(_) => DataType::Date32,
            Float64(_) => DataType::Float64,
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Null => write!(f, "NULL"),
            Boolean(bl) => write!(f, "{}", bl),
            Int32(il) => write!(f, "{}", il),
            Int64(il) => write!(f, "{}", il),
            Utf8(sl) => write!(f, "\"{}\"", sl),
            Date32(d) => write!(f, "\"{}\"", d),
            Float64(value) => write!(f, "{:?}", Into::<F64>::into(*value)),
        }
    }
}
