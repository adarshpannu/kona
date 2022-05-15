// includes.rs
pub use log::{error, warn, info, debug};
pub use std::fs;

//pub const DATADIR: &str = "/Users/adarshrp/Projects/flare/data";
pub const DATADIR: &str = "/Users/adarshrp/Projects/tpch-data/sf0.01";
pub const TEMPDIR: &str = "/Users/adarshrp/Projects/flare/tmp";
pub const GRAPHVIZDIR: &str = "/Users/adarshrp/Projects/flare";

pub type ColId = usize;
pub type QunId = usize;
pub type QBId = usize;
pub type PartitionId = usize;
pub type RegisterId = usize;
pub type StageId = usize;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QunCol(pub QunId, pub ColId);

pub type ColMap = std::collections::HashMap<ColId, ColId>;

pub use crate::Env;

pub use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TextFilePartition(pub u64, pub u64);

pub use std::mem::replace;

pub fn enquote(s: &String) -> String {
    format!("\"{}\"", s)
}

pub fn remove_quotes(s: &str) -> String {
    s.replace("\"", "")
}

pub fn stringify<E: std::fmt::Debug>(e: E) -> String {
    format!("{:?}", e)
}

pub fn stringify1<E: std::fmt::Debug, P: std::fmt::Debug>(e: E, param1: P) -> String {
    format!("{:?}: {:?}", e, param1)
}

pub fn yes_or_no(s: &str) -> Option<bool> {
    match s {
        "Y" | "YES" => Some(true),
        "N" | "NO" => Some(false),
        _ => None
    }
}

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes()).map_err(stringify)?;
    }};
}
pub(crate) use fprint;

pub fn list_files(dirname: &String) -> Result<Vec<String>, String> {
    let dir = fs::read_dir(dirname).map_err(|err| stringify1(err, &dirname))?;
    let mut pathnames = vec![];
    for entry in dir {
        let entry = entry.map_err(stringify)?;
        let path = entry.path();
        if !path.is_dir() {
            let pathstr = path.into_os_string().into_string().map_err(stringify)?;
            pathnames.push(pathstr)
        }
    }
    Ok(pathnames)
}

use std::io::{self, BufRead};
use std::fs::File;
use std::path::Path;

pub fn read_lines<P>(pathname: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(pathname)?;
    Ok(io::BufReader::new(file).lines())
}
