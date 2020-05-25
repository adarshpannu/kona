// main.rs

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

#[derive(Debug)]
struct TextFileSplit(u64, u64);

use std::error::Error;
use std::fs;
use std::io::{self, prelude::*, BufReader};

struct ThreadContext {
    thread_id:  i32,
    pll_degree: i32
}

impl ThreadContext {
    fn new(thread_id: i32, pll_degree: i32) -> ThreadContext {
        ThreadContext {thread_id, pll_degree}
    }
}

fn main() {
    let filename = String::from("A-Christmas-Carol.txt");
    let ctx = ThreadContext { thread_id: 0, pll_degree: 6 };

    let op = TextFileOp {ctx: ctx, filename: filename} . 
            map(|line| line);    
}

trait DataOp<T> {
    fn map<U, F>(&self, mapfn: F) -> MapOp<T, U, F> where F: Fn(T) -> U {
        MapOp {mapfn: mapfn, phantom_t: None, phantom_u: None}
    }

    fn next() -> Option<Box<T>>;
}

struct TextFileOp {
    ctx:        ThreadContext, 
    filename:   String
}

impl DataOp<String> for TextFileOp {
    fn next() -> Option<Box<String>> {
        Some(Box::new(String::from("Hello")))
    }
}

use std::marker::PhantomData;

struct MapOp<T, U, F> where F: Fn(T) -> U {
    mapfn: F,
    phantom_t: Option<PhantomData<T>>,
    phantom_u: Option<PhantomData<U>>,
}

/*
impl<T,U,F> DataOp<T> for MapOp<T,U,F> {

    fn next() -> Option<Box<U>> {
        None
    }
}
*/

fn compute_splits(filename: &str, nsplits: u64) -> Result<Vec<TextFileSplit>, Box<dyn Error>> {
    let f = fs::File::open(&filename)?;
    let mut reader = BufReader::new(f);

    let metadata = fs::metadata(filename)?;
    let sz = metadata.len();
    let blk_size = sz / nsplits;

    let mut begin = 0;
    let mut end = 0;
    let mut splits: Vec<TextFileSplit> = Vec::new();
    let mut line = String::new();

    while end < sz {
        end = begin + blk_size;
        if end > sz {
            end = sz;
        } else {
            reader.seek(io::SeekFrom::Start(end))?;
            line.clear();
            reader.read_line(&mut line)?;
            end += line.len() as u64 + 1;
        }
        splits.push(TextFileSplit(begin, end));
        begin = end;
    }
    Ok(splits)
}

