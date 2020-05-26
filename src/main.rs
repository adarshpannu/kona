// main.rs

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use std::error::{self, Error};
use std::fs;
use std::io::{self, prelude::*, BufReader};
use std::fmt;

mod errors;
use errors::FlareError;

#[derive(Debug, Clone)]
struct ThreadContext {
    thread_id: i32,
    pll_degree: i32,
}

impl ThreadContext {
    fn new(thread_id: i32, pll_degree: i32) -> ThreadContext {
        ThreadContext {
            thread_id,
            pll_degree,
        }
    }
}

trait DataOp<T> {
    /*
    fn map<U, F>(&self, mapfn: F) -> MapOp<T, U, F>
    where
        F: Fn(T) -> U,
    {
        MapOp {
            mapfn: mapfn,
            phantom_t: None,
            phantom_u: None,
        }
    }
    */

    fn has_next(&self) -> bool;
    fn next(&mut self) -> Result<Box<T>, Box<dyn Error>>;
}

/*
use std::marker::PhantomData;

struct MapOp<T, U, F>
where
    F: Fn(T) -> U,
{
    mapfn: F,
    phantom_t: Option<PhantomData<T>>,
    phantom_u: Option<PhantomData<U>>,
}

impl<T, U, F> DataOp<U> for MapOp<T, U, F>
where
    F: Fn(T) -> U,
{
    fn next(&mut self) -> Result<Box<U>, Box<dyn Error>> {
        Err("Please use a vector with at least one element")
    }
}
*/

#[derive(Debug, Clone)]
struct TextFileSplit(u64, u64);

#[derive(Debug)]
struct TextFileOp {
    ctx:            ThreadContext,
    filename:       String,
    splits:         Vec<TextFileSplit>,

    buf_reader:     Option<io::BufReader<fs::File>>,
    cur_offset:     u64,
    end_offset:     u64,
}

use std::ops::DerefMut;

impl DataOp<String> for TextFileOp {
    fn has_next(&self) -> bool {
        self.cur_offset < self.end_offset
    }

    fn next(&mut self) -> Result<Box<String>, Box<dyn Error>> {
        if self.cur_offset < self.end_offset {
            let mut line = String::new();
            let buf_reader = self.buf_reader.as_mut().unwrap();

            buf_reader.read_line(&mut line)?;
            //println!("   >{}<", line.trim_end());
            self.cur_offset += line.len() as u64;
            Ok(Box::new(line))
        } else {
            Err(Box::new(FlareError {}))
        }
    }
}

impl TextFileOp {

    fn new(ctx: ThreadContext, filename: &str) -> TextFileOp {
        let filename = String::from(filename);
        let splits = Self::compute_splits(&filename, ctx.pll_degree as u64).unwrap();
        TextFileOp {ctx, filename, splits, buf_reader: None, cur_offset: 0, end_offset: 0}
    }

    fn open(&mut self) -> Result<(), Box<dyn Error>> {
        let fp = fs::File::open(&self.filename)?;
        let mut buf_reader = BufReader::new(fp);
        let split = &self.splits[self.ctx.thread_id as usize];
        let (cur_offset, end_offset) = (split.0, split.1);
        buf_reader.seek(io::SeekFrom::Start(cur_offset))?;

        self.buf_reader = Some(buf_reader);
        self.cur_offset = cur_offset;
        self.end_offset = end_offset;
        Ok(())
    }

    fn compute_splits(filename: &String, nsplits: u64) -> Result<Vec<TextFileSplit>, Box<dyn Error>> {
        let metadata = fs::metadata(filename)?;
        let sz = metadata.len();
        let f = fs::File::open(filename)?;
        let mut reader = BufReader::new(f);

        let blk_size = sz / nsplits;

        let blk_size = 20;

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
                end += line.len() as u64;
            }
            splits.push(TextFileSplit(begin, end));
            begin = end;
        }
        Ok(splits)
    }
}

fn main() {
    println!("\n------- flare --------");

    let filename = "A-Christmas-Carol.txt";
    let filename = "test.txt";

    let ctx = ThreadContext {
        thread_id: 2,
        pll_degree: 1000,
    };

    let op = TextFileOp::new(ctx.clone(), filename);
        //.map(|line| line)
        //.map(|line| line);

    let mut op = TextFileOp::new(ctx.clone(), filename);
    let e = op.open();
    while op.has_next() {
        let tpl = op.next().unwrap();
        println!("tuple = :{}:", tpl.trim_end());
    }
}

