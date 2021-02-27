// main.rs
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

#[derive(Debug)]
struct TextFileSplit(u64, u64);

use crate::consts::*;
use std::error::Error;
use std::fs;
use std::io::prelude::*;
use std::io::SeekFrom;

fn find_end_of_split(f: &mut fs::File) -> Result<u64, Box<dyn Error>> {
    // Read from current file position, and return offset of newline or EOF.
    let mut buffer: [u8; 64] = [0; 64];
    let mut total_len = 0;

    loop {
        let len = f.read(&mut buffer)?;
        if len == 0 {
            return Ok(total_len as u64); // EOF
        } else if let Some(index) =
            buffer.iter().position(|&ch| ch == '\n' as u8)
        {
            return Ok((total_len + index) as u64); // Found newline
        } else {
            total_len += len; // Not found
        }
    }
}

#[test]
fn test_find_end_of_split() {
    let filename = "test.txt";
    let mut f = fs::File::open(&filename).unwrap();

    let position = find_end_of_split(&mut f).unwrap();
    assert_eq!(position, 5);

    f.seek(SeekFrom::Start(position + 1)).unwrap();
    let position = find_end_of_split(&mut f).unwrap();
    assert_eq!(position, 2);

    f.seek(SeekFrom::Start(position + 1)).unwrap();
    let position = find_end_of_split(&mut f).unwrap();
    println!("position = {}", position);
}

#[test]
fn test() {
    println!("Hello, world!");
    let filename = format!("{}/{}", DATADIR, "emp.csv").to_string();

    for s in compute_splits(&filename, 2) {
        println!("split = {:?}", s);
    }

    println!("Done");
}

use std::fs::File;
use std::io::{self, prelude::*, BufReader};

fn compute_splits(
    filename: &str, nsplits: u64,
) -> Result<Vec<TextFileSplit>, Box<dyn Error>> {
    let mut f = fs::File::open(&filename)?;
    let mut reader = BufReader::new(f);

    let metadata = fs::metadata(filename)?;
    let sz = metadata.len();
    let blk_size = sz / nsplits;
    //let blk_size = 50;

    let mut begin = 0;
    let mut end = 0;
    let mut splits: Vec<TextFileSplit> = Vec::new();
    let mut line = String::new();

    while end < sz {
        end = begin + blk_size;
        if end > sz {
            end = sz;
        } else {
            reader.seek(SeekFrom::Start(end))?;
            line.clear();
            reader.read_line(&mut line)?;
            end += line.len() as u64 + 1;
        }
        splits.push(TextFileSplit(begin, end));
        println!("begin = {}, end = {}", begin, end);
        begin = end;
    }
    Ok(splits)
}
