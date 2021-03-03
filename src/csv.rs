// main.rs
#![allow(warnings)]

#[derive(Debug, Clone, Copy)]
pub struct TextFilePartition(u64, u64);

use crate::consts::*;
use std::error::Error;
use std::fs;
use std::io::prelude::*;
use std::io::SeekFrom;

pub struct CSVPartitionIter {
    reader: io::BufReader<File>,
    partition_size: u64,
    cur_read: u64,
}

impl CSVPartitionIter {
    pub fn new(
        filename: &String, partition: &TextFilePartition,
    ) -> CSVPartitionIter {
        let file = File::open(filename).unwrap();
        let mut reader = BufReader::new(file);

        reader.seek(SeekFrom::Start(partition.0));

        CSVPartitionIter {
            reader,
            partition_size: (partition.1 - partition.0),
            cur_read: 0,
        }
    }
}

impl Iterator for CSVPartitionIter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_read >= self.partition_size {
            None
        } else {
            let mut line = String::new();
            self.reader.read_line(&mut line);
            if line.len() > 0 {
                self.cur_read += line.len() as u64;
                Some(line)
            } else {
                None
            }
        }
    }
}

use std::fs::File;
use std::io::{self, prelude::*, BufReader};

pub fn compute_partitions(
    filename: &str, nsplits: u64,
) -> Result<Vec<TextFilePartition>, Box<dyn Error>> {
    let mut f = fs::File::open(&filename)?;
    let mut reader = BufReader::new(f);

    let metadata = fs::metadata(filename)?;
    let sz = metadata.len();
    let blk_size = sz / nsplits;
    //let blk_size = 50;

    let mut begin = 0;
    let mut end = 0;
    let mut splits: Vec<TextFilePartition> = Vec::new();
    let mut line = String::new();

    while end < sz {
        end = begin + blk_size;
        if end > sz {
            end = sz;
        } else {
            reader.seek(SeekFrom::Start(end))?;
            line.clear();
            reader.read_line(&mut line)?;
            end += line.len() as u64;
        }
        splits.push(TextFilePartition(begin, end));
        println!("begin = {}, end = {}", begin, end);
        begin = end;
    }
    Ok(splits)
}

#[test]
fn test() {
    println!("Hello, world!");
    let filename = format!("{}/{}", DATADIR, "emp.csv").to_string();

    let partitions = compute_partitions(&filename, 4).unwrap();
    for partition in partitions.iter() {
        println!("split = {:?}", partition);
    }

    let ptniter = CSVPartitionIter::new(&filename, &partitions[1]);
    for line in ptniter {
        println!("line = {:?}", line);
    }

    println!("Done");
}


