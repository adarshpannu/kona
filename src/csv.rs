// main.rs
#![allow(warnings)]

use crate::includes::*;

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
        debug!(
            "File {}, partition-{} offsets = [{}, {})",
            filename,
            splits.len(),
            begin,
            end
        );
        begin = end;
    }
    Ok(splits)
}

/*
#[test]
fn test() {
    debug!("Hello, world!");
    let filename = format!("{}/{}", DATADIR, "emp.csv").to_string();

    let partitions = compute_partitions(&filename, 4).unwrap();
    for partition in partitions.iter() {
        debug!("split = {:?}", partition);
    }

    let ptniter = CSVPartitionIter::new(&filename, &partitions[1]);
    for line in ptniter {
        debug!("line = {:?}", line);
    }

    debug!("Done");
}
*/

/***************************************************************************************************/

pub struct CSVDirIter {
    filenames: Vec<std::path::PathBuf>,
    cur_file_offset: usize,
    cur_read: u64,
    reader: Option<io::BufReader<File>>,
}

impl CSVDirIter {
    pub fn new(dirname: &String) -> CSVDirIter {
        let mut filenames = fs::read_dir(dirname)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()
            .unwrap();
        filenames.sort();

        CSVDirIter {
            filenames,
            cur_file_offset: 0,
            cur_read: 0,
            reader: None,
        }
    }
}

impl Iterator for CSVDirIter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.reader.is_none() {
                if self.cur_file_offset >= self.filenames.len() {
                    // Exhausted all files
                    return None;
                } else {
                    // Open first/next file
                    let filename = &self.filenames[self.cur_file_offset];
                    let file =
                        File::open(filename)
                            .unwrap();
                    let mut reader = BufReader::new(file);
                    reader.seek(SeekFrom::Start(0));
                    self.reader = Some(reader);
                }
            }
            if let Some(mut reader) = self.reader.as_mut() {
                let mut line = String::new();
                reader.read_line(&mut line);
                if line.len() > 0 {
                    // Return line read
                    self.cur_read += line.len() as u64;
                    //debug!("Read line: {}", line);
                    return Some(line);
                } else {
                    // Exhausted all lines. Read next file, if any.
                    self.reader = None;
                    self.cur_file_offset += 1;
                    continue;
                }
            } else {
                return None;
            }
        }
    }
}

#[test]
fn test() {
    let dirname = format!("{}/stage-3/consumer-1/", DATADIR);
    let iter = CSVDirIter::new(&dirname);
    for row in iter {
        println!(":{}:", row);
    }
}