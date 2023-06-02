// csv

use crate::{
    includes::*,
    pop::{POPContext, POPProps},
    task::Task,
};
use arrow2::io::csv::{
    read,
    read::{ByteRecord, Reader, ReaderBuilder},
};
use csv::Position;
use std::{
    fmt, fs,
    io::{prelude::*, BufReader, SeekFrom},
};

pub struct CSVContext {
    iter: CSVPartitionIter,
}

impl CSVContext {
    pub fn new(iter: CSVPartitionIter) -> Box<dyn POPContext> {
        Box::new(CSVContext { iter })
    }
}

impl POPContext for CSVContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct CSVPartitionIter {
    fields: Vec<Field>,
    projection: Vec<ColId>,
    reader: Reader<fs::File>,
    rows: Vec<ByteRecord>,
    partition: TextFilePartition,
}

impl CSVPartitionIter {
    pub fn new(csv: &CSV, partition_id: PartitionId) -> Result<CSVPartitionIter, String> {
        let has_headers = if partition_id == 0 { csv.header } else { false };
        let partition = csv.partitions[partition_id];

        let mut reader = ReaderBuilder::new()
            .has_headers(has_headers)
            .delimiter(csv.separator as u8)
            .from_path(&csv.pathname)
            .map_err(|err| stringify1(err, &csv.pathname))?;

        // Position iterator to beginning of partition
        if partition_id > 0 {
            //let mut pos = csv::Position::new();
            let mut pos = Position::new();
            pos.set_byte(partition.0);
            reader.seek(pos).map_err(stringify)?;
        }

        let rows = vec![ByteRecord::default(); CHUNK_SIZE];

        Ok(CSVPartitionIter {
            fields: csv.fields.clone(),
            projection: csv.projection.clone(),
            reader,
            rows,
            partition,
        })
    }

    pub fn read_rows(&mut self) -> Result<usize, String> {
        let reader = &mut self.reader;
        let rows = &mut self.rows;

        let mut row_number = 0;
        for row in rows.iter_mut() {
            let has_more = reader
                .read_byte_record(row)
                .map_err(|e| (format!(" at line {}", row_number), Box::new(e)))
                .map_err(stringify)?;
            let pos = reader.position();
            if pos.byte() > self.partition.1 {
                break;
            }
            if !has_more {
                break;
            }
            row_number += 1;
        }
        Ok(row_number)
    }
}

impl Iterator for CSVPartitionIter {
    type Item = Chunk<Box<dyn Array>>;

    fn next(&mut self) -> Option<Self::Item> {
        // allocate space to read from CSV to. The size of this vec denotes how many rows are read.

        // skip 0 (excluding the header) and read up to 100 rows.
        // this is IO-intensive and performs minimal CPU work. In particular,
        // no deserialization is performed.
        let rows_read = self.read_rows().map_err(|err| stringify(err)).ok();
        if rows_read.is_none() {
            return None;
        }

        let rows = &self.rows[..rows_read.unwrap()];

        // parse the rows into a `Chunk`. This is CPU-intensive, has no IO,
        // and can be performed on a different thread by passing `rows` through a channel.
        // `deserialize_column` is a function that maps rows and a column index to an Array
        let cols = read::deserialize_batch(rows, &self.fields, Some(&self.projection), 0, read::deserialize_column)
            .map_err(|err| stringify(err))
            .ok();
        //dbg!(&cols);
        cols
    }
}

pub fn compute_partitions(pathname: &str, nsplits: u64) -> Result<Vec<TextFilePartition>, String> {
    let f = fs::File::open(&pathname).map_err(|err| stringify1(err, &pathname))?;
    let mut reader = BufReader::new(f);

    let metadata = fs::metadata(pathname).map_err(|err| stringify1(err, &pathname))?;
    let sz = metadata.len();
    let blk_size = sz / nsplits;
    //let blk_size = 50;

    let mut begin = 0;
    let mut end = 0;
    let mut splits: Vec<TextFilePartition> = Vec::new();
    let mut line = String::new();

    debug!("Compute partitions for file {}", pathname);
    while end < sz {
        end = begin + blk_size;
        if end > sz {
            end = sz;
        } else {
            reader.seek(SeekFrom::Start(end)).map_err(|err| stringify1(err, &pathname))?;
            line.clear();
            reader.read_line(&mut line).map_err(|err| stringify1(err, &pathname))?;
            end += line.len() as u64;
        }
        splits.push(TextFilePartition(begin, end));
        debug!("   partition-{} offsets = [{}, {})", splits.len(), begin, end);
        begin = end;
    }
    Ok(splits)
}

/*
#[test]
fn test() {
    debug!("Hello, world!");
    let pathname = format!("{}/{}", DATADIR, "emp.csv").to_string();

    let partitions = compute_partitions(&pathname, 4).unwrap();
    for partition in partitions.iter() {
        debug!("split = {:?}", partition);
    }

    let ptniter = CSVPartitionIter::new(&pathname, &partitions[1]);
    for line in ptniter {
        debug!("line = {:?}", line);
    }

    debug!("Done");
}
*/

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct CSV {
    pub pathname: String,
    pub fields: Vec<Field>,
    pub header: bool,
    pub separator: char,
    pub partitions: Vec<TextFilePartition>,
    pub projection: Vec<ColId>,
}

impl CSV {
    pub fn new(pathname: String, fields: Vec<Field>, header: bool, separator: char, npartitions: usize, projection: Vec<ColId>) -> CSV {
        let partitions = compute_partitions(&pathname, npartitions as u64).unwrap();

        CSV {
            pathname,
            fields,
            header,
            separator,
            partitions,
            projection,
        }
    }

    pub fn next(&self, task: &mut Task, props: &POPProps) -> Result<Chunk<Box<dyn Array>>, String> {
        let context = &mut task.contexts[props.index_in_stage];
        let csv_context = context.as_any_mut().downcast_mut::<CSVContext>().expect("Wasn't a CSVContext!");
        let iter = &mut csv_context.iter;

        return iter.next().ok_or("CSV::next() failed!".to_string())
    }
}

impl fmt::Debug for CSV {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let pathname = self.pathname.split("/").last().unwrap();
        fmt.debug_struct("").field("file", &pathname).finish()
    }
}
