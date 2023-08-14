// csv

use std::{
    fmt, fs,
    io::{prelude::*, BufReader, SeekFrom},
};

use arrow2::io::csv::{
    read,
    read::{ByteRecord, Reader, ReaderBuilder},
};
use csv::Position;

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, POPContext},
    stage::Stage,
};

/***************************************************************************************************/

pub struct CSVContext {
    pop_key: POPKey,
    fields: Vec<Field>,
    projection: Vec<ColId>,
    reader: Reader<fs::File>,
    rows: Vec<ByteRecord>,
    partition_id: PartitionId,
    partition: TextFilePartition,
}

impl CSVContext {
    pub fn try_new(pop_key: POPKey, csv: &CSV, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let has_headers = if partition_id == 0 { csv.header } else { false };
        let partition = csv.partitions[partition_id];

        let mut reader =
            ReaderBuilder::new().has_headers(has_headers).delimiter(csv.separator as u8).from_path(&csv.pathname).map_err(|err| stringify1(err, &csv.pathname))?;

        // Position iterator to beginning of partition
        if partition_id > 0 {
            let mut pos = Position::new();
            pos.set_byte(partition.0);
            reader.seek(pos).map_err(stringify)?;
        }

        let rows = vec![ByteRecord::default(); CHUNK_SIZE];

        let csvctx = CSVContext { pop_key, fields: csv.fields.clone(), projection: csv.input_projection.clone(), reader, rows, partition_id, partition };

        Ok(Box::new(csvctx))
    }

    pub fn read_rows(&mut self) -> Result<usize, String> {
        let reader = &mut self.reader;
        let rows = &mut self.rows;

        let mut row_number = 0;
        for row in rows.iter_mut() {
            let has_more = reader.read_byte_record(row).map_err(|e| (format!(" at line {}", row_number), Box::new(e))).map_err(stringify)?;
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

    fn next0(&mut self) -> Result<ChunkBox, String> {
        let rows_read = self.read_rows().map_err(stringify)?;

        let rows = &self.rows[..rows_read];
        read::deserialize_batch(rows, &self.fields, Some(&self.projection), 0, read::deserialize_column).map_err(stringify)
    }
}

impl POPContext for CSVContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, _: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let props = stage.pop_graph.get_properties(pop_key);

        let mut chunk = self.next0()?;

        //debug!("Before preds: \n{}", chunk_to_string(&chunk, "Before preds"));

        if !chunk.is_empty() {
            // Run predicates and virtcols, if any
            chunk = POPKey::eval_predicates(props, chunk);
            //debug!("After preds: \n{}", chunk_to_string(&chunk, "After preds"));

            let projection_chunk = POPKey::eval_projection(props, &chunk);
            let headerstr = format!("CSVContext::next Stage = {}, {:?}, Partition = {}", stage.stage_id, pop_key, self.partition_id);
            debug!("{}", chunk_to_string(&projection_chunk, &headerstr));
            Ok(Some(projection_chunk))
        } else {
            Ok(None)
        }
    }
}

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct CSV {
    pub pathname: String,
    pub fields: Vec<Field>,
    pub header: bool,
    pub separator: char,
    pub partitions: Vec<TextFilePartition>,
    pub input_projection: Vec<ColId>,
}

impl CSV {
    pub fn new(pathname: String, fields: Vec<Field>, header: bool, separator: char, npartitions: usize, input_projection: Vec<ColId>) -> CSV {
        let partitions = Self::compute_partitions(&pathname, npartitions as u64).unwrap();

        CSV { pathname, fields, header, separator, partitions, input_projection }
    }

    fn compute_partitions(pathname: &str, nsplits: u64) -> Result<Vec<TextFilePartition>, String> {
        let f = fs::File::open(pathname).map_err(|err| stringify1(err, pathname))?;
        let mut reader = BufReader::new(f);

        let metadata = fs::metadata(pathname).map_err(|err| stringify1(err, pathname))?;
        let sz = metadata.len();
        let blk_size = sz / nsplits;

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
                reader.seek(SeekFrom::Start(end)).map_err(|err| stringify1(err, pathname))?;
                line.clear();
                reader.read_line(&mut line).map_err(|err| stringify1(err, pathname))?;
                end += line.len() as u64;
            }
            splits.push(TextFilePartition(begin, end));
            debug!("   partition-{} offsets = [{}, {})", splits.len(), begin, end);
            begin = end;
        }

        // Kludge: We may not be able to split a CSV into the requisite # of pieces.
        // If that happens, fabricate zero-sized partitions.
        for _ in 0..(nsplits - splits.len() as u64) {
            splits.push(TextFilePartition(0, 0));
        }
        Ok(splits)
    }
}

impl fmt::Debug for CSV {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let pathname = self.pathname.split('/').last().unwrap();
        fmt.debug_struct("").field("file", &pathname).finish()
    }
}
