// pop: Physical operators

use std::{
    collections::HashMap,
    io::{self, Write},
};

use arrow2::io::csv::write;

use crate::{
    expr::AggType,
    flow::Flow,
    graph::{ExprKey, Graph, POPKey},
    includes::*,
    pcode::PCode,
    pop_csv::CSV,
    pop_hashmatch::HashMatch,
    pop_repartition::{RepartitionRead, RepartitionWrite},
    stage::Stage,
};

pub type POPGraph = Graph<POPKey, POP, POPProps>;

/***************************************************************************************************/

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Agg {
    agg_type: AggType,
    colid: ColId,
}

/***************************************************************************************************/
#[derive(Debug, Eq, PartialEq, Hash)]
pub enum Projection {
    QunCol(QunCol),
    VirtCol(ExprKey),
    AggCol(Agg), // SUM($3.2) == Agg(`SUM`, 2)
}

/***************************************************************************************************/
#[derive(Debug, Default)]
pub struct ProjectionMap {
    pub hashmap: HashMap<Projection, ColId>, // Projection -> ColId
}

impl ProjectionMap {
    pub fn set(&mut self, prj: Projection, colid: ColId) {
        self.hashmap.insert(prj, colid);
    }

    pub fn get(&self, prj: Projection) -> Option<ColId> {
        self.hashmap.get(&prj).cloned()
    }

    pub fn append(mut self, other: ProjectionMap) -> Self {
        let offset = self.hashmap.len();
        for (prj, colid) in other.hashmap.into_iter() {
            let colid = colid + offset;
            self.set(prj, colid);
        }
        self
    }

    pub fn set_agg(&mut self, agg_type: AggType, colid: ColId) -> ColId {
        let prj = Projection::AggCol(Agg { agg_type, colid });
        let next_colid = self.hashmap.len();
        let retval = self.hashmap.entry(prj).or_insert(next_colid);
        *retval
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct POPProps {
    pub predicates: Option<Vec<PCode>>,
    pub cols: Option<Vec<ColId>>,
    pub virtcols: Option<Vec<PCode>>,
    pub npartitions: usize,
}

impl POPProps {
    pub fn new(predicates: Option<Vec<PCode>>, cols: Option<Vec<ColId>>, virtcols: Option<Vec<PCode>>, npartitions: usize) -> POPProps {
        POPProps {
            predicates,
            cols,
            virtcols,
            npartitions,
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub enum POP {
    CSV(CSV),
    HashMatch(HashMatch),
    RepartitionWrite(RepartitionWrite),
    RepartitionRead(RepartitionRead),
}

/***************************************************************************************************/
pub trait POPContext {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String>;
}

struct VecWriter {
    buffer: Vec<u8>,
}

impl VecWriter {
    fn new() -> VecWriter {
        VecWriter { buffer: Vec::new() }
    }

    fn into_string(self) -> String {
        String::from_utf8(self.buffer).unwrap()
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn chunk_to_string(chunk: &ChunkBox, header: &str) -> String {
    let mut writer = VecWriter::new();
    let options = write::SerializeOptions::default();
    writer.write_fmt(format_args!("\n---------- {} ----------\n", header)).unwrap();
    write::write_chunk(&mut writer, chunk, &options).unwrap();
    //writer.write(b"--------------------\n").unwrap();
    writer.into_string()
}

pub fn chunk_to_tabularstring(chunk: &ChunkBox, header: &str) -> String {
    let mut writer = VecWriter::new();
    let options = write::SerializeOptions::default();
    write::write_chunk(&mut writer, chunk, &options).unwrap();
    let s = writer.into_string();

    let mut writer = VecWriter::new();

    let rows = s.split('\n').collect::<Vec<_>>();
    for (rx, row) in rows.iter().enumerate() {
        let cols = row.split(',').collect::<Vec<_>>();
        if rx == 0 || rx == rows.len() - 1 {
            let len = rows[0].split(',').collect::<Vec<_>>().len();
            for _ in 0..len {
                writer.write_fmt(format_args!("{:10}", "+----------")).unwrap();
            }
            writer.write(b"+\n").unwrap();
        }
        if rx < rows.len() - 1 {
            for (cx, col) in cols.iter().enumerate() {
                writer.write_fmt(format_args!("|{:10}", col)).unwrap();
            }
            writer.write(b"|\n").unwrap();
        }
    }
    writer.into_string()
}
