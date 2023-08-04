// pq

use std::{fmt, fs::File};

use arrow2::io::parquet::read::{self, FileReader};

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, POPContext},
    stage::Stage,
};

/***************************************************************************************************/

pub struct ParquetContext {
    pop_key: POPKey,
    file_reader: FileReader<File>,
    //metadata: FileMetaData,
    //schema: Schema,
    //row_groups: RowGroupMetaData,
    partition_id: PartitionId,
}

impl ParquetContext {
    pub fn try_new(pop_key: POPKey, pq: &Parquet, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let mut reader = File::open(&pq.pathname).map_err(stringify)?;
        let metadata = read::read_metadata(&mut reader).map_err(stringify)?;
        let schema = read::infer_schema(&metadata).map_err(stringify)?;
        let schema = schema.filter(|ix, _field| pq.ordered_input_projection.iter().find(|&&jx| ix == jx).is_some());

        let row_groups = metadata.row_groups.into_iter().enumerate().map(|(_, row_group)| row_group).collect::<Vec<_>>();
        let file_reader = read::FileReader::new(reader, row_groups, schema, Some(1024 * 8 * 8), None, None);

        let pqctx = ParquetContext { pop_key, file_reader, partition_id };
        Ok(Box::new(pqctx))
    }
}

impl POPContext for ParquetContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, _: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let props = stage.pop_graph.get_properties(pop_key);

        let chunk = self.file_reader.next();

        if let Some(chunk) = chunk {
            let mut chunk = chunk.map_err(stringify)?;
            debug!("ParquetContext:next(): \n{}", chunk_to_string(&chunk, "ParquetContext:next()"));

            // Run predicates and virtcols, if any
            chunk = POPKey::eval_predicates(props, chunk);

            let projection_chunk = POPKey::eval_projection(props, &chunk);
            let headerstr = format!("ParquetContext::next Stage = {}, {:?}, Partition = {}", stage.stage_id, pop_key, self.partition_id);
            debug!("{}", chunk_to_string(&projection_chunk, &headerstr));
            Ok(Some(projection_chunk))
        } else {
            Ok(None)
        }
    }
}

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct Parquet {
    pub pathname: String,
    pub fields: Vec<Field>,
    pub ordered_input_projection: Vec<ColId>,
}

impl Parquet {
    pub fn new(pathname: String, fields: Vec<Field>, _npartitions: usize, ordered_input_projection: Vec<ColId>) -> Parquet {
        Parquet { pathname, fields, ordered_input_projection }
    }
}

impl fmt::Debug for Parquet {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let pathname = self.pathname.split('/').last().unwrap();
        fmt.debug_struct("").field("file", &pathname).finish()
    }
}
