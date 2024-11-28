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

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParquetContext {
    pop_key: POPKey,
    input_projection_final_ordering: Vec<usize>,
    partition_id: PartitionId,

    #[derivative(Debug = "ignore")]
    file_reader: FileReader<File>,
}

impl ParquetContext {
    #[tracing::instrument(fields(pop_key), skip_all)]
    pub fn try_new(pop_key: POPKey, pq: &Parquet, npartitions: usize, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let mut reader = File::open(&pq.pathname).map_err(stringify)?;
        let metadata = read::read_metadata(&mut reader).map_err(stringify)?;
        let schema = read::infer_schema(&metadata).map_err(stringify)?;
        let schema = schema.filter(|ix, _field| pq.input_projection.iter().find(|&&jx| ix == jx).is_some());

        //let row_groups = metadata.row_groups.into_iter().enumerate().map(|(_, row_group)| row_group).collect::<Vec<_>>();

        // Partition selection (temporary)
        let n_row_groups = metadata.row_groups.len();
        let npartitions = n_row_groups.min(npartitions);
        let n_row_groups_per_partition = n_row_groups / npartitions;

        //dbg!(n_row_groups);
        //dbg!(npartitions);
        //dbg!(n_row_groups_per_partition);

        let (lo, mut hi) = (n_row_groups_per_partition * partition_id, n_row_groups_per_partition * (partition_id + 1));
        if partition_id == npartitions - 1 {
            hi = n_row_groups;
        }
        //dbg!((lo, hi));

        let row_groups = metadata.row_groups.into_iter().enumerate().filter(|&(ix, _)| lo <= ix && ix < hi).map(|(_, row_group)| row_group).collect::<Vec<_>>();
        debug!("partition_id = {}, # metadata.row_groups {}, # row_groups {:?}", partition_id, n_row_groups, row_groups.len());

        let file_reader = read::FileReader::new(reader, row_groups, schema, Some(1024 * 8 * 8), None, None);

        let mut input_projection_pairs: Vec<(ColId, usize)> = pq.input_projection.iter().cloned().enumerate().collect::<Vec<_>>();
        input_projection_pairs.sort_by(|a, b| a.1.cmp(&b.1));

        let input_projection_final_ordering: Vec<usize> = input_projection_pairs.iter().map(|e| e.0).collect();

        let pqctx = ParquetContext { pop_key, input_projection_final_ordering, file_reader, partition_id };
        debug!("input_projection {:?}", pq.input_projection);

        debug!("{:?}", pqctx);

        Ok(Box::new(pqctx))
    }
}

impl POPContext for ParquetContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[tracing::instrument(fields(stage_id = stage.stage_id, pop_key = %self.pop_key, partition_id = self.partition_id), skip_all, parent = None)]
    fn next(&mut self, _: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let props = stage.pop_graph.get_properties(pop_key);

        let chunk = self.file_reader.next();

        if let Some(chunk) = chunk {
            let chunk = chunk.map_err(stringify)?;

            //debug!("ParquetContext:next(): \n{}", chunk_to_string(&chunk, "ParquetContext:next before reorder"));

            // Parquet readers read columns by ordinal # but the input projection could be unordered
            // We need to re-build the chunk based on unordered input projection.
            let mut arrays = chunk.into_arrays().into_iter().zip(self.input_projection_final_ordering.iter()).collect::<Vec<_>>();
            arrays.sort_by(|a, b| a.1.cmp(&b.1));
            let arrays = arrays.into_iter().map(|(a, _)| a).collect::<Vec<_>>();

            let chunk = Chunk::new(arrays);

            #[cfg(debug_assertions)]
            if !chunk.is_empty() {
                debug!(input = 0, "{}", chunk_to_string(&chunk, "input"));
            }

            // Compute predicates, if any
            let chunk = POPKey::eval_predicates(props, chunk);

            #[cfg(debug_assertions)]
            if !chunk.is_empty() {
                debug!(filtered = 1, "{}", chunk_to_string(&chunk, "filtered"));
            }

            // Project and return
            let chunk = POPKey::eval_projection(props, &chunk);

            #[cfg(debug_assertions)]
            if !chunk.is_empty() {
                //let headerstr = format!("ParquetContext::next Stage = {}, {:?}, Partition = {}", stage.stage_id, pop_key, self.partition_id);
                debug!(projected = 2, "{}", chunk_to_string(&chunk, ""));
            }
            Ok(Some(chunk))
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
    pub input_projection: Vec<ColId>,
}

impl Parquet {
    pub fn new(pathname: String, fields: Vec<Field>, _npartitions: usize, input_projection: Vec<ColId>) -> Parquet {
        Parquet { pathname, fields, input_projection }
    }
}

impl fmt::Debug for Parquet {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pathname = self.pathname.split('/').last().unwrap();
        fmt.debug_struct("").field("file", &pathname).finish()
    }
}
