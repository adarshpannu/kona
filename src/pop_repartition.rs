// pop_repartition

use crate::flow::Flow;
use crate::stage::{Stage, StageLink};
use crate::{graph::POPKey, includes::*, pcode::PCode, pop::chunk_to_string, pop::POPContext, pop::POP};
use arrow2::compute::arithmetics::ArrayRem;
use arrow2::compute::filter::filter_chunk;
use arrow2::compute::hash::hash;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::ipc::write::{FileWriter, WriteOptions};
use getset::Getters;
use self_cell::self_cell;
use std::fs::File;
use std::rc::Rc;

/***************************************************************************************************/
pub struct RepartitionWriteContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    writers: Vec<Option<FileWriter<File>>>,
}

impl RepartitionWriteContext {
    pub fn try_new(
        pop_key: POPKey, rpw: &RepartitionWrite, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId,
    ) -> Result<Box<dyn POPContext>, String> {
        let writers = (0..rpw.cpartitions).map(|_| None).collect();

        Ok(Box::new(RepartitionWriteContext {
            pop_key,
            children,
            partition_id,
            writers,
        }))
    }

    fn eval_repart_keys(repart_code: &[PCode], input: &ChunkBox) -> ChunkBox {
        let arrays = repart_code.iter().map(|code| code.eval(input)).collect();
        Chunk::new(arrays)
    }

    fn hash_chunk(chunk: ChunkBox) -> Result<PrimitiveArray<u64>, String> {
        // FIXME: We hash the first column only. Need to include all columns.
        let last_ix = chunk.columns().len() - 1;
        let arr0 = &chunk.columns()[last_ix];
        hash(&**arr0).map_err(stringify)
    }

    fn compute_partitions(hashed: PrimitiveArray<u64>, npartitions: PartitionId) -> PrimitiveArray<u64> {
        hashed.rem(&(npartitions as u64))
    }

    fn get_writer(&mut self, flow_id: usize, rpw: &RepartitionWrite, cpartition: PartitionId) -> Result<&mut FileWriter<File>, String> {
        if self.writers[cpartition].is_none() {
            let dirname = get_partition_dir(flow_id, rpw.stage_link, cpartition);
            let path = format!("{}/producer-{}.arrow", dirname, self.partition_id);
            std::fs::create_dir_all(dirname).map_err(stringify)?;

            let file = File::create(path).map_err(stringify)?;

            let options = WriteOptions { compression: None };
            let schema = &*rpw.schema.clone();
            let mut writer = FileWriter::new(file, schema.clone(), None, options);
            writer.start().map_err(stringify)?;

            self.writers[cpartition] = Some(writer);
        }
        Ok(self.writers[cpartition].as_mut().unwrap())
    }

    fn finish_writers(&mut self, rpw: &RepartitionWrite) -> Result<(), String> {
        for cpartition in 0..rpw.cpartitions {
            if let Some(writer) = self.writers[cpartition].as_mut() {
                writer.finish().map_err(stringify)?;
            }
        }
        Ok(())
    }

    fn filter_partition(chunk: &ChunkBox, part_array: &PrimitiveArray<u64>, cpartition: PartitionId) -> Result<ChunkBox, String> {
        use arrow2::compute::comparison::primitive::eq_scalar;
        let arr = eq_scalar(part_array, cpartition as u64);
        filter_chunk(chunk, &arr).map_err(stringify)
    }

    fn write_partitions(&mut self, flow_id: usize, rpw: &RepartitionWrite, chunk: ChunkBox, part_array: PrimitiveArray<u64>) -> Result<(), String> {
        let partition_id = self.partition_id;

        for cpartition in 0..rpw.cpartitions {
            // Filter chunk to only grab this partition
            let filtered_chunk = Self::filter_partition(&chunk, &part_array, cpartition)?;
            if !filtered_chunk.is_empty() {
                let writer = self.get_writer(flow_id, rpw, cpartition)?;

                let headerstr = format!(
                    "RepartitionWriteContext Stage {} -> {}, Partition {}, Consumer {}",
                    rpw.stage_link.0, rpw.stage_link.1, partition_id, cpartition
                );
                debug!("{}", chunk_to_string(&filtered_chunk, &headerstr));

                writer.write(&filtered_chunk, None).map_err(stringify)?
            }
        }
        Ok(())
    }
}

impl POPContext for RepartitionWriteContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        let props = stage.pop_graph.get_properties(pop_key);

        if let POP::RepartitionWrite(rpw) = pop {
            let repart_key_code = &rpw.repart_key;

            while let Some(chunk) = self.children[0].next(flow, stage)? {
                if !chunk.is_empty() {
                    let chunk = POPKey::eval_projection(props, &chunk);

                    // Compute partitioning keys
                    let repart_keys = Self::eval_repart_keys(repart_key_code, &chunk);

                    // Compute hash
                    let repart_hash = Self::hash_chunk(repart_keys)?;

                    // Compute partitions
                    let part_array = Self::compute_partitions(repart_hash, rpw.cpartitions);
                    /*
                    debug!(
                        "[{:?}] RepartitionWriteContext partition = {}::cpartitions: \n{:?}",
                        self.pop_key, self.partition_id, part_array
                    );
                    */

                    // Write partitions
                    self.write_partitions(flow.id, rpw, chunk, part_array)?;
                }
            }
            self.finish_writers(rpw)?;
        } else {
            panic!("ugh")
        }
        Ok(None)
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize, Getters)]
pub struct RepartitionWrite {
    #[getset(get = "pub")]
    repart_key: Vec<PCode>,

    #[getset(get = "pub")]
    schema: Rc<Schema>,

    #[getset(get = "pub")]
    cpartitions: PartitionId,

    #[getset(get = "pub")]
    stage_link: StageLink,
}

impl RepartitionWrite {
    pub fn new(repart_key: Vec<PCode>, schema: Rc<Schema>, stage_link: StageLink, cpartitions: PartitionId) -> Self {
        RepartitionWrite {
            repart_key,
            schema,
            stage_link,
            cpartitions,
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize, Getters)]
pub struct RepartitionRead {
    #[getset(get = "pub")]
    schema: Rc<Schema>,

    #[getset(get = "pub")]
    stage_link: StageLink,
}

impl RepartitionRead {
    pub fn new(schema: Rc<Schema>, stage_link: StageLink) -> Self {
        RepartitionRead { schema, stage_link }
    }
}

/***************************************************************************************************/
pub struct RepartitionReadContext {
    pop_key: POPKey,
    partition_id: PartitionId,
    cell: RepartitionReadCell,
}

type ChunkIter<'a> = Box<dyn Iterator<Item = ChunkBox> + 'a>;

self_cell!(
    struct RepartitionReadCell {
        owner: Vec<String>,

        #[covariant]
        dependent: ChunkIter,
    }
);

impl RepartitionReadCell {
    fn next(&mut self) -> Option<ChunkBox> {
        self.with_dependent_mut(|_, dependent| dependent.next())
    }
}

impl RepartitionReadContext {
    pub fn try_new(flow_id: usize, pop_key: POPKey, rpw: &RepartitionRead, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        // Enumerate directory
        let dirname = get_partition_dir(flow_id, rpw.stage_link, partition_id);
        let files = list_files(&dirname);
        let files = if let Err(errstr) = files {
            if !errstr.contains("kind: NotFound") {
                return Err(errstr);
            }
            vec![]
        } else {
            files.unwrap()
        };
        debug!(
            "[{:?}] RepartitionReadContext::new, partition = {}, files = {:?}",
            pop_key, partition_id, &files
        );

        let cell = RepartitionReadCell::new(files, |files| {
            let reader = files.iter().flat_map(|path| {
                debug!("RepartitionReadContext: reading file {}", &path);

                let mut reader = File::open(path).unwrap();
                let metadata = read_file_metadata(&mut reader).unwrap();
                FileReader::new(reader, metadata, None, None).map(|e| e.unwrap())
            });
            let reader: Box<dyn Iterator<Item = ChunkBox>> = Box::new(reader);
            reader
        });

        Ok(Box::new(RepartitionReadContext { pop_key, partition_id, cell }))
    }
}

impl POPContext for RepartitionReadContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, _: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);

        if let POP::RepartitionRead(_) = pop {
            let chunk = self.cell.next();
            if let Some(chunk) = chunk {
                let headerstr = format!(
                    "RepartitionReadContext::next Stage = {}, {:?}, Partition = {}",
                    stage.stage_id, pop_key, self.partition_id
                );
                debug!("{}", chunk_to_string(&chunk, &headerstr));
                return Ok(Some(chunk));
            }
        } else {
            panic!("ugh")
        }
        Ok(None)
    }
}

pub fn list_files(dirname: &String) -> Result<Vec<String>, String> {
    let dir = fs::read_dir(dirname).map_err(|err| stringify1(err, dirname))?;
    let mut pathnames = vec![];
    for entry in dir {
        let entry = entry.map_err(stringify)?;
        let path = entry.path();
        if !path.is_dir() {
            let pathstr = path.into_os_string().into_string().map_err(stringify)?;
            pathnames.push(pathstr)
        }
    }
    Ok(pathnames)
}
