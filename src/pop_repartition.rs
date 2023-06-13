// pop_repartition

use crate::{flow::Flow, graph::POPKey, includes::*, pcode::PCode, pop::chunk_to_string, pop::POPContext, pop::POP};
use arrow2::compute::arithmetics::ArrayRem;
use arrow2::compute::filter::filter_chunk;
use arrow2::compute::hash::hash;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::ipc::write::{FileWriter, WriteOptions};
use getset::Getters;
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
    pub fn new(pop_key: POPKey, rpw: &RepartitionWrite, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let writers = (0..rpw.cpartitions).map(|_| None).collect();

        Ok(Box::new(RepartitionWriteContext {
            pop_key,
            children,
            partition_id,
            writers,
        }))
    }

    fn eval_repart_keys(repart_code: &Vec<PCode>, input: &ChunkBox) -> ChunkBox {
        let arrays = repart_code.iter().map(|code| code.eval(&input)).collect();
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
            let dirname = get_partition_dir(flow_id, self.pop_key, cpartition);
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
        for cpartition in 0..rpw.cpartitions {
            // Filter chunk to only grab this partition
            let filtered_chunk = Self::filter_partition(&chunk, &part_array, cpartition)?;
            if filtered_chunk.len() > 0 {
                let writer = self.get_writer(flow_id, rpw, cpartition)?;
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

    fn next(&mut self, flow: &Flow) -> Result<Chunk<Box<dyn Array>>, String> {
        let pop_key = self.pop_key;
        let pop = flow.pop_graph.get_value(pop_key);
        if let POP::RepartitionWrite(rpw) = pop {
            let repart_key_code = &rpw.repart_key;

            loop {
                let child = &mut self.children[0];
                let chunk = child.next(flow)?;
                if chunk.len() == 0 {
                    break;
                }

                debug!(
                    "RepartitionWriteContext {:?} partition = {}::child chunk: \n{}",
                    self.pop_key,
                    self.partition_id,
                    chunk_to_string(&chunk)
                );

                // Compute partitioning keys
                let repart_keys = Self::eval_repart_keys(&repart_key_code, &chunk);
                debug!(
                    "RepartitionWriteContext {:?} partition = {}::repart_keys: \n{}",
                    self.pop_key,
                    self.partition_id,
                    chunk_to_string(&repart_keys)
                );

                // Compute hash
                let repart_hash = Self::hash_chunk(repart_keys)?;

                // Compute partitions
                let part_array = Self::compute_partitions(repart_hash, rpw.cpartitions);
                debug!(
                    "RepartitionWriteContext {:?} partition = {}::cpartitions: \n{:?}",
                    self.pop_key, self.partition_id, part_array
                );

                // Write partitions
                self.write_partitions(flow.id, rpw, chunk, part_array)?;
            }
            self.finish_writers(rpw)?;
        } else {
            panic!("ugh")
        }

        Ok(Chunk::new(vec![]))
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
}

impl RepartitionWrite {
    pub fn new(repart_key: Vec<PCode>, schema: Rc<Schema>, cpartitions: PartitionId) -> Self {
        RepartitionWrite {
            repart_key,
            schema,
            cpartitions,
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize, Getters)]
pub struct RepartitionRead {
    #[getset(get = "pub")]
    schema: Rc<Schema>,
}

impl RepartitionRead {
    pub fn new(schema: Rc<Schema>) -> Self {
        RepartitionRead { schema }
    }
}

pub struct RepartitionReadContext {
    pop_key: POPKey,
    pop_key_of_writer: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    files: Vec<String>,
    //reader: Box<dyn Iterator<Item = ChunkBox>>,
}

impl RepartitionReadContext {
    pub fn new(
        flow_id: usize, pop_key: POPKey, pop_key_of_writer: POPKey, rpw: &RepartitionRead, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId,
    ) -> Result<Box<dyn POPContext>, String> {
        // Enumerate directory
        let dirname = get_partition_dir(flow_id, pop_key_of_writer, partition_id);
        let files = list_files(&dirname);
        let files = if let Err(errstr) = files {
            if errstr.find("kind: NotFound").is_none() {
                return Err(errstr);
            }
            vec![]
        } else {
            files.unwrap()
        };

        /*
        let reader = files.iter().flat_map(|path| {
            let mut reader = File::open(&path).unwrap();
            let metadata = read_file_metadata(&mut reader).unwrap();
            let mut reader = FileReader::new(reader, metadata, None, None);
            reader.next().unwrap()
        });
        let reader: Box<dyn Iterator<Item = ChunkBox>> = Box::new(reader);
        */

        debug!("RepartitionReadContext::new, partition = {}, files = {:?}", partition_id, &files);

        Ok(Box::new(RepartitionReadContext {
            pop_key,
            pop_key_of_writer,
            children,
            partition_id,
            files,
            //reader,
        }))
    }
}

impl POPContext for RepartitionReadContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow) -> Result<Chunk<Box<dyn Array>>, String> {
        let pop_key = self.pop_key;
        let pop = flow.pop_graph.get_value(pop_key);

        /*
        if let POP::RepartitionRead(rpw) = pop {
            todo!()
        } else {
            panic!("ugh")
        }
        */

        Ok(Chunk::new(vec![]))
    }
}

fn get_partition_dir(flow_id: usize, pop_key: POPKey, pid: PartitionId) -> String {
    format!("{}/flow-{}/stage-{}/consumer-{}", TEMPDIR, flow_id, pop_key.printable_id(), pid)
}

pub fn list_files(dirname: &String) -> Result<Vec<String>, String> {
    let dir = fs::read_dir(dirname).map_err(|err| stringify1(err, &dirname))?;
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
