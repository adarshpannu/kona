// pop_repartition

use crate::{flow::Flow, graph::POPKey, includes::*, pcode::PCode, pop::chunk_to_string, pop::POPContext};
use arrow2::io::ipc::write::{FileWriter, WriteOptions};
use getset::Getters;
use std::fs::File;
use std::rc::Rc;

/***************************************************************************************************/
pub struct RepartitionWriteContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
}

impl RepartitionWriteContext {
    pub fn new(
        pop_key: POPKey, _rpw: &RepartitionWrite, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId,
    ) -> Result<Box<dyn POPContext>, String> {
        Ok(Box::new(RepartitionWriteContext {
            pop_key,
            children,
            partition_id,
        }))
    }

    /*
    fn hash_chunk(chunk: ChunkBox, pcode: &Vec<PCode>) -> Result<Chunk<Box<dyn Array>>, String> {
        todo!()
    }
    */
}

impl POPContext for RepartitionWriteContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow) -> Result<Chunk<Box<dyn Array>>, String> {
        let child = &mut self.children[0];
        //let pop_key = self.pop_key;
        //let node = flow.pop_graph.get_value(pop_key);

        loop {
            let chunk = child.next(flow)?;
            if chunk.len() == 0 {
                break;
            }

            //let chunk = Self::hash_chunk(chunk)?;

            debug!("RepartitionWriteContext {:?}::next: \n{}", self.pop_key, chunk_to_string(&chunk));
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
    cpartitions: usize,
}

impl RepartitionWrite {
    pub fn new(repart_key: Vec<PCode>, schema: Rc<Schema>, cpartitions: usize) -> Self {
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

fn _write_batches(path: &str, schema: Schema, chunks: &[Chunk<Box<dyn Array>>]) -> A2Result<()> {
    let file = File::create(path)?;

    let options = WriteOptions { compression: None };
    let mut writer = FileWriter::new(file, schema, None, options);

    writer.start()?;
    for chunk in chunks {
        writer.write(chunk, None)?
    }
    writer.finish()
}
