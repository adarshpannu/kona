// pop_repartition

use crate::{includes::*, pcode::PCode, pop::POPProps, task::Task};
use arrow2::io::ipc::write;
use getset::Getters;
use std::fs::File;
use std::rc::Rc;

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize, Getters)]
pub struct RepartitionWrite {
    #[getset(get = "pub")]
    repart_key: Vec<PCode>,

    #[getset(get = "pub")]
    schema: Rc<Schema>,
}

impl RepartitionWrite {
    pub fn new(repart_key: Vec<PCode>, schema: Rc<Schema>) -> Self {
        RepartitionWrite { repart_key, schema }
    }

    pub fn next(&self, _task: &mut Task, _props: &POPProps) -> Result<ChunkBox, String> {
        debug!("RepartitionWrite:next():");

        todo!()
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct RepartitionWriteContext {


}

impl RepartitionWriteContext {
    pub fn new(_rpw: &RepartitionWrite) -> Self {
        RepartitionWriteContext {}
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

    pub fn next(&self, _task: &mut Task, _props: &POPProps) -> Result<ChunkBox, String> {
        debug!("RepartitionRead:next():");

        todo!()
    }
}

fn write_batches(path: &str, schema: Schema, chunks: &[Chunk<Box<dyn Array>>]) -> A2Result<()> {
    let file = File::create(path)?;

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(file, schema, None, options);

    writer.start()?;
    for chunk in chunks {
        writer.write(chunk, None)?
    }
    writer.finish()
}
