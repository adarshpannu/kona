// pop_repartition

use crate::{graph::POPKey, includes::*, pcode::PCode, pop::POPContext, flow::Flow};
use arrow2::io::ipc::write;
use getset::Getters;
use std::fs::File;
use std::rc::Rc;

/***************************************************************************************************/
pub struct RepartitionWriteContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
}

impl RepartitionWriteContext {
    pub fn new(pop_key: POPKey, _rpw: &RepartitionWrite, children: Vec<Box<dyn POPContext>>) -> Result<Box<dyn POPContext>, String> {
        Ok(Box::new(RepartitionWriteContext { pop_key, children }))
    }
}


impl POPContext for RepartitionWriteContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow) -> Result<Chunk<Box<dyn Array>>, String> {
        let child = &mut self.children[0];
        let chunk = child.next(flow)?;

        debug!("Repartition::next: {:?}", &chunk);
        Ok(chunk)
    }
}

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

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(file, schema, None, options);

    writer.start()?;
    for chunk in chunks {
        writer.write(chunk, None)?
    }
    writer.finish()
}
