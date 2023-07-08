// task

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_tabularstring, POPContext, POP},
    pop_csv::CSVContext,
    pop_hashmatch::HashMatchContext,
    pop_repartition::{RepartitionReadContext, RepartitionWriteContext},
    stage::Stage,
};
use arrow2::io::csv::write;
use std::fs::File;

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub partition_id: PartitionId,

    #[serde(skip)]
    pub contexts: Vec<Box<dyn POPContext>>,
}

// Tasks write to flow-id / top-id / dest-part-id / source-part-id
impl Task {
    pub fn new(partition_id: PartitionId) -> Task {
        Task {
            partition_id,
            contexts: vec![],
        }
    }

    pub fn run(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        let options = write::SerializeOptions::default();

        let mut writer = None;

        if stage.stage_id == 0 {
            let dirname = get_output_dir(flow.id);
            std::fs::create_dir_all(&dirname).map_err(stringify)?;
        }
        /*
        debug!(
            "Running task: stage = {:?}, partition = {}/{}",
            stage.root_pop_key, self.partition_id, stage.npartitions_producer
        );
        */
        let root_pop_key = stage.root_pop_key.unwrap();

        let mut root_context = self.init_context(flow, stage, root_pop_key)?;
        while let chunk = root_context.next(flow, stage)? {
            if let Some(chunk) = chunk {
                if stage.stage_id == 0 {
                    if writer.is_none() {
                        // Tasks in top-level stages write their outputs to disk
                        let dirname = get_output_dir(flow.id);
                        std::fs::create_dir_all(&dirname).map_err(stringify)?;
                        let path = format!("{}/partition-{}.csv", dirname, self.partition_id);
                        let localwriter = std::fs::File::create(path).map_err(stringify)?;
                        writer = Some(localwriter);
                    }

                    if let Some(writer) = writer.as_mut() {
                        write::write_chunk(writer, &chunk, &options);
                    }
                }
            } else {
                break
            }
        }
        Ok(())
    }

    pub fn init_context(&self, flow: &Flow, stage: &Stage, popkey: POPKey) -> Result<Box<dyn POPContext>, String> {
        let (pop, _, children) = stage.pop_graph.get3(popkey);
        let child_contexts = if let Some(children) = children {
            let children = children
                .iter()
                .map(|&child_popkey| self.init_context(flow, stage, child_popkey).unwrap())
                .collect::<Vec<_>>();
            Some(children)
        } else {
            None
        };

        let ctxt = match &pop {
            POP::CSV(csv) => CSVContext::try_new(popkey, csv, self.partition_id)?,
            POP::RepartitionWrite(rpw) => RepartitionWriteContext::try_new(popkey, rpw, child_contexts.unwrap(), self.partition_id)?,

            // FIXME: Since POP graphs are local to stages, repartition directories may not be unique. Use LOPKey?
            POP::RepartitionRead(rpr) => RepartitionReadContext::try_new(flow.id, popkey, rpr, self.partition_id)?,
            POP::HashMatch(hj) => HashMatchContext::try_new(popkey, hj, child_contexts.unwrap(), self.partition_id)?,
            _ => unimplemented!(),
        };
        Ok(ctxt)
    }
}
