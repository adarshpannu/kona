// pop: Physical operators

use arrow2::compute::filter::filter_chunk;

use crate::{
    graph::POPKey,
    includes::*,
    pop::{POPProps, POP},
    stage::Stage,
    task::Task,
    Flow,
};

impl POPKey {
    pub fn next(&self, flow: &Flow, _stage: &Stage, task: &mut Task) -> Result<ChunkBox, String> {
        let (pop, props, ..) = flow.pop_graph.get3(*self);

        loop {
            let mut chunk = match pop {
                POP::CSV(inner_node) => inner_node.next(task)?,
                POP::CSVDir(inner_node) => inner_node.next(task)?,
                POP::Repartition(inner_node) => inner_node.next(task)?,
                POP::RepartitionRead(inner_node) => inner_node.next(task)?,
                POP::HashJoin(inner_node) => inner_node.next(task)?,
                POP::Aggregation(inner_node) => inner_node.next(task)?,
            };

            debug!("Before preds: {:?}", &chunk);

            if chunk.len() > 0 {
                // Run predicates and virtcols, if any
                chunk = Self::eval_predicates(props, chunk);
                debug!("After preds: {:?}", &chunk);

                let projection_chunk: Option<Chunk<Box<dyn Array>>> = Self::eval_virtcols(props, &chunk);
                if let Some(projection_chunk) = projection_chunk {
                    debug!("Virtcols: {:?}", &projection_chunk);
                }
            }
            return Ok(chunk);
        }
    }

    pub fn eval_predicates(props: &POPProps, input: ChunkBox) -> ChunkBox {
        let mut filtered_chunk = input;
        if let Some(preds) = props.predicates.as_ref() {
            for pred in preds.iter() {
                let bool_chunk = pred.eval(&filtered_chunk);
                let bool_array = bool_chunk.as_any().downcast_ref::<BooleanArray>().unwrap();

                filtered_chunk = filter_chunk(&filtered_chunk, &bool_array).unwrap();
            }
        }
        return filtered_chunk;
    }

    pub fn eval_virtcols(props: &POPProps, input: &ChunkBox) -> Option<ChunkBox> {
        if let Some(virtcols) = props.virtcols.as_ref() {
            let virtoutput = virtcols
                .iter()
                .map(|pcode| {
                    let result = pcode.eval(&input);
                    result
                })
                .collect::<Vec<_>>();
            Some(Chunk::new(virtoutput))
        } else {
            None
        }
    }
}
