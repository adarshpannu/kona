// pop: Physical operators

//#![allow(unused_variables)]

pub use crate::{includes::*, pcode::*, pcode::*, qgm::*, row::*};

pub type POPGraph = Graph<POPKey, POP, POPProps>;
use arrow2::compute::filter::filter_chunk;

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
                // Run predicates and emits, if any
                chunk = Self::eval_predicates(props, chunk);
                debug!("After preds: {:?}", &chunk);

                let emitchunk: Option<Chunk<Box<dyn Array>>> = Self::eval_emitcols(props, &chunk);
                if let Some(emitchunk) = emitchunk {
                    debug!("Emitcols: {:?}", &emitchunk);
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

    pub fn eval_emitcols(props: &POPProps, input: &ChunkBox) -> Option<ChunkBox> {
        if let Some(emitcols) = props.emitcols.as_ref() {
            let emit_output = emitcols
                .iter()
                .map(|emit| {
                    let result = emit.eval(&input);
                    result
                })
                .collect::<Vec<_>>();
            Some(Chunk::new(emit_output))
        } else {
            None
        }
    }
}
