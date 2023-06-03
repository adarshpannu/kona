// pop: Physical operators

use arrow2::compute::filter::filter_chunk;

use crate::{graph::POPKey, includes::*, pop::POPProps};

impl POPKey {
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
