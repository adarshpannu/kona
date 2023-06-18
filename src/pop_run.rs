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

                filtered_chunk = filter_chunk(&filtered_chunk, bool_array).unwrap();
            }
        }
        filtered_chunk
    }

    pub fn eval_projection(props: &POPProps, input: &ChunkBox) -> ChunkBox {
        let mut output = vec![];
        let arrays = input.columns();

        if let Some(colids) = props.cols.as_ref() {
            for &colid in colids.iter() {
                let arr = arrays[colid].clone();
                output.push(arr)
            }
        }

        if let Some(virtcols) = props.virtcols.as_ref() {
            for pcode in virtcols.iter() {
                let arr = pcode.eval(input);
                output.push(arr)
            }
        }
        Chunk::new(output)
    }
}
