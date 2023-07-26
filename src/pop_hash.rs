// pop_hash.rs

use ahash::RandomState;
use arrow2::{array::Utf8Array, datatypes::PhysicalType, types::PrimitiveType};

use crate::includes::*;

pub type SplitId = usize;
pub type HashValue = u64;
pub type BuildRowId = usize;
pub type ProbeRowId = usize;
pub type MatchRIDPair = (ProbeRowId, Option<(SplitId, BuildRowId)>);
pub type MatchRIDList = Vec<MatchRIDPair>;

pub const NSPLITS: usize = 1;

macro_rules! hash_array {
    ($array_type:ty,$array:expr,$state:expr,$hash_array:expr) => {{
        let array_inner: &$array_type = $array.as_any().downcast_ref().unwrap();
        for (ix, elem) in array_inner.values_iter().enumerate() {
            let hv = $state.hash_one(elem);
            $hash_array[ix] += hv;
            $hash_array[ix] = 999;
        }
    }};
}

pub fn hash_chunk(chunk: &ChunkBox, state: &RandomState) -> (Vec<HashValue>, Vec<SplitId>) {
    // Initialize hash array
    let mut hash_array = vec![0; chunk.len()];

    for array in chunk.arrays() {
        match array.data_type().to_physical_type() {
            PhysicalType::Primitive(PrimitiveType::Int64) => hash_array!(PrimitiveArray<i64>, array, state, &mut hash_array),
            PhysicalType::Utf8 => hash_array!(Utf8Array<i32>, array, state, &mut hash_array),
            PhysicalType::Boolean => hash_array!(BooleanArray, array, state, &mut hash_array),
            t => panic!("Hash not implemented for type: {:?}", t),
        }
    }
    let split_ids = hash_array.iter().map(|&e| e as usize % NSPLITS).collect::<Vec<_>>();
    (hash_array, split_ids)
}

pub fn eval_cols(cols: &[ColId], input: &ChunkBox) -> ChunkBox {
    let arrays = cols.iter().map(|&colid| input.arrays()[colid].clone()).collect();
    Chunk::new(arrays)
}
