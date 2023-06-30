// pop_hashjoin

use std::collections::HashMap;

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pcode::PCode,
    pop::{chunk_to_string, POPContext, POP},
    stage::Stage,
};
use ahash::RandomState;
use arrow2::{
    array::{MutableBooleanArray, MutablePrimitiveArray, Utf8Array},
    compute::take,
};
use self_cell::self_cell;

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {
    pub keycols: Vec<Vec<ColId>>,
}

impl HashJoin {}

/***************************************************************************************************/
pub struct HashJoinContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    state: RandomState,
    pub cell: Option<HashJoinCell>,
}

impl POPContext for HashJoinContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        if let POP::HashJoin(hj) = pop {
            // Build side (right child)
            if self.cell.is_none() {
                self.process_build_side(flow, stage)?;
            }

            // Probe-side (left child)
            let child = &mut self.children[0];

            while let Some(chunk) = child.next(flow, stage)? {
                if !chunk.is_empty() {
                    let chunk = self.process_probe_side(flow, stage, hj, chunk)?;
                    debug!("HashJoinContext::next \n{}", chunk_to_string(&chunk, "HashJoinContext::next"));
                    return Ok(Some(chunk));
                }
            }
        } else {
            panic!("ugh")
        }
        Ok(None)
    }
}

type HashJoinHashMap = HashMap<u64, Vec<(usize, usize)>>;

type BoxHashJoinHashMap<'a> = Box<HashMap<u64, Vec<(usize, usize)>>>;

self_cell!(
    pub struct HashJoinCell {
        owner: Vec<ChunkBox>,

        #[covariant]
        dependent: BoxHashJoinHashMap,
    }
);

impl HashJoinContext {
    pub fn try_new(pop_key: POPKey, _: &HashJoin, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let state = RandomState::with_seeds(97, 31, 45, 21);

        Ok(Box::new(HashJoinContext {
            pop_key,
            children,
            partition_id,
            state,
            cell: None,
        }))
    }

    fn eval_keys(pcode: &[PCode], input: &ChunkBox) -> ChunkBox {
        let arrays = pcode.iter().map(|code| code.eval(input)).collect();
        Chunk::new(arrays)
    }

    fn eval_cols(cols: &[ColId], input: &ChunkBox) -> ChunkBox {
        let arrays = input.arrays();
        let arrays = cols.iter().map(|&colid| arrays[colid].clone()).collect();
        Chunk::new(arrays)
    }

    fn process_build_side(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        let mut build_chunks: Vec<ChunkBox> = vec![];
        let child = &mut self.children[1];

        if let POP::HashJoin(hj) = pop {
            // Collect all build chunks
            while let Some(chunk) = child.next(flow, stage)? {
                if chunk.len() > 0 {
                    build_chunks.push(chunk);
                }
            }

            let cell = HashJoinCell::new(build_chunks, |build_chunks| {
                let mut hash_map: HashJoinHashMap = HashMap::new(); // secondary-hash -> (vec-index, chunk-index>)

                for (ix, chunk) in build_chunks.iter().enumerate() {
                    let keycols = &hj.keycols[1];
                    let keys = Self::eval_cols(keycols, &chunk);
                    let hash_array = hash_chunk(&keys, &self.state);

                    /*
                    debug!(
                        "HashJoinContext {:?} partition = {}::build_keys: \n{}, hash: {:?}",
                        self.pop_key,
                        self.partition_id,
                        chunk_to_string(&keys, "build keys"),
                        &hash_array
                    );
                    */

                    for (jx, &hv) in hash_array.iter().enumerate() {
                        let elems = hash_map.entry(hv).or_insert(vec![]);
                        elems.push((ix, jx))
                    }
                }
                Box::new(hash_map)
            });
            self.cell = Some(cell);
        }
        Ok(())
    }

    fn process_probe_side(&mut self, flow: &Flow, stage: &Stage, hj: &HashJoin, chunk: Chunk<Box<dyn Array>>) -> Result<ChunkBox, String> {
        let pop_key = self.pop_key;
        let props = stage.pop_graph.get_properties(pop_key);

        let keycols = &hj.keycols[0];

        // Hash input keys
        let keys = Self::eval_cols(keycols, &chunk);
        let hash_array = hash_chunk(&keys, &self.state);

        /*
        debug!(
            "HashJoinContext {:?} partition = {}, hash = {:?}{}{}",
            self.pop_key,
            self.partition_id,
            &hash_array,
            chunk_to_string(&chunk, "probe input"),
            chunk_to_string(&keys, "probe keys"),
        );
        */

        // Build array indices based on hash-match
        let mut indices: Vec<(usize, (usize, usize))> = vec![];
        for (ix, hash_key) in hash_array.iter().enumerate() {
            let cell = self.cell.as_ref().unwrap();
            let hash_map = cell.borrow_dependent();

            let matches = hash_map.get(hash_key);
            if let Some(matches) = matches {
                debug!("FOUND!");
                for &mtch in matches.iter() {
                    indices.push((ix, mtch))
                }
            }
        }

        // Build chunk to join keys
        if indices.len() > 0 {
            let probe_chunk = self.contruct_probe_chunk_from_indices(&indices, &chunk)?;
            let build_chunk = self.contruct_build_chunk_from_indices(&indices)?;

            let mut probe_arrays = probe_chunk.into_arrays();
            let mut build_arrays = build_chunk.into_arrays();
            probe_arrays.append(&mut build_arrays);

            let chunk = Chunk::new(probe_arrays);

            // Run predicates, if any
            let chunk = POPKey::eval_predicates(props, chunk);
            //debug!("After join preds: \n{}", chunk_to_string(&chunk, "After join preds"));

            let projection_chunk = POPKey::eval_projection(props, &chunk);
            debug!("hash_join_projection: \n{}", chunk_to_string(&projection_chunk, "hash_join_projection"));
            Ok(projection_chunk)
        } else {
            Ok(Chunk::new(vec![]))
        }
    }

    fn contruct_probe_chunk_from_indices(&mut self, indices: &Vec<(usize, (usize, usize))>, keys: &ChunkBox) -> Result<ChunkBox, String> {
        let probe_indices: PrimitiveArray<u64> = indices.iter().map(|e| Some(e.0 as u64)).collect();
        let probe_arrays = Self::take_chunk(&keys, probe_indices)?;
        let probe_chunk = Chunk::new(probe_arrays);

        /*
        debug!(
            "HashJoinContext {:?} partition = {}{}",
            self.pop_key,
            self.partition_id,
            chunk_to_string(&probe_chunk, "probe_chunk"),
        );
        */
        Ok(probe_chunk)
    }

    fn contruct_build_chunk_from_indices(&mut self, indices: &Vec<(usize, (usize, usize))>) -> Result<ChunkBox, String> {
        use arrow2::datatypes::PhysicalType;
        use arrow2::types::PrimitiveType;

        let build_indices: PrimitiveArray<u64> = indices.iter().map(|e| Some(e.0 as u64)).collect();
        let cell = self.cell.as_ref().unwrap();
        let chunks = cell.borrow_owner();
        if chunks.len() == 0 {
            return Ok(Chunk::new(vec![]));
        }

        let first_chunk = &chunks[0];
        let mut new_arrays = vec![];

        for (colid, array) in first_chunk.columns().iter().enumerate() {
            match array.data_type().to_physical_type() {
                PhysicalType::Primitive(PrimitiveType::Int64) => {
                    let mut mutarr = MutablePrimitiveArray::<i64>::new();
                    for &(_, (chunk_ix, ix)) in indices.iter() {
                        let chunk = &cell.borrow_owner()[chunk_ix];
                        let array = &chunk.columns()[colid];
                        let primarr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                        mutarr.extend_constant(1, primarr.get(ix))
                    }
                    let primarr = PrimitiveArray::<i64>::from(mutarr);
                    let primarr: Box<dyn Array> = Box::new(primarr);
                    new_arrays.push(primarr)
                }
                t => panic!("Hash not implemented for type: {:?}", t),
            }
        }

        let build_chunk = Chunk::new(new_arrays);

        debug!(
            "HashJoinContext {:?} partition = {}{}",
            self.pop_key,
            self.partition_id,
            chunk_to_string(&build_chunk, "build_chunk"),
        );

        Ok(build_chunk)
    }

    fn take_chunk(chunk: &ChunkBox, indices: PrimitiveArray<u64>) -> Result<Vec<Box<dyn Array>>, String> {
        use arrow2::compute::take::take;
        chunk
            .arrays()
            .iter()
            .map(|array| Ok(take(&**array, &indices).map_err(stringify)?))
            .collect::<Result<Vec<_>, String>>()
    }
}

fn hash_chunk(chunk: &ChunkBox, state: &RandomState) -> Vec<u64> {
    use arrow2::datatypes::PhysicalType;
    use arrow2::types::PrimitiveType;

    // Initialize hash array
    let mut hash_array = vec![0; chunk.len()];
    //let mut hash_array = PrimitiveArray::<u64>::from(hash_array);

    for array in chunk.columns() {
        let array = &**array;
        //let h = hash(array);

        match array.data_type().to_physical_type() {
            PhysicalType::Boolean => hash_boolean(array.as_any().downcast_ref().unwrap(), state, &mut hash_array),
            PhysicalType::Primitive(PrimitiveType::Int64) => hash_primitive(array.as_any().downcast_ref().unwrap(), state, &mut hash_array),
            PhysicalType::Utf8 => hash_utf8(array.as_any().downcast_ref().unwrap(), state, &mut hash_array),
            t => panic!("Hash not implemented for type: {:?}", t),
        }
    }
    hash_array
}

pub fn hash_boolean(array: &BooleanArray, state: &RandomState, hash_array: &mut [u64]) {
    for (ix, elem) in array.values_iter().enumerate() {
        let hv = state.hash_one(elem);
        hash_array[ix] += hv;
    }
}

pub fn hash_utf8(array: &Utf8Array<i32>, state: &RandomState, hash_array: &mut [u64]) {
    for (ix, elem) in array.values_iter().enumerate() {
        let hv = state.hash_one(elem);
        hash_array[ix] += hv;
    }
}

pub fn hash_primitive(array: &PrimitiveArray<i64>, state: &RandomState, hash_array: &mut [u64]) {
    for (ix, elem) in array.values_iter().enumerate() {
        let hv = state.hash_one(elem);
        hash_array[ix] += hv;
    }
}
