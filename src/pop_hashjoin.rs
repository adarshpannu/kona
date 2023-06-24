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
use arrow2::array::Utf8Array;
use self_cell::self_cell;

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {
    pub keyexprs: Vec<Vec<PCode>>,
}

impl HashJoin {}

/***************************************************************************************************/
pub struct HashJoinContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    state: RandomState,
    pub cell: Option<HashJoinCell>
}

impl POPContext for HashJoinContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Chunk<Box<dyn Array>>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        if let POP::HashJoin(hj) = pop {
            // Build side (right child)
            if self.cell.is_none() {
                self.process_build_side(flow, stage)?;
            }

            // Probe-side (left child)
            loop {
                let child = &mut self.children[0];

                let chunk = child.next(flow, stage)?;
                if chunk.is_empty() {
                    break;
                }

                self.process_probe_side(hj, chunk);
            }
        } else {
            panic!("ugh")
        }

        Ok(Chunk::new(vec![]))
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
            cell: None
        }))
    }

    fn eval_keys(pcode: &[PCode], input: &ChunkBox) -> ChunkBox {
        let arrays = pcode.iter().map(|code| code.eval(input)).collect();
        Chunk::new(arrays)
    }

    fn process_build_side(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        let mut build_chunks: Vec<ChunkBox> = vec![];
        let child = &mut self.children[1];

        if let POP::HashJoin(hj) = pop {
            // Collect all build chunks
            loop {
                let chunk = child.next(flow, stage)?;
                if chunk.is_empty() {
                    break;
                }
                build_chunks.push(chunk);
            }

            let cell = HashJoinCell::new(build_chunks, |build_chunks| {
                let mut hash_map: HashJoinHashMap = HashMap::new(); // secondary-hash -> (vec-index, chunk-index>)

                for (ix, chunk) in build_chunks.iter().enumerate() {
                    let keyscode = &hj.keyexprs[1];
                    let keys = Self::eval_keys(keyscode, &chunk);
                    let hash_array = hash_chunk(&keys, &self.state);

                    debug!(
                        "HashJoinContext {:?} partition = {}::build_keys: \n{}, hash: {:?}",
                        self.pop_key,
                        self.partition_id,
                        chunk_to_string(&keys),
                        &hash_array
                    );

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

    fn process_probe_side(&mut self, hj: &HashJoin, chunk: Chunk<Box<dyn Array>>) {
        let keyscode = &hj.keyexprs[0];

        // Hash input keys
        let keys = Self::eval_keys(keyscode, &chunk);
        let hash_array = hash_chunk(&keys, &self.state);

        debug!(
            "HashJoinContext {:?} partition = {}::\n input = {}\n keys = {}:, hash: {:?}",
            self.pop_key,
            self.partition_id,
            chunk_to_string(&chunk),
            chunk_to_string(&keys),
            &hash_array
        );

        // Probe and build array indices
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
        //let keys_arrays = vec![];
        if indices.len() > 0 {

        }
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
