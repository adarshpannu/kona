// pop_hashmatch

use std::collections::HashMap;
use ahash::RandomState;
use arrow2::{
    array::{MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array},
    compute::take,
    datatypes::PhysicalType,
    types::PrimitiveType,
};
use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pcode::PCode,
    pop::{chunk_to_string, Agg, POPContext, POP},
    stage::Stage,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum HashMatchSubtype {
    Join,
    Aggregation(Vec<(Agg, ColId)>),
}

const NSPLITS: usize = 10;

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashMatch {
    pub keycols: Vec<Vec<ColId>>,
    pub subtype: HashMatchSubtype,
}

type HashValue = u64;
type RowId = usize;
type SplitId = usize;

/***************************************************************************************************/
struct HashMatchSplit {
    id: SplitId,
    mut_arrays: Vec<Box<dyn MutableArray>>,
    arrays: Vec<Box<dyn Array>>,
    hash_map: HashMap<HashValue, Vec<RowId>>, // Hash-of-keys -> {Row-Id}*
}

macro_rules! copy_to_build_array {
    ($from_array_typ:ty, $from_array:expr, $to_array_typ:ty, $to_array:expr, $split_ids:expr, $cur_split_id:expr, $hash_array:expr, $hash_map:expr) => {{
        let primarr = $from_array.as_any().downcast_ref::<$from_array_typ>().unwrap();
        let mutarr = $to_array.as_mut_any().downcast_mut::<$to_array_typ>().unwrap();
        let hash_map = $hash_map;
        $split_ids.iter().enumerate().filter(|(_, &splid_id)| splid_id == $cur_split_id).for_each(|(rid, _)| {
            let value = primarr.get(rid);
            debug!("copy_to_build_array: inserted {:?} into split {}", value, $cur_split_id);
            mutarr.push(value);

            let hash_value = $hash_array[rid];
            let rids = hash_map.entry(hash_value).or_insert(vec![]);
            rids.push(rid)
        });
    }};
}

impl HashMatchSplit {
    fn new(id: SplitId) -> Self {
        HashMatchSplit { id, mut_arrays: vec![], arrays: vec![], hash_map: HashMap::new() }
    }

    pub fn insert(&mut self, build_chunk: &ChunkBox, hash_array: &[u64], split_ids: &[SplitId]) {
        if self.mut_arrays.is_empty() {
            self.alloc_build_arrays(build_chunk);
        }

        let cur_split_id = self.id;
        for (from_array, to_array) in build_chunk.arrays().iter().zip(self.mut_arrays.iter_mut()) {
            match from_array.data_type().to_physical_type() {
                PhysicalType::Primitive(PrimitiveType::Int64) => {
                    copy_to_build_array!(PrimitiveArray<i64>, from_array, MutablePrimitiveArray<i64>, to_array, split_ids, cur_split_id, hash_array, &mut self.hash_map)
                }
                PhysicalType::Boolean => {
                    copy_to_build_array!(BooleanArray, from_array, MutableBooleanArray, to_array, split_ids, cur_split_id, hash_array, &mut self.hash_map)
                }
                PhysicalType::Utf8 => {
                    copy_to_build_array!(Utf8Array<i32>, from_array, MutableUtf8Array<i32>, to_array, split_ids, cur_split_id, hash_array, &mut self.hash_map)
                }
                _ => todo!(),
            }
        }
    }

    fn alloc_build_arrays(&mut self, chunk: &ChunkBox) {
        let arrays: Vec<Box<dyn MutableArray>> = chunk
            .arrays()
            .iter()
            .map(|array| {
                let mut_array: Box<dyn MutableArray> = match array.data_type().to_physical_type() {
                    PhysicalType::Primitive(PrimitiveType::Int64) => Box::new(MutablePrimitiveArray::<i64>::new()),
                    PhysicalType::Utf8 => Box::new(MutableUtf8Array::<i32>::new()),
                    _ => panic!("alloc_arrays(), todo: {:?}", array.data_type().to_physical_type()),
                };
                mut_array
            })
            .collect();
        self.mut_arrays = arrays;
    }

    fn demut_build_arrays(&mut self) {
        let mut_arrays = std::mem::take(&mut self.mut_arrays);
        self.arrays = mut_arrays.into_iter().map(|mut mut_array| mut_array.as_box()).collect();
    }
}

/***************************************************************************************************/
pub struct HashMatchContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    state: RandomState,
    splits: Vec<HashMatchSplit>,
}

impl POPContext for HashMatchContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        if let POP::HashMatch(hash_match) = pop {
            // Build side (right child)
            if self.splits.is_empty() {
                self.process_build_side(flow, stage)?;
            }

            // Probe-side (left child)
            let child = &mut self.children[0];

            while let Some(chunk) = child.next(flow, stage)? {
                if !chunk.is_empty() {
                    let chunk = self.process_probe_side(flow, stage, hash_match, chunk)?;
                    debug!("HashMatchContext::next \n{}", chunk_to_string(&chunk, "HashMatchContext::next"));
                    return Ok(Some(chunk));
                }
            }
        } else {
            panic!("ugh")
        }
        Ok(None)
    }
}

impl HashMatchContext {
    pub fn try_new(pop_key: POPKey, _: &HashMatch, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let state = RandomState::with_seeds(97, 31, 45, 21);

        Ok(Box::new(HashMatchContext { pop_key, children, partition_id, state, splits: vec![] }))
    }

    fn eval_keys(pcode: &[PCode], input: &ChunkBox) -> ChunkBox {
        let arrays = pcode.iter().map(|code| code.eval(input)).collect();
        Chunk::new(arrays)
    }

    fn eval_cols(cols: &[ColId], input: &ChunkBox) -> ChunkBox {
        let arrays = cols.iter().map(|&colid| input.arrays()[colid].clone()).collect();
        Chunk::new(arrays)
    }

    fn process_build_side(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);
        let child = &mut self.children[1];

        // Initialize splits
        for split_id in 0..NSPLITS {
            let split = HashMatchSplit::new(split_id);
            self.splits.push(split)
        }

        if let POP::HashMatch(hm) = pop {
            // Collect all build chunks
            while let Some(chunk) = child.next(flow, stage)? {
                // Compute split # for each row
                let keycols = &hm.keycols[1];
                let keys = Self::eval_cols(keycols, &chunk);
                let (hash_array, split_ids) = hash_chunk(&keys, &self.state);

                for split in self.splits.iter_mut() {
                    split.insert(&chunk, &hash_array, &split_ids);
                }
            }
        }

        for split in self.splits.iter_mut() {
            split.demut_build_arrays();
        }

        Ok(())
    }

    #[allow(unused_variables)]
    fn process_probe_side(&mut self, flow: &Flow, stage: &Stage, hash_match: &HashMatch, chunk: ChunkBox) -> Result<ChunkBox, String> {
        let pop_key = self.pop_key;
        let props = stage.pop_graph.get_properties(pop_key);

        let keycols = &hash_match.keycols[0];

        // Hash input keys
        let keys = Self::eval_cols(keycols, &chunk);
        let (hash_array, split_ids) = hash_chunk(&keys, &self.state);

        debug!(
            "HashMatchContext {:?} partition = {}, hash = {:?}{}{}",
            self.pop_key,
            self.partition_id,
            &hash_array,
            chunk_to_string(&chunk, "probe input"),
            chunk_to_string(&keys, "probe keys"),
        );

        // Build array indices based on hash-match
        // Chunk-RowId -> SplitId + RowId
        let mut indices: Vec<(RowId, (SplitId, RowId))> = vec![];
        for (build_rid, (hash_key, &splid_id)) in hash_array.iter().zip(split_ids.iter()).enumerate() {
            let split = &self.splits[splid_id];
            let rids = split.hash_map.get(hash_key);
            if let Some(matches) = rids {
                debug!("FOUND!");
                for &rid_in_split in matches.iter() {
                    indices.push((build_rid, (splid_id, rid_in_split)))
                }
            }
        }

        // Build chunk to join keys
        if !indices.is_empty() {
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

    fn contruct_probe_chunk_from_indices(&mut self, indices: &Vec<(RowId, (SplitId, RowId))>, keys: &ChunkBox) -> Result<ChunkBox, String> {
        let probe_indices: PrimitiveArray<u64> = indices.iter().map(|e| Some(e.0 as u64)).collect();
        let probe_arrays = Self::take_chunk(keys, probe_indices)?;
        let probe_chunk = Chunk::new(probe_arrays);

        /*
        debug!(
            "HashMatchContext {:?} partition = {}{}",
            self.pop_key,
            self.partition_id,
            chunk_to_string(&probe_chunk, "probe_chunk"),
        );
        */
        Ok(probe_chunk)
    }

    fn contruct_build_chunk_from_indices(&mut self, indices: &[(RowId, (SplitId, RowId))]) -> Result<ChunkBox, String> {
        if self.splits.is_empty() {
            return Ok(Chunk::new(vec![]));
        }

        let first_splid_id = indices[0].1 .0;
        let first_split = &self.splits[first_splid_id];
        let types = first_split.arrays.iter().map(|a| a.data_type().to_physical_type()).collect::<Vec<_>>();

        let build_arrays = types
            .into_iter()
            .enumerate()
            .map(|(colid, typ)| match typ {
                PhysicalType::Primitive(PrimitiveType::Int64) => {
                    let iter = indices.iter().map(|&(_, (split_id, rid_in_split))| {
                        let split = &self.splits[split_id];
                        let array = &split.arrays[colid];
                        let primarr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                        primarr.get(rid_in_split)
                    });
                    let primarr = PrimitiveArray::<i64>::from_trusted_len_iter(iter);
                    let primarr: Box<dyn Array> = Box::new(primarr);
                    primarr
                }
                t => panic!("contruct_build_chunk_from_indices() not yet implemented for type: {:?}", t),
            })
            .collect();

        let build_chunk = Chunk::new(build_arrays);

        debug!("HashMatchContext {:?} partition = {}{}", self.pop_key, self.partition_id, chunk_to_string(&build_chunk, "build_chunk"),);

        Ok(build_chunk)
    }

    fn take_chunk(chunk: &ChunkBox, indices: PrimitiveArray<u64>) -> Result<Vec<Box<dyn Array>>, String> {
        chunk.arrays().iter().map(|array| Ok(take::take(&**array, &indices).map_err(stringify)?)).collect::<Result<Vec<_>, String>>()
    }
}

macro_rules! hash_array {
    ($array_type:ty,$array:expr,$state:expr,$hash_array:expr) => {{
        let array_inner: &$array_type = $array.as_any().downcast_ref().unwrap();
        for (ix, elem) in array_inner.values_iter().enumerate() {
            let hv = $state.hash_one(elem);
            $hash_array[ix] += hv;
        }
    }};
}

fn hash_chunk(chunk: &ChunkBox, state: &RandomState) -> (Vec<HashValue>, Vec<SplitId>) {
    // Initialize hash array
    let mut hash_array = vec![0; chunk.len()];

    for array in chunk.arrays() {
        match array.data_type().to_physical_type() {
            PhysicalType::Boolean => hash_array!(BooleanArray, array, state, &mut hash_array),
            PhysicalType::Primitive(PrimitiveType::Int64) => hash_array!(PrimitiveArray<i64>, array, state, &mut hash_array),
            PhysicalType::Utf8 => hash_array!(Utf8Array<i32>, array, state, &mut hash_array),
            t => panic!("Hash not implemented for type: {:?}", t),
        }
    }
    let split_ids = hash_array.iter().map(|&e| e as usize % NSPLITS).collect::<Vec<_>>();
    (hash_array, split_ids)
}
