// pop_hashmatch

use std::collections::HashMap;

use ahash::RandomState;
use arrow2::{
    array::{MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array},
    compute::{filter::filter_chunk, take},
    datatypes::PhysicalType,
    types::PrimitiveType,
};
use itertools::izip;

use crate::{
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, POPContext, POP},
    pop_hash::*,
    stage::Stage,
};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashMatch {
    pub keycols: Vec<Vec<ColId>>, // Maintain a list of key columns for each child. len() == 2 for joins
    pub children_data_types: Vec<Vec<DataType>>,
}

/***************************************************************************************************/
struct HashMatchSplit {
    id: SplitId,
    mut_arrays: Vec<Box<dyn MutableArray>>,
    arrays: Vec<Box<dyn Array>>,
    hash_map: HashMap<HashValue, Vec<BuildRowId>>, // Hash-of-keys -> {Row-Id}*
}

impl HashMatchSplit {
    fn new(id: SplitId) -> Self {
        HashMatchSplit { id, mut_arrays: vec![], arrays: vec![], hash_map: HashMap::new() }
    }
}

macro_rules! copy_to_build_array {
    ($from_array_typ:ty, $from_array:expr, $to_array_typ:ty, $to_array:expr, $split_ids:expr, $cur_split_id:expr, $hash_array:expr, $hash_map:expr) => {{
        let primarr = $from_array.as_any().downcast_ref::<$from_array_typ>().unwrap();
        let mutarr = $to_array.as_mut_any().downcast_mut::<$to_array_typ>().unwrap();
        let hash_map = $hash_map;
        let mutarr_height = mutarr.len();

        $split_ids.iter().enumerate().filter(|(_, &split_id)| split_id == $cur_split_id).for_each(|(rid, _)| {
            let value = primarr.get(rid);
            mutarr.push(value);
            debug!("copy_to_build_array: inserted {:?} into split {}", value, $cur_split_id);

            let hash_value = $hash_array[rid];
            let rids = hash_map.entry(hash_value).or_insert(vec![]);
            rids.push(rid + mutarr_height)
        });
    }};
}

macro_rules! copy_from_build_array {
    ($from_array_typ:ty, $self:expr, $rids:expr, $colid:expr) => {{
        let iter = $rids.iter().map(|&(_, build_rid)| {
            if let Some((split_id, build_rid)) = build_rid {
                let split = &$self.splits[split_id];
                let array = &split.arrays[$colid];
                let primarr = array.as_any().downcast_ref::<$from_array_typ>().unwrap();
                primarr.get(build_rid)
            } else {
                panic!("copy_from_build_array() was passed rid-list with no build rids.")
            }
        });
        let arr = <$from_array_typ>::from_trusted_len_iter(iter);
        let arr: Box<dyn Array> = Box::new(arr);
        arr
    }};
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
            self.next_join(flow, stage, hash_match)
        } else {
            panic!("ugh");
        }
    }
}

impl HashMatchContext {
    pub fn try_new(pop_key: POPKey, _: &HashMatch, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let state = RandomState::with_seeds(97, 31, 45, 21);

        Ok(Box::new(HashMatchContext { pop_key, children, partition_id, state, splits: vec![] }))
    }

    fn next_join(&mut self, flow: &Flow, stage: &Stage, hash_match: &HashMatch) -> Result<Option<ChunkBox>, String> {
        // Build hash-tables
        if self.splits.is_empty() {
            self.process_join_build_input(flow, stage, hash_match)?;
        }

        // Probe
        let child = &mut self.children[0];
        while let Some(chunk) = child.next(flow, stage)? {
            if !chunk.is_empty() {
                let chunk = self.process_join_probe_input(flow, stage, hash_match, chunk)?;
                debug!("HashMatchContext::next \n{}", chunk_to_string(&chunk, "HashMatchContext::next"));
                return Ok(Some(chunk));
            }
        }
        Ok(None)
    }

    fn process_join_build_input(&mut self, flow: &Flow, stage: &Stage, hash_match: &HashMatch) -> Result<(), String> {
        // Initialize splits
        if self.splits.is_empty() {
            self.splits = (0..NSPLITS).map(|split_id| HashMatchSplit::new(split_id)).collect();
        }

        let child = &mut self.children[1];
        while let Some(chunk) = child.next(flow, stage)? {
            // Compute hash + split-# for each row in the chunk
            let keycols = &hash_match.keycols[1];
            let keys = eval_cols(keycols, &chunk);
            let (hash_array, split_ids) = hash_chunk(&keys, &self.state);

            for split in self.splits.iter_mut() {
                Self::insert(hash_match, split, &chunk, &hash_array, &split_ids);
            }
        }

        for split in self.splits.iter_mut() {
            Self::demut_build_arrays(split);
        }

        Ok(())
    }

    fn insert(hash_match: &HashMatch, split: &mut HashMatchSplit, build_chunk: &ChunkBox, hash_array: &[u64], split_ids: &[SplitId]) {
        if split.mut_arrays.is_empty() {
            Self::alloc_build_arrays(hash_match, split, build_chunk);
        }

        let cur_split_id = split.id;
        for (from_array, to_array, typ) in izip!(build_chunk.arrays(), split.mut_arrays.iter_mut(), &hash_match.children_data_types[1]) {
            match typ.to_physical_type() {
                PhysicalType::Primitive(PrimitiveType::Int64) => {
                    copy_to_build_array!(PrimitiveArray<i64>, from_array, MutablePrimitiveArray<i64>, to_array, split_ids, cur_split_id, hash_array, &mut split.hash_map)
                }
                PhysicalType::Utf8 => {
                    copy_to_build_array!(Utf8Array<i32>, from_array, MutableUtf8Array<i32>, to_array, split_ids, cur_split_id, hash_array, &mut split.hash_map)
                }
                PhysicalType::Boolean => {
                    copy_to_build_array!(BooleanArray, from_array, MutableBooleanArray, to_array, split_ids, cur_split_id, hash_array, &mut split.hash_map)
                }
                typ => panic!("insert(), todo: {:?}", typ),
            }
        }
    }

    #[allow(unused_variables)]
    fn alloc_build_arrays(hash_match: &HashMatch, split: &mut HashMatchSplit, chunk: &ChunkBox) {
        // Compute physical types of each build array
        let types = &hash_match.children_data_types[1];

        debug!("alloc_build_arrays(): types = {:?}", &types);

        let arrays: Vec<Box<dyn MutableArray>> = types
            .iter()
            .map(|typ| {
                let mut_array: Box<dyn MutableArray> = match typ.to_physical_type() {
                    PhysicalType::Primitive(PrimitiveType::Int64) => Box::new(MutablePrimitiveArray::<i64>::new()),
                    PhysicalType::Utf8 => Box::new(MutableUtf8Array::<i32>::new()),
                    PhysicalType::Boolean => Box::new(MutableBooleanArray::new()),
                    typ => panic!("alloc_build_arrays(), todo: {:?}", typ),
                };
                mut_array
            })
            .collect();
        split.mut_arrays = arrays;
    }

    fn demut_build_arrays(split: &mut HashMatchSplit) {
        let mut_arrays = std::mem::take(&mut split.mut_arrays);
        split.arrays = mut_arrays.into_iter().map(|mut mut_array| mut_array.as_box()).collect();
    }

    #[allow(unused_variables)]
    fn process_join_probe_input(&mut self, flow: &Flow, stage: &Stage, hash_match: &HashMatch, chunk: ChunkBox) -> Result<ChunkBox, String> {
        let props = stage.pop_graph.get_properties(self.pop_key);
        let keycols = &hash_match.keycols[0];

        // Hash input keys
        let keys = eval_cols(keycols, &chunk);
        let (hash_array, split_ids) = hash_chunk(&keys, &self.state);

        debug!(
            "HashMatchContext {:?} partition = {}, hash = {:?}{}{}",
            self.pop_key,
            self.partition_id,
            &hash_array,
            chunk_to_string(&chunk, "probe input"),
            chunk_to_string(&keys, "probe keys"),
        );

        let rids = self.find_matches(hash_array, split_ids, false);

        if !rids.is_empty() {
            let probe_chunk = self.contruct_probe_output(&rids, &chunk)?;
            let build_chunk = self.contruct_build_output(hash_match, &rids)?;

            let chunk = Self::contruct_joined_chunk(hash_match, build_chunk, probe_chunk)?;

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

    fn find_matches(&mut self, hash_array: Vec<u64>, split_ids: Vec<SplitId>, include_missing: bool) -> MatchRIDList {
        // Build rid-list based on hash-match: Probe-RowId -> SplitId + BuildRowId
        let mut rid_matches = vec![];
        for (probe_rid, (hash_key, &split_id)) in hash_array.iter().zip(split_ids.iter()).enumerate() {
            let split = &self.splits[split_id];
            let build_rid = split.hash_map.get(hash_key);
            if let Some(matches) = build_rid {
                for &build_rid in matches.iter() {
                    debug!("process_probe_side: probe_rid = {}, build_rid = {}", probe_rid, build_rid);
                    rid_matches.push((probe_rid, Some((split_id, build_rid))))
                }
            } else if include_missing {
                rid_matches.push((probe_rid, None))
            }
        }
        rid_matches
    }

    fn contruct_build_output(&mut self, hash_match: &HashMatch, rids: &[MatchRIDPair]) -> Result<ChunkBox, String> {
        if self.splits.is_empty() {
            return Ok(Chunk::new(vec![]));
        }
        
        let types = &hash_match.children_data_types[1];
        let build_arrays = types
            .into_iter()
            .enumerate()
            .map(|(colid, typ)| match typ.to_physical_type() {
                PhysicalType::Primitive(PrimitiveType::Int64) => copy_from_build_array!(PrimitiveArray<i64>, self, rids, colid),
                PhysicalType::Utf8 => copy_from_build_array!(Utf8Array<i32>, self, rids, colid),
                PhysicalType::Boolean => copy_from_build_array!(BooleanArray, self, rids, colid),
                typ => panic!("contruct_build_output(), todo: {:?}", typ),
            })
            .collect();

        let build_chunk = Chunk::new(build_arrays);

        debug!("HashMatchContext {:?} partition = {}{}", self.pop_key, self.partition_id, chunk_to_string(&build_chunk, "build_chunk"),);

        Ok(build_chunk)
    }

    #[allow(dead_code)]
    fn copy_from_build_arrays0(&mut self, rids: &[(usize, (usize, usize))], colid: usize) -> Box<dyn Array> {
        let iter = rids.iter().map(|&(_, (split_id, build_rid))| {
            let split = &self.splits[split_id];
            let array = &split.arrays[colid];
            let primarr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            primarr.get(build_rid)
        });
        let arr = PrimitiveArray::<i64>::from_trusted_len_iter(iter);
        let arr: Box<dyn Array> = Box::new(arr);
        arr
    }

    fn contruct_probe_output(&mut self, rids: &MatchRIDList, keys: &ChunkBox) -> Result<ChunkBox, String> {
        let probe_rids: PrimitiveArray<u64> = rids.iter().map(|e| Some(e.0 as u64)).collect();
        let probe_arrays = Self::take_chunk(keys, probe_rids)?;
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

    fn contruct_joined_chunk(hash_match: &HashMatch, build_chunk: ChunkBox, probe_chunk: ChunkBox) -> Result<ChunkBox, String> {
        // So far, we've only matched build/probe based on hash-values. Make sure the actual keys match.
        assert!(build_chunk.len() == probe_chunk.len());

        debug!("contruct_joined_chunk: {} {}", chunk_to_string(&build_chunk, "build_chunk"), chunk_to_string(&probe_chunk, "probe_chunk"),);

        let chunk_height = build_chunk.len();
        let build_cols = &hash_match.keycols[1];
        let probe_cols = &hash_match.keycols[0];

        let mut build_arrays = build_chunk.into_arrays();
        let mut probe_arrays = probe_chunk.into_arrays();

        let build_keys = build_arrays.iter().enumerate().filter_map(|(ix, array)| if build_cols.contains(&ix) { Some(&**array) } else { None }).collect::<Vec<_>>();
        let probe_keys = probe_arrays.iter().enumerate().filter_map(|(ix, array)| if probe_cols.contains(&ix) { Some(&**array) } else { None }).collect::<Vec<_>>();

        // Compare key columns
        let mut filter = BooleanArray::from(vec![Some(true); chunk_height]);
        for (&build_keycol, &probe_keycol) in build_keys.iter().zip(probe_keys.iter()) {
            let filter2 = comparison::eq(build_keycol, probe_keycol);
            filter = boolean::and(&filter, &filter2);
        }

        // Join and filter final chunk
        probe_arrays.append(&mut build_arrays);
        let chunk = Chunk::new(probe_arrays);
        filter_chunk(&chunk, &filter).map_err(stringify)
    }

    fn take_chunk(chunk: &ChunkBox, rids: PrimitiveArray<u64>) -> Result<Vec<Box<dyn Array>>, String> {
        chunk.arrays().iter().map(|array| Ok(take::take(&**array, &rids).map_err(stringify)?)).collect::<Result<Vec<_>, String>>()
    }
}
