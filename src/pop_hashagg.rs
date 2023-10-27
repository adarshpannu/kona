// pop_hashagg

#![allow(warnings)]

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use arrow2::{
    array::{MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array},
    datatypes::PhysicalType,
    types::PrimitiveType,
};
use fasthash::xx;

use crate::{
    datum::F64,
    expr::AggType,
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, Agg, POPContext, POP},
    pop_hash::NSPLITS,
    stage::Stage,
    Datum,
};

type DataRow = Vec<Option<Datum>>;

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashAgg {
    pub keycols: Vec<Vec<ColId>>, // Maintain a list of key columns for each child. len() == 2 for joins
    pub child_data_types: Vec<DataType>,
    pub child_physical_types: Vec<PhysicalType>,
    pub aggs: Vec<(Agg, ColId)>, // ColId represents the ordering of each aggregator and starts at keylen
}

impl HashAgg {
    pub fn new(keycols: Vec<Vec<ColId>>, child_data_types: Vec<DataType>, aggs: Vec<(Agg, ColId)>) -> Self {
        let child_physical_types = child_data_types.iter().map(|typ| typ.to_physical_type()).collect::<Vec<_>>();
        HashAgg { keycols, child_data_types, child_physical_types, aggs }
    }

    pub fn keylen(&self) -> usize {
        self.keycols[0].len()
    }
}

/***************************************************************************************************/
struct HashAggSplit {
    hash_map: MyHashTable<DataRow, DataRow>, // Hash-of-keys -> Accumulators
}

impl HashAggSplit {
    fn new() -> Self {
        HashAggSplit { hash_map: MyHashTable::new(1000) }
    }
}
/***************************************************************************************************/
#[derive(Derivative)]
#[derivative(Debug)]
pub struct HashAggContext {
    pop_key: POPKey,
    partition_id: PartitionId,

    #[derivative(Debug = "ignore")]
    splits: Vec<HashAggSplit>,

    output_split: usize,

    #[derivative(Debug = "ignore")]
    children: Vec<Box<dyn POPContext>>,
}

impl HashAggContext {
    #[tracing::instrument(fields(pop_key), skip_all)]
    pub fn try_new(pop_key: POPKey, _: &HashAgg, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let ctxt = HashAggContext { pop_key, children, partition_id, splits: vec![], output_split: 0 };
        debug!("{:?}", ctxt);
        Ok(Box::new(ctxt))
    }

    //#[tracing::instrument(fields(), skip_all, parent = None)]
    fn next_agg(&mut self, flow: &Flow, stage: &Stage, hash_agg: &HashAgg) -> Result<Option<ChunkBox>, String> {
        // Initialize hash-tables
        if self.splits.is_empty() {
            // Initialize splits
            if self.splits.is_empty() {
                self.splits = (0..NSPLITS).map(|_| HashAggSplit::new()).collect();
            }
        }

        // Perform the aggregation
        while let Some(chunk) = self.children[0].next(flow, stage)? {
            if !chunk.is_empty() {
                self.upsert(hash_agg, chunk)?;
            }
        }

        self.contruct_internal_output(stage, hash_agg)
    }

    fn hash_chunk(chunk: &ChunkBox, hash_agg: &HashAgg) -> Vec<u64> {
        let mut hasharr = vec![0u64; chunk.len()];
        let keylen = hash_agg.keycols[0].len();

        for array in chunk.arrays().iter().take(keylen) {
            match array.data_type().to_physical_type() {
                PhysicalType::Primitive(PrimitiveType::Int64) => {
                    let primarr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                    hasharr.iter_mut().zip(primarr.iter()).for_each(|(hashval, newval)| *hashval = *hashval ^ *newval.unwrap() as u64);
                }
                PhysicalType::Utf8 => {
                    let primarr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                    hasharr.iter_mut().zip(primarr.iter()).for_each(|(hashval, newval)| *hashval = *hashval ^ xx::hash64(newval.unwrap()));
                }
                _ => todo!(),
            }
        }
        hasharr
    }

    //#[tracing::instrument(fields(key, value), skip_all, parent = None)]
    fn upsert(&mut self, hash_agg: &HashAgg, chunk: ChunkBox) -> Result<(), String> {
        let keycols = &hash_agg.keycols[0];
        let keylen = hash_agg.keylen();
        let hash_arr = Self::hash_chunk(&chunk, hash_agg);
        let split_arr = hash_arr.iter().map(|&hash_value| hash_value as usize % NSPLITS).collect::<Vec<_>>();

        for ix in 0..chunk.len() {
            let hash_value = hash_arr[ix];
            let split_id = split_arr[ix];
            let split = &mut self.splits[split_id];

            let cmp_key = |key: &DataRow| -> bool { compare_key(&chunk, ix, key) };
            let gen_key = || -> DataRow { build_key(&chunk, keylen, ix) };

            // PERF TODO: Only allocate key if it's not in hash table
            let entry = split.hash_map.find(hash_value as usize, cmp_key, gen_key, || Self::init_accumulators(hash_agg, &chunk, ix));
            let do_init = matches!(entry, MyEntry::Inserted(_));
            if do_init {
                continue;
            }

            let bucket = entry.bucket();
            let mut key_value = split.hash_map.key_value_mut(bucket);
            let (key, accumulators) = &mut key_value.as_mut().unwrap();

            //for (accnum, &(Agg { agg_type, input_colid, .. }, _)) in hash_agg.aggs.iter().enumerate() {
            for (acc, &(Agg { agg_type, input_colid, .. }, _)) in accumulators.iter_mut().zip(hash_agg.aggs.iter()) {
                let array = &chunk.arrays()[input_colid].as_any();
                let input_type = &hash_agg.child_physical_types[input_colid];
                match (agg_type, input_type) {
                    (AggType::COUNT, _) => {
                        acc.as_mut().unwrap().add_i64(1);
                    }
                    (AggType::SUM, PhysicalType::Primitive(PrimitiveType::Int64)) => {
                        let primarr = array.downcast_ref::<PrimitiveArray<i64>>().unwrap();
                        let cur_value = primarr.get(ix).unwrap();
                        let old_sum = acc.as_ref().map_or(0, |e| e.try_as_i64().unwrap());
                        *acc = Some(Int64(old_sum + cur_value));
                    }
                    (AggType::SUM, PhysicalType::Primitive(PrimitiveType::Float64)) => {
                        let primarr = array.downcast_ref::<PrimitiveArray<f64>>().unwrap();
                        let cur_value = primarr.get(ix).unwrap();
                        acc.as_mut().unwrap().add_f64(cur_value);
                    }
                    (AggType::MAX | AggType::MIN, PhysicalType::Primitive(PrimitiveType::Int64)) => {
                        let primarr = array.downcast_ref::<PrimitiveArray<i64>>().unwrap();
                        let cur_datum = primarr.get(ix).map(|ival| Int64(ival));
                        if let Some(Int64(cur_int)) = cur_datum {
                            if let Some(Int64(acc_int)) = acc {
                                if (agg_type == AggType::MAX) && (cur_int > *acc_int) {
                                    *acc = cur_datum;
                                } else if (agg_type == AggType::MIN) && (cur_int < *acc_int) {
                                    *acc = cur_datum;
                                }
                            } else {
                                // Accumulator is NULL
                                todo!("What to do here?")
                            }
                        } else {
                            // Current value is NULL
                            todo!("What to do here?")
                        }
                    }
                    (AggType::MAX | AggType::MIN, PhysicalType::Utf8) => {
                        let primarr = array.downcast_ref::<Utf8Array<i32>>().unwrap();
                        let cur_datum = primarr.get(ix).map(|sval| Utf8(sval.to_string()));
                        if let Some(Utf8(cur_str)) = cur_datum.as_ref() {
                            if let Some(Utf8(acc_str)) = acc {
                                let cur_str = cur_str.as_str();
                                let acc_str = acc_str.as_str();
                                if (agg_type == AggType::MAX) && (cur_str > acc_str) {
                                    *acc = cur_datum;
                                } else if (agg_type == AggType::MIN) && (cur_str < acc_str) {
                                    *acc = cur_datum;
                                }
                            } else {
                                // Accumulator is NULL
                                todo!("What to do here?")
                            }
                        } else {
                            // Current value is NULL
                            todo!("What to do here?")
                        }
                    }
                    _ => panic!("HashAggContext::insert(): Combination of {:?} not yet supported", (agg_type, input_type)),
                }
            }
        }

        Ok(())
    }

    fn init_accumulators(hash_agg: &HashAgg, chunk: &ChunkBox, ix: usize) -> DataRow {
        let mut accumulators = vec![];
        for &(Agg { agg_type, input_colid, .. }, _) in hash_agg.aggs.iter() {
            let array = &chunk.arrays()[input_colid].as_any();
            let input_type = &hash_agg.child_physical_types[input_colid];

            match (agg_type, input_type) {
                (AggType::COUNT, _) => {
                    accumulators.push(Some(Int64(1)));
                }
                (AggType::SUM, PhysicalType::Primitive(PrimitiveType::Int64)) => {
                    let primarr = array.downcast_ref::<PrimitiveArray<i64>>().unwrap();
                    let cur_value = primarr.get(ix).unwrap();
                    accumulators.push(Some(Int64(cur_value)));
                }
                (AggType::SUM, PhysicalType::Primitive(PrimitiveType::Float64)) => {
                    let primarr = array.downcast_ref::<PrimitiveArray<f64>>().unwrap();
                    let cur_value = primarr.get(ix).unwrap();
                    accumulators.push(Some(Float64(F64::from(cur_value))));
                }
                (AggType::MAX | AggType::MIN, PhysicalType::Primitive(PrimitiveType::Int64)) => {
                    let primarr = array.downcast_ref::<PrimitiveArray<i64>>().unwrap();
                    let cur_datum = primarr.get(ix).map(|ival| Int64(ival));
                    if cur_datum.is_some() {
                        accumulators.push(cur_datum);
                    } else {
                        // Current value is NULL
                        accumulators.push(None);
                    }
                }
                (AggType::MAX | AggType::MIN, PhysicalType::Utf8) => {
                    let primarr = array.downcast_ref::<Utf8Array<i32>>().unwrap();
                    let cur_datum = primarr.get(ix).map(|sval| Utf8(sval.to_string()));
                    if cur_datum.is_some() {
                        accumulators.push(cur_datum);
                    } else {
                        // Current value is NULL
                        accumulators.push(None);
                    }
                }
                _ => panic!("HashAggContext::init_accumulators(): Combination of {:?} not yet supported", (agg_type, input_type)),
            }
        }
        accumulators
    }

    fn init_mutable_array(data_type: &DataType, len: usize) -> Box<dyn MutableArray> {
        match data_type {
            DataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(len)),
            DataType::Date32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(len).to(DataType::Date32)),
            DataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::with_capacity(len)),
            DataType::Utf8 => Box::new(MutableUtf8Array::<i32>::with_capacity(len)),
            DataType::Boolean => Box::new(MutableBooleanArray::with_capacity(len)),
            DataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::with_capacity(len)),
            typ => todo!("not implemented: {:?}", typ),
        }
    }

    fn append_mutable_array(mutarr: &mut Box<dyn MutableArray>, datum: Option<&Datum>) {
        match mutarr.data_type().to_physical_type() {
            PhysicalType::Primitive(PrimitiveType::Int32) => {
                let mutarr = mutarr.as_mut_any().downcast_mut::<MutablePrimitiveArray<i32>>().unwrap();
                mutarr.push(datum.map(|ivalue| ivalue.try_as_i32().unwrap()));
            }
            PhysicalType::Primitive(PrimitiveType::Int64) => {
                let mutarr = mutarr.as_mut_any().downcast_mut::<MutablePrimitiveArray<i64>>().unwrap();
                mutarr.push(datum.map(|ivalue| ivalue.try_as_i64().unwrap()));
            }
            PhysicalType::Primitive(PrimitiveType::Float64) => {
                let mutarr = mutarr.as_mut_any().downcast_mut::<MutablePrimitiveArray<f64>>().unwrap();
                mutarr.push(datum.map(|ivalue| ivalue.try_as_f64().unwrap()));
            }
            PhysicalType::Utf8 => {
                let mutarr = mutarr.as_mut_any().downcast_mut::<MutableUtf8Array<i32>>().unwrap();
                let a = datum.map(|ivalue| ivalue.to_string());
                mutarr.push(a);
            }

            _ => todo!(),
        }
    }

    fn convert_mutarr_to_immutable(mutarrays: Vec<Box<dyn MutableArray>>) -> Vec<Box<dyn Array>> {
        mutarrays
            .into_iter()
            .map(|mutarr| {
                let data_type = mutarr.data_type();
                match data_type {
                    DataType::Int32 | DataType::Date32 => {
                        let mutarr = mutarr.as_any().downcast_ref::<MutablePrimitiveArray<i32>>().unwrap().clone();
                        let iter = mutarr.iter().map(|i| i.cloned());
                        let arr = PrimitiveArray::<i32>::from_trusted_len_iter(iter);
                        let arr = if *data_type == DataType::Date32 { arr.to(DataType::Date32) } else { arr };
                        let arr: Box<dyn Array> = Box::new(arr);
                        arr
                    }
                    DataType::Int64 => {
                        let mutarr = mutarr.as_any().downcast_ref::<MutablePrimitiveArray<i64>>().unwrap().clone();
                        let iter = mutarr.iter().map(|i| i.cloned());
                        let arr = PrimitiveArray::<i64>::from_trusted_len_iter(iter);
                        let arr: Box<dyn Array> = Box::new(arr);
                        arr
                    }
                    DataType::Float64 => {
                        let mutarr = mutarr.as_any().downcast_ref::<MutablePrimitiveArray<f64>>().unwrap().clone();
                        let iter = mutarr.iter().map(|i| i.cloned());
                        let arr = PrimitiveArray::<f64>::from_trusted_len_iter(iter);
                        let arr: Box<dyn Array> = Box::new(arr);
                        arr
                    }
                    DataType::Utf8 => {
                        let mutarr = mutarr.as_any().downcast_ref::<MutableUtf8Array<i32>>().unwrap().clone();
                        let iter = mutarr.iter().map(|s| s.to_owned());
                        let arr = Utf8Array::<i32>::from_trusted_len_iter(iter);
                        let arr: Box<dyn Array> = Box::new(arr);
                        arr
                    }
                    _ => todo!(),
                }
            })
            .collect()
    }

    fn contruct_internal_output(&mut self, stage: &Stage, hash_agg: &HashAgg) -> Result<Option<ChunkBox>, String> {
        let props = stage.pop_graph.get_properties(self.pop_key);

        while self.output_split < self.splits.len() {
            let split = &self.splits[self.output_split];
            debug!("[{:?}, p={}] Final hash_map: {:?}", self.pop_key, self.partition_id, split.hash_map);
            self.output_split += 1;

            if split.hash_map.len() > 0 {
                // Build internal output arrays
                let nelements = split.hash_map.len();
                let mut arrays: Vec<Box<dyn MutableArray>> = Vec::with_capacity(hash_agg.keylen() + hash_agg.aggs.len());
                for ix in 0..hash_agg.keylen() {
                    let mutarr = Self::init_mutable_array(&hash_agg.child_data_types[ix], nelements);
                    arrays.push(mutarr);
                }
                for agg in hash_agg.aggs.iter() {
                    let mutarr = Self::init_mutable_array(&agg.0.output_data_type, nelements);
                    arrays.push(mutarr);
                }

                // Populate arrays
                debug!("[{:?}, p={}] Final hash_map: {:?}", self.pop_key, self.partition_id, split.hash_map);
                for (key, accumulators) in split.hash_map.iter() {
                    let keylen = hash_agg.keylen();
                    for (kx, key) in key.iter().enumerate() {
                        let mutarr = &mut arrays[kx];
                        Self::append_mutable_array(mutarr, key.as_ref());
                    }
                    for (ax, acc) in accumulators.iter().enumerate() {
                        let mutarr = &mut arrays[ax + keylen];
                        Self::append_mutable_array(mutarr, acc.as_ref());
                    }
                }

                let arrays = Self::convert_mutarr_to_immutable(arrays);
                let chunk = Chunk::new(arrays);
                chunk_to_string(&chunk, "Aggregation internal output");

                // Run predicates, if any
                let chunk = POPKey::eval_predicates(props, chunk);

                let projection_chunk = POPKey::eval_projection(props, &chunk);
                debug!("hash_agg projection: \n{}", chunk_to_string(&projection_chunk, "hash_agg projection"));

                return Ok(Some(projection_chunk));
            }
        }
        return Ok(None);
    }
}

fn compare_key(chunk: &ChunkBox, ix: usize, key: &DataRow) -> bool {
    let keylen = key.len();
    for (array, keydatum) in chunk.arrays().iter().take(keylen).zip(key.iter()) {
        let keydatum = keydatum.as_ref();
        let cmpstat = match array.data_type() {
            DataType::Utf8 => {
                let basearr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let e = basearr.get(ix);
                match (&e, &keydatum) {
                    (Some(s1), Some(Utf8(s2))) => (s1 == s2),
                    (None, None) => true,
                    _ => false,
                }
            }
            typ => panic!("array_to_iter(), todo: {:?}", typ),
        };
        if !cmpstat {
            // Comparison failed ... bail out
            return false;
        }
    }
    true
}

fn build_key(chunk: &ChunkBox, keylen: usize, ix: usize) -> Vec<Option<Datum>> {
    let mut key = Vec::with_capacity(keylen);
    for array in chunk.arrays().iter().take(keylen) {
        match array.data_type() {
            DataType::Date32 => {
                let basearr = array.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                let e = basearr.get(ix).map(|e| Date32(e));
                key.push(e)
            }
            DataType::Int64 => {
                let basearr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                let e = basearr.get(ix).map(|e| Int64(e));
                key.push(e)
            }
            DataType::Utf8 => {
                let basearr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let e = basearr.get(ix).map(|e| Utf8(e.to_string()));
                key.push(e)
            }
            DataType::Boolean => {
                let basearr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let e = basearr.get(ix).map(|e| Boolean(e));
                key.push(e)
            }
            DataType::Float64 => {
                let basearr = array.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                let e = basearr.get(ix).map(|e| Float64(F64::from(e)));
                key.push(e)
            }
            typ => panic!("array_to_iter(), todo: {:?}", typ),
        }
    }
    key
}

impl POPContext for HashAggContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn next(&mut self, flow: &Flow, stage: &Stage) -> Result<Option<ChunkBox>, String> {
        let pop_key = self.pop_key;
        let pop = stage.pop_graph.get_value(pop_key);

        if let POP::HashAgg(hash_agg) = pop {
            self.next_agg(flow, stage, hash_agg)
        } else {
            panic!("ugh");
        }
    }
}

#[derive(Debug)]
struct MyHashTable<K, V>
where
    K: PartialEq + Eq + Hash + Clone,
    V: Clone, {
    noccupied: usize,
    hvec: Vec<Option<(K, V)>>,
}

#[derive(Debug, Clone, Copy)]
enum MyEntry {
    Found(usize),
    Inserted(usize),
}

impl MyEntry {
    fn bucket(&self) -> usize {
        match self {
            MyEntry::Found(ix) => *ix,
            MyEntry::Inserted(ix) => *ix,
        }
    }
}

impl<K, V> MyHashTable<K, V>
where
    K: PartialEq + Eq + Hash + Clone,
    V: Clone,
{
    fn new(default_size: usize) -> Self {
        MyHashTable { noccupied: 0, hvec: vec![None; default_size] }
    }

    fn len(&self) -> usize {
        self.noccupied
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &(K, V)> + '_> {
        let it = self.hvec.iter().filter_map(|e| e.as_ref());
        Box::new(it)
    }

    fn key_value_mut(&mut self, bucket: usize) -> Option<&mut (K, V)> {
        let r = self.hvec[bucket].as_mut();
        r
    }

    fn find<C, G, D>(&mut self, hsh: usize, cmp_key: C, gen_key: G, default_v: D) -> MyEntry
    where
        C: Fn(&K) -> bool,
        G: Fn() -> K,
        D: Fn() -> V, {
        // Ensure minimum 50% occupancy
        debug_assert!(self.noccupied < self.hvec.len() / 2);

        // Compute hash bucket
        let mut bucket = hsh % self.hvec.len();

        // Linear probing
        loop {
            if let Some(entry) = &mut self.hvec[bucket] {
                if cmp_key(&entry.0) {
                    return MyEntry::Found(bucket);
                } else {
                    if bucket == self.hvec.len() - 1 {
                        // Wrap-around
                        bucket = 0;
                    } else {
                        bucket += 1;
                    }
                }
            } else {
                // Not found ... insert
                let key = gen_key();
                self.hvec[bucket] = Some((key, default_v()));
                self.noccupied += 1;
                return MyEntry::Inserted(bucket);
            }
        }
    }
}
