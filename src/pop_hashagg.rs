// pop_hashagg

//#![allow(warnings)]

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};

use arrow2::{
    array::{MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array},
    datatypes::PhysicalType,
    types::PrimitiveType,
};

use crate::{
    expr::AggType,
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, Agg, POPContext, POP},
    pop_hash::NSPLITS,
    stage::Stage,
    Datum,
};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashAgg {
    pub keycols: Vec<Vec<ColId>>, // Maintain a list of key columns for each child. len() == 2 for joins
    pub child_data_types: Vec<DataType>,
    pub aggs: Vec<(Agg, ColId)>, // ColId represents the ordering of each aggregator and starts at keylen
}

impl HashAgg {
    pub fn keylen(&self) -> usize {
        self.keycols[0].len()
    }
}
/***************************************************************************************************/
struct HashAggSplit {
    hash_map: HashMap<Vec<Option<Datum>>, Vec<Option<Datum>>>, // Hash-of-keys -> Accumulators
}

impl HashAggSplit {
    fn new() -> Self {
        HashAggSplit { hash_map: HashMap::new() }
    }
}
/***************************************************************************************************/

pub struct HashAggContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    splits: Vec<HashAggSplit>,
    output_split: usize,
}

impl HashAggContext {
    pub fn try_new(pop_key: POPKey, _: &HashAgg, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        Ok(Box::new(HashAggContext { pop_key, children, partition_id, splits: vec![], output_split: 0 }))
    }

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
                self.perform_aggregation(flow, stage, hash_agg, chunk)?;
            }
        }

        self.contruct_internal_output(stage, hash_agg)
    }

    fn perform_aggregation(&mut self, _: &Flow, _stage: &Stage, hash_agg: &HashAgg, chunk: ChunkBox) -> Result<(), String> {
        let keycols = &hash_agg.keycols[0];

        let mut iters = chunk.arrays().iter().map(|array| array_to_iter(&**array)).collect::<Vec<_>>();
        for _ in 0..chunk.len() {
            let key = iters.iter_mut().take(keycols.len()).map(|it| it.next().unwrap()).collect::<Vec<_>>();
            let value = iters.iter_mut().skip(keycols.len()).map(|it| it.next().unwrap()).collect::<Vec<_>>();
            self.insert(hash_agg, key.clone(), value.clone());
        }

        Ok(())
    }

    fn init_mutable_array(data_type: &DataType, len: usize) -> Box<dyn MutableArray> {
        match data_type {
            DataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(len)),
            DataType::Date32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(len).to(DataType::Date32)),
            DataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::with_capacity(len)),
            DataType::Utf8 => Box::new(MutableUtf8Array::<i32>::with_capacity(len)),
            DataType::Boolean => Box::new(MutableBooleanArray::with_capacity(len)),
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

    #[tracing::instrument(fields(key, value), skip_all, parent = None)]
    fn insert(&mut self, hash_agg: &HashAgg, key: Vec<Option<Datum>>, value: Vec<Option<Datum>>) {
        let mut hasher = DefaultHasher::new();

        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        let split_id = hash_value as usize % NSPLITS;

        debug!("split_id = {}, key = {:?} value = {:?}", split_id, key, value);

        let split = &mut self.splits[split_id];

        let accumulators = split.hash_map.entry(key.clone()).or_insert_with(|| vec![]);
        let do_init = accumulators.len() == 0;
        let keylen = hash_agg.keylen();

        let get_input = |colid: ColId| {
            if colid < keylen {
                key[colid].as_ref()
            } else {
                value[colid - keylen].as_ref()
            }
        };

        for (accnum, &(Agg { agg_type, input_colid, .. }, _)) in hash_agg.aggs.iter().enumerate() {
            let input_type = &hash_agg.child_data_types[input_colid].to_physical_type();
            match (agg_type, input_type) {
                (AggType::COUNT, _) => {
                    if do_init {
                        accumulators.push(Some(Int64(1)));
                    } else {
                        let acc = &mut accumulators[accnum];
                        let new_count = acc.as_ref().unwrap().try_as_i64().unwrap() + 1;
                        *acc = Some(Int64(new_count))
                    }
                }
                (AggType::SUM, _) => {
                    let cur_value = get_input(input_colid).map_or(0, |e| e.try_as_i64().unwrap());
                    if do_init {
                        accumulators.push(Some(Int64(cur_value)));
                    } else {
                        let acc = &mut accumulators[accnum];
                        let old_sum = acc.as_ref().map_or(0, |e| e.try_as_i64().unwrap());
                        *acc = Some(Int64(old_sum + cur_value));
                    }
                }
                (AggType::MAX | AggType::MIN, PhysicalType::Primitive(PrimitiveType::Int64)) => {
                    let cur_datum = get_input(input_colid);
                    if do_init {
                        if cur_datum.is_some() {
                            accumulators.push(cur_datum.cloned());
                        } else {
                            // Current value is NULL
                            accumulators.push(None);
                        }
                    } else {
                        if let Some(Int64(cur_int)) = cur_datum {
                            let acc = &mut accumulators[accnum];
                            if let Some(Int64(acc_int)) = acc {
                                if (agg_type == AggType::MAX) && (cur_int > acc_int) {
                                    *acc = cur_datum.cloned()
                                } else if (agg_type == AggType::MIN) && (cur_int < acc_int) {
                                    *acc = cur_datum.cloned()
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
                }
                (AggType::MAX | AggType::MIN, PhysicalType::Primitive(PrimitiveType::Int32)) => {
                    let cur_datum = get_input(input_colid);
                    if do_init {
                        if cur_datum.is_some() {
                            accumulators.push(cur_datum.cloned());
                        } else {
                            // Current value is NULL
                            accumulators.push(None);
                        }
                    } else {
                        if let Some(cur_int) = cur_datum {
                            let cur_int = cur_int.try_as_i32();
                            let acc = &mut accumulators[accnum];
                            if let Some(acc_int) = acc {
                                let acc_int = acc_int.try_as_i32();
                                if (agg_type == AggType::MAX) && (cur_int > acc_int) {
                                    *acc = cur_datum.cloned()
                                } else if (agg_type == AggType::MIN) && (cur_int < acc_int) {
                                    *acc = cur_datum.cloned()
                                }
                            } else {
                                // Accumulator is NULL
                                todo!("What to do here?")
                            }
                        } else {
                            // Current value is NULL ... How do you compare NULLs to something else?
                            todo!("What to do here?")
                        }
                    }
                }

                (AggType::MAX | AggType::MIN, PhysicalType::Utf8) => {
                    let cur_datum = get_input(input_colid);
                    if do_init {
                        if cur_datum.is_some() {
                            accumulators.push(cur_datum.cloned());
                        } else {
                            // Current value is NULL
                            accumulators.push(None);
                        }
                    } else {
                        if let Some(Utf8(cur_str)) = cur_datum {
                            let acc = &mut accumulators[accnum];
                            if let Some(Utf8(acc_str)) = acc {
                                let cur_str = cur_str.as_str();
                                let acc_str = acc_str.as_str();
                                if (agg_type == AggType::MAX) && (cur_str > acc_str) {
                                    *acc = cur_datum.cloned()
                                } else if (agg_type == AggType::MIN) && (cur_str < acc_str) {
                                    *acc = cur_datum.cloned()
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
                }
                _ => panic!("HashAggContext::insert(): Combination of {:?} not yet supported", (agg_type, input_type)),
            }
        }
    }
}

fn array_to_iter(array: &dyn Array) -> Box<dyn Iterator<Item = Option<Datum>> + '_> {
    match array.data_type() {
        DataType::Date32 => {
            let basearr = array.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            let it = basearr.iter().map(|e| e.map(|&e| Date32(e)));
            Box::new(it)
        }
        DataType::Int64 => {
            let basearr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            let it = basearr.iter().map(|e| e.map(|&e| Int64(e)));
            Box::new(it)
        }
        DataType::Utf8 => {
            let basearr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let it = basearr.iter().map(|e| e.map(|e| Utf8(e.to_string())));
            Box::new(it)
        }
        DataType::Boolean => {
            let basearr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let it = basearr.iter().map(|e| e.map(|e| Boolean(e)));
            Box::new(it)
        }
        typ => panic!("array_to_iter(), todo: {:?}", typ),
    }
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

#[test]
fn foo() {
    use std::collections::HashMap;

    let mut map: HashMap<&str, u32> = HashMap::new();

    let e = map.entry("poneyland");

    e.and_modify(|e| {
        *e += 1;
    })
    .or_insert(42);
    assert_eq!(map["poneyland"], 42);
}
