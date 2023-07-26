// pop_hashagg

//#![allow(warnings)]

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    rc::Rc,
};

use ahash::RandomState;
use arrow2::{array::Utf8Array, datatypes::PhysicalType, types::PrimitiveType};

use crate::{
    datum::Datum,
    expr::AggType,
    flow::Flow,
    graph::POPKey,
    includes::*,
    pop::{chunk_to_string, Agg, POPContext, POP},
    pop_hash::{SplitId, NSPLITS},
    stage::Stage,
};

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct HashAgg {
    pub keycols: Vec<Vec<ColId>>, // Maintain a list of key columns for each child. len() == 2 for joins
    pub child_data_types: Vec<DataType>,
    pub aggs: Vec<(Agg, ColId)>, // ColId represents offset into
}

/***************************************************************************************************/
struct HashAggSplit {
    id: SplitId,
    hash_map: HashMap<Vec<Option<Datum>>, Vec<Option<Datum>>>, // Hash-of-keys -> Accumulators
}

impl HashAggSplit {
    fn new(id: SplitId) -> Self {
        HashAggSplit { id, hash_map: HashMap::new() }
    }
}
/***************************************************************************************************/

pub struct HashAggContext {
    pop_key: POPKey,
    children: Vec<Box<dyn POPContext>>,
    partition_id: PartitionId,
    state: RandomState,
    splits: Vec<HashAggSplit>,
}

impl HashAggContext {
    pub fn try_new(pop_key: POPKey, _: &HashAgg, children: Vec<Box<dyn POPContext>>, partition_id: PartitionId) -> Result<Box<dyn POPContext>, String> {
        let state = RandomState::with_seeds(97, 31, 45, 21);

        Ok(Box::new(HashAggContext { pop_key, children, partition_id, state, splits: vec![] }))
    }

    fn next_agg(&mut self, flow: &Flow, stage: &Stage, hash_agg: &HashAgg) -> Result<Option<ChunkBox>, String> {
        // Build hash-tables
        if self.splits.is_empty() {
            // Initialize splits
            if self.splits.is_empty() {
                self.splits = (0..NSPLITS).map(|split_id| HashAggSplit::new(split_id)).collect();
            }
        }

        // Aggregate
        let child = &mut self.children[0];
        while let Some(chunk) = child.next(flow, stage)? {
            if !chunk.is_empty() {
                let chunk = self.process_agg_input(flow, stage, hash_agg, chunk)?;
                debug!("HashAggContext::next \n{}", chunk_to_string(&chunk, "HashAggContext::next"));
                return Ok(Some(chunk));
            }
        }
        Ok(None)
    }

    fn process_agg_input(&mut self, flow: &Flow, stage: &Stage, hash_agg: &HashAgg, chunk: ChunkBox) -> Result<ChunkBox, String> {
        let props = stage.pop_graph.get_properties(self.pop_key);
        let keycols = &hash_agg.keycols[0];

        let mut iters = chunk.arrays().iter().map(|array| array_to_iter(&**array)).collect::<Vec<_>>();
        for _ in 0..chunk.len() {
            let key = iters.iter_mut().take(keycols.len()).map(|it| it.next().unwrap()).collect::<Vec<_>>();
            let value = iters.iter_mut().skip(keycols.len()).map(|it| it.next().unwrap()).collect::<Vec<_>>();
            self.insert(hash_agg, key.clone(), value.clone());
        }
        for split in self.splits.iter() {
            debug!("Final hash_map: {:?}", split.hash_map);
        }
        // Extract values
        todo!()
    }

    fn insert(&mut self, hash_agg: &HashAgg, key: Vec<Option<Datum>>, value: Vec<Option<Datum>>) {
        let mut hasher = DefaultHasher::new();

        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        let split_id = hash_value as usize % NSPLITS;

        debug!("split_id = {}, key = {:?} value = {:?}", split_id, key, value);

        let split = &mut self.splits[split_id];

        let accumulators = split.hash_map.entry(key.clone()).or_insert_with(|| vec![]);
        let do_init = accumulators.len() == 0;
        let keylen = hash_agg.keycols[0].len();

        let get_input = |colid: ColId| {
            if colid < keylen {
                key[colid].as_ref()
            } else {
                value[colid - keylen].as_ref()
            }
        };

        for (accnum, &(Agg { agg_type, input_colid }, _)) in hash_agg.aggs.iter().enumerate() {
            let input_type = &hash_agg.child_data_types[input_colid].to_physical_type();

            match (agg_type, input_type) {
                (AggType::COUNT, _) => {
                    if do_init {
                        accumulators.push(Some(Datum::INT(1isize)));
                    } else {
                        let acc = &mut accumulators[accnum];
                        *acc = Some(Datum::INT(1isize));
                    }
                }
                (AggType::SUM, _) => {
                    let cur_value = get_input(input_colid).map_or(0, |e| e.as_isize());
                    if do_init {
                        accumulators.push(Some(Datum::INT(cur_value)));
                    } else {
                        let acc = &mut accumulators[accnum];
                        let old_sum = acc.as_ref().map_or(0, |e| e.as_isize());
                        *acc = Some(Datum::INT(old_sum + cur_value));
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
                        if let Some(Datum::INT(cur_int)) = cur_datum {
                            let acc = &mut accumulators[accnum];
                            if let Some(Datum::INT(acc_int)) = acc {
                                if (agg_type == AggType::MAX) && (cur_int > acc_int) {
                                    *acc = cur_datum.cloned()
                                } else if (agg_type == AggType::MIN) && (cur_int < acc_int) {
                                    *acc = cur_datum.cloned()
                                }
                            } else {
                                // Current value is NULL
                                todo!("What to do here?")
                            }
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
                        if let Some(Datum::STR(cur_str)) = cur_datum {
                            let acc = &mut accumulators[accnum];
                            if let Some(Datum::STR(acc_str)) = acc {
                                let cur_str = &*cur_str.as_str();
                                let acc_str = &*acc_str.as_str();
                                if (agg_type == AggType::MAX) && (cur_str > acc_str) {
                                    *acc = cur_datum.cloned()
                                } else if (agg_type == AggType::MIN) && (cur_str < acc_str) {
                                    *acc = cur_datum.cloned()
                                }
                            } else {
                                // Current value is NULL
                                todo!("What to do here?")
                            }
                        }
                    }
                }
                _ => panic!("HashAggContext::insert(): Combination of {:?} not yet supported", (agg_type, input_type)),
            }
        }
    }
}

fn array_to_iter(array: &dyn Array) -> Box<dyn Iterator<Item = Option<Datum>> + '_> {
    match array.data_type().to_physical_type() {
        PhysicalType::Primitive(PrimitiveType::Int64) => {
            let basearr = array.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            let it = basearr.iter().map(|e| e.map(|&e| Datum::INT(e as isize)));
            Box::new(it)
        }
        PhysicalType::Utf8 => {
            let basearr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let it = basearr.iter().map(|e| e.map(|e| Datum::STR(Rc::new(String::from(e)))));
            Box::new(it)
        }
        PhysicalType::Boolean => {
            let basearr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let it = basearr.iter().map(|e| e.map(|e| Datum::BOOL(e)));
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
