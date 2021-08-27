#![allow(warnings)]

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::thread::JoinHandle;
use typed_arena::Arena;

use crate::csv22::*;
use crate::expr::{Expr::*, *};
use crate::graphviz::{htmlify, write_flow_to_graphviz};
use crate::includes::*;
use crate::row::*;
use crate::task::*;

pub trait Node2: serde_traitobject::Serialize {}

#[derive(Serialize)]
struct EmitNode2 {
    id: NodeId,
}

impl Node2 for EmitNode2 {}

#[derive(Serialize)]
pub struct Flow2 {
    #[serde(with = "serde_traitobject")]
    pub nodes: Box<dyn Node2>,
}

impl Node2 for EmitNode {}
impl Node2 for ProjectNode {}
impl Node2 for FilterNode {}
impl Node2 for CSVNode {}
impl Node2 for JoinNode {}
impl Node2 for AggNode {}

/***************************************************************************************************/
type NodeArena = Arena<Box<dyn Node>>;
pub trait Node: serde_traitobject::Serialize {
    fn emit<'a>(&self, arena: &'a NodeArena) -> &'a Box<dyn Node> {
        let npartitions = self.base().npartitions;
        let base = NodeBase::new1(&arena, self.id(), npartitions);
        let retval = arena.alloc(Box::new(EmitNode { base }));
        retval
    }

    fn project<'a>(
        &self, arena: &'a NodeArena, colids: Vec<ColId>,
    ) -> &'a Box<dyn Node> {
        let npartitions = self.base().npartitions;
        let base = NodeBase::new1(&arena, self.id(), npartitions);
        let retval = arena.alloc(Box::new(ProjectNode { base, colids }));
        retval
    }

    fn filter<'a>(
        &self, arena: &'a NodeArena, expr: Expr,
    ) -> &'a Box<dyn Node> {
        let npartitions = self.base().npartitions;
        let base = NodeBase::new1(&arena, self.id(), npartitions);
        let retval = arena.alloc(Box::new(FilterNode::new(base, expr)));
        retval
    }

    fn join<'a>(
        &self, arena: &'a NodeArena, other_children: Vec<&Box<dyn Node>>,
        preds: Vec<JoinPredicate>,
    ) -> &'a Box<dyn Node> {
        let npartitions = self.base().npartitions;
        let base =
            NodeBase::new(&arena, self.id(), other_children, npartitions);
        let retval = arena.alloc(Box::new(JoinNode { base, preds }));
        retval
    }

    fn agg<'a>(
        &self, arena: &'a NodeArena, keycolids: Vec<ColId>,
        aggcolids: Vec<(AggType, ColId)>, npartitions: usize,
    ) -> &'a Box<dyn Node> {
        let base = NodeBase::new1(&arena, self.id(), npartitions);
        let retval = arena.alloc(Box::new(AggNode {
            base,
            keycolids,
            aggcolids,
        }));
        retval
    }

    fn base(&self) -> &NodeBase;

    fn desc(&self) -> String;

    fn id(&self) -> NodeId {
        self.base().id
    }

    fn children(&self) -> &Vec<NodeId> {
        &self.base().children
    }

    fn nchildren(&self) -> usize {
        self.base().children.len()
    }

    fn child<'a>(&self, flow: &'a Flow, ix: NodeId) -> &'a Box<dyn Node> {
        let children = &self.base().children;
        flow.get_node(children[ix])
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row>;

    fn is_endpoint(&self) -> bool {
        false
    }

    fn npartitions(&self) -> usize {
        self.base().npartitions
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
pub struct NodeBase {
    id: NodeId,
    children: Vec<NodeId>,
    npartitions: usize,
}

impl NodeBase {
    fn new0(arena: &NodeArena, npartitions: usize) -> NodeBase {
        let id = arena.len();
        NodeBase {
            id,
            children: vec![],
            npartitions,
        }
    }

    fn new1(
        arena: &NodeArena, child_id: NodeId, npartitions: usize,
    ) -> NodeBase {
        let id = arena.len();
        NodeBase {
            id,
            children: vec![child_id],
            npartitions,
        }
    }

    fn new(
        arena: &NodeArena, child_id: NodeId,
        other_children: Vec<&Box<dyn Node>>, npartitions: usize,
    ) -> NodeBase {
        let id = arena.len();
        let mut children: Vec<_> =
            other_children.iter().map(|e| e.id()).collect();
        children.push(child_id);
        NodeBase {
            id,
            children,
            npartitions,
        }
    }
}

/***************************************************************************************************/
pub enum NodeRuntime {
    Unused,
    CSV { iter: CSVPartitionIter },
}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
struct EmitNode {
    base: NodeBase,
}

impl Node for EmitNode {
    fn desc(&self) -> String {
        format!("EmitNode-#{}", self.id())
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        self.child(flow, 0).next(flow, stage, task, false)
    }

    fn is_endpoint(&self) -> bool {
        true
    }
}

impl EmitNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
struct CSVNode {
    base: NodeBase,
    filename: String,
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
    partitions: Vec<TextFilePartition>,
}

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

impl CSVNode {
    fn new(
        arena: &NodeArena, filename: String, npartitions: usize,
    ) -> &Box<dyn Node> {
        let (colnames, coltypes) = Self::infer_metadata(&filename);
        let base = NodeBase::new0(&arena, npartitions);

        let partitions =
            compute_partitions(&filename, npartitions as u64).unwrap();
        let retval = arena.alloc(Box::new(CSVNode {
            base,
            filename,
            colnames,
            coltypes,
            partitions,
        }));
        retval
    }

    fn infer_datatype(str: &String) -> DataType {
        let res = str.parse::<i32>();
        if res.is_ok() {
            DataType::INT
        } else if str.eq("true") || str.eq("false") {
            DataType::BOOL
        } else {
            DataType::STR
        }
    }

    fn infer_metadata(filename: &str) -> (Vec<String>, Vec<DataType>) {
        let mut iter = read_lines(&filename).unwrap();
        let mut colnames: Vec<String> = vec![];
        let mut coltypes: Vec<DataType> = vec![];
        let mut first_row = true;

        while let Some(line) = iter.next() {
            let cols: Vec<String> =
                line.unwrap().split(',').map(|e| e.to_owned()).collect();
            if colnames.len() == 0 {
                colnames = cols;
            } else {
                for (ix, col) in cols.iter().enumerate() {
                    let datatype = CSVNode::infer_datatype(col);
                    if first_row {
                        coltypes.push(datatype)
                    } else if coltypes[ix] != DataType::STR {
                        coltypes[ix] = datatype;
                    } else {
                        coltypes[ix] = DataType::STR;
                    }
                }
                first_row = false;
            }
        }
        //dbg!(&colnames);
        //dbg!(&coltypes);
        (colnames, coltypes)
    }
}

impl Node for CSVNode {
    fn desc(&self) -> String {
        let filename =
            self.filename.split("/").last().unwrap_or(&self.filename);

        format!(
            "CSVNode-#{} (p={})|{} {:?}",
            self.id(),
            self.npartitions(),
            filename,
            self.colnames
        )
        .replace("\"", "\\\"")
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        let partition_id = task.partition_id;
        let runtime = task.contexts.entry(self.id()).or_insert_with(|| {
            let partition = &self.partitions[partition_id];
            let mut iter = CSVPartitionIter::new(&self.filename, partition);
            if partition_id == 0 {
                iter.next(); // Consume the header row
            }
            NodeRuntime::CSV { iter }
        });

        if let NodeRuntime::CSV { iter } = runtime {
            if let Some(line) = iter.next() {
                debug!("line = :{}:", &line.trim_end());
                let cols = line
                    .trim_end()
                    .split(',')
                    .enumerate()
                    .map(|(ix, col)| match self.coltypes[ix] {
                        DataType::INT => {
                            let ival = col.parse::<isize>().unwrap();
                            Datum::INT(ival)
                        }
                        DataType::STR => Datum::STR(Box::new(col.to_owned())),
                        _ => unimplemented!(),
                    })
                    .collect::<Vec<Datum>>();
                return Some(Row::from(cols));
            } else {
                return None;
            }
        }
        panic!("Cannot get NodeRuntime::CSV")
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
struct ProjectNode {
    base: NodeBase,
    colids: Vec<ColId>,
}

impl Node for ProjectNode {
    fn desc(&self) -> String {
        format!("ProjectNode-#{}|{:?}", self.id(), self.colids)
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        //let flow = task.stage().flow();
        //let flow = &*(&*task.stage).flow;

        if let Some(row) = self.child(flow, 0).next(flow, stage, task, false) {
            return Some(row.project(&self.colids));
        } else {
            return None;
        }
    }
}

impl ProjectNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
struct FilterNode {
    base: NodeBase,
    expr: Expr,
}

impl Node for FilterNode {
    fn desc(&self) -> String {
        let s = format!("FilterNode-#{}|{}", self.id(), self.expr);
        htmlify(s)
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        while let Some(e) = self.child(flow, 0).next(flow, stage, task, false) {
            if let Datum::BOOL(b) = self.expr.eval(&e) {
                if b {
                    return Some(e);
                }
            }
        }
        return None;
    }
}

impl FilterNode {
    fn new(base: NodeBase, expr: Expr) -> FilterNode {
        if let Expr::RelExpr(..) = expr {
            FilterNode { base, expr }
        } else {
            panic!("Invalid filter expression")
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
struct JoinNode {
    base: NodeBase,
    preds: Vec<JoinPredicate>, // (left-column,[eq],right-column)*
}

type JoinPredicate = (ColId, RelOp, ColId);

impl Node for JoinNode {
    fn desc(&self) -> String {
        let s = format!("JoinNode-#{}|{:?}", self.id(), self.preds);
        htmlify(s)
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        None
    }
}

impl JoinNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize)]
struct AggNode {
    base: NodeBase,
    keycolids: Vec<ColId>,
    aggcolids: Vec<(AggType, ColId)>,
}

impl Node for AggNode {
    fn desc(&self) -> String {
        let s = format!(
            "AggNode-#{} (p={})|by = {:?}, aggs = {:?}",
            self.id(),
            self.npartitions(),
            self.keycolids,
            self.aggcolids
        );
        s
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        let htable: HashMap<Row, Row> = self.run_producer(flow, stage, task);
        None
    }

    fn is_endpoint(&self) -> bool {
        true
    }
}

use std::cmp::Ordering;

impl AggNode {
    fn run_producer_one_row(&self, accrow: &mut Row, currow: &Row) {
        for (ix, &(agg_type, agg_colid)) in self.aggcolids.iter().enumerate() {
            let acccol = accrow.get_column_mut(ix);
            let curcol = currow.get_column(agg_colid);

            match agg_type {
                AggType::COUNT => {
                    *acccol = Datum::INT(acccol.as_int() + 1);
                }
                AggType::SUM => {
                    *acccol = Datum::INT(acccol.as_int() + curcol.as_int());
                }
                AggType::MIN => {
                    if curcol.cmp(&acccol) == Ordering::Less {
                        accrow.set_column(ix, &curcol)
                    }
                }
                AggType::MAX => {
                    if curcol.cmp(&acccol) == Ordering::Greater {
                        accrow.set_column(ix, &curcol)
                    }
                }
            }
        }
    }

    fn run_producer(
        &self, flow: &Flow, stage: &Stage, task: &mut Task,
    ) -> HashMap<Row, Row> {
        let mut htable: HashMap<Row, Row> = HashMap::new();
        let child = self.child(&*flow, 0);

        while let Some(currow) = child.next(flow, stage, task, false) {
            // build key
            let key = currow.project(&self.keycolids);
            //debug!("-- key = {}", key);

            let acc = htable.entry(key).or_insert_with(|| {
                let acc_cols: Vec<Datum> = self
                    .aggcolids
                    .iter()
                    .map(|&(aggtype, ix)| {
                        // Build an empty accumumator Row
                        match aggtype {
                            AggType::COUNT => Datum::INT(0),
                            AggType::SUM => Datum::INT(0),
                            AggType::MAX | AggType::MIN => {
                                currow.get_column(ix).clone()
                            }
                        }
                    })
                    .collect();
                Row::from(acc_cols)
            });
            AggNode::run_producer_one_row(self, acc, &currow);
            //debug!("   acc = {}", acc);
        }
        for (k, v) in htable.iter() {
            write_partition(flow, stage, task, v);
        }
        htable
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum AggType {
    COUNT,
    MIN,
    MAX,
    SUM,
    //AVG,
}

/***************************************************************************************************/
//#[derive(Serialize)]
pub struct Flow {
    //#[serde(with = "serde_traitobject")]
    pub nodes: Vec<Box<dyn Node>>,
}

/*
use serde::ser::{Serialize, Serializer, SerializeSeq, SerializeMap};

impl Serialize for Flow
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(0))?;

        for e in &self.nodes {
            seq.serialize_element(&true)?;
        }
        seq.end()
    }
}
*/

impl Flow {
    pub fn get_node(&self, node_id: NodeId) -> &Box<dyn Node> {
        &self.nodes[node_id]
    }
}

/***************************************************************************************************/
pub fn make_join_flow() -> Flow {
    let arena: NodeArena = Arena::new();
    let empfilename = format!("{}/{}", DATADIR, "emp.csv").to_string();
    let deptfilename = format!("{}/{}", DATADIR, "dept.csv").to_string();

    let emp = CSVNode::new(&arena, empfilename, 4)
        .project(&arena, vec![0, 1, 2])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 1)], 3);

    let dept = CSVNode::new(&arena, deptfilename, 4);

    let join = emp.join(&arena, vec![&dept], vec![(2, RelOp::Eq, 0)]).agg(
        &arena,
        vec![0],
        vec![(AggType::COUNT, 1)],
        3,
    );
    Flow {
        nodes: arena.into_vec(),
    }
}

fn make_mvp_flow() -> Flow {
    let arena: Arena<_> = Arena::new();

    /*
        CSV -> Project -> Agg
    */
    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string(), 4)
        .project(&arena, vec![2])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 0)], 3);

    Flow {
        nodes: arena.into_vec(),
    }
}

pub fn make_simple_flow() -> Flow {
    let arena: NodeArena = Arena::new();
    let expr = RelExpr(
        Box::new(CID(1)),
        RelOp::Gt,
        Box::new(Literal(Datum::INT(15))),
    );

    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string(), 4) // name,age,dept_id
        //.filter(&arena, expr)
        //.project(&arena, vec![2, 1, 0])
        .agg(
            &arena,
            vec![2],
            vec![
                (AggType::COUNT, 0),
                (AggType::SUM, 2),
                (AggType::MIN, 0),
                (AggType::MAX, 0),
            ],
            3,
        )
        .emit(&arena);

    Flow {
        nodes: arena.into_vec(),
    }
}

/***************************************************************************************************/

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn write_partition(flow: &Flow, stage: &Stage, task: &Task, row: &Row) {
    // Key: flow-id / rdd-id / dest-part / src-part
    let npartitions_consumer = stage.npartitions_consumer;
    let partition = calculate_hash(row) % stage.npartitions_consumer as u64;

    debug!("write = {} to partition {} ", row, partition);
}
