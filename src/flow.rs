#![allow(warnings)]

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::csv::*;
use crate::expr::{Expr::*, *};
use crate::graphviz::{htmlify, write_flow_to_graphviz};
use crate::includes::*;
use crate::row::*;
use crate::task::*;

#[derive(Debug, Serialize, Deserialize)]
enum NodeInner {
    EmitNode(EmitNode),
    CSVNode(CSVNode),
    ProjectNode(ProjectNode),
    FilterNode(FilterNode),
    AggNode(AggNode),
    JoinNode(JoinNode),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    id: NodeId,
    children: Vec<NodeId>,
    npartitions: usize,
    node_inner: NodeInner,
}

impl Node {
    fn base(&self) -> &Self {
        self
    }

    fn new0(
        arena: &NodeArena, npartitions: usize, node_inner: NodeInner,
    ) -> &Node {
        let id = arena.len();
        arena.alloc(Node {
            id,
            children: vec![],
            npartitions,
            node_inner,
        })
    }

    fn new1(
        arena: &NodeArena, child_id: NodeId, npartitions: usize,
        node_inner: NodeInner,
    ) -> &Node {
        let id = arena.len();
        arena.alloc(Node {
            id,
            children: vec![child_id],
            npartitions,
            node_inner,
        })
    }

    fn new<'a>(
        arena: &'a NodeArena, child_id: NodeId, other_children: Vec<&Node>,
        npartitions: usize, node_inner: NodeInner,
    ) -> &'a Node {
        let id = arena.len();
        let mut children: Vec<_> =
            other_children.iter().map(|e| e.id()).collect();
        children.push(child_id);
        arena.alloc(Node {
            id,
            children,
            npartitions,
            node_inner,
        })
    }
}

/***************************************************************************************************/
impl Node {
    pub fn emit<'a>(&self, arena: &'a NodeArena) -> &'a Node {
        let npartitions = self.npartitions;
        let retval =
            Node::new1(&arena, self.id(), npartitions, EmitNode::new());
        retval
    }

    pub fn project<'a>(
        &self, arena: &'a NodeArena, colids: Vec<ColId>,
    ) -> &'a Node {
        let npartitions = self.base().npartitions;
        let retval = Node::new1(
            &arena,
            self.id(),
            npartitions,
            ProjectNode::new(colids),
        );
        retval
    }

    pub fn filter<'a>(&self, arena: &'a NodeArena, expr: Expr) -> &'a Node {
        let npartitions = self.base().npartitions;
        let retval =
            Node::new1(&arena, self.id(), npartitions, FilterNode::new(expr));
        retval
    }

    pub fn join<'a>(
        &self, arena: &'a NodeArena, other_children: Vec<&Node>,
        preds: Vec<JoinPredicate>,
    ) -> &'a Node {
        let retval = Node::new(
            &arena,
            self.id(),
            other_children,
            self.npartitions, // TBD: Partitions need to be decided
            JoinNode::new(preds),
        );
        retval
    }

    pub fn agg<'a>(
        &self, arena: &'a NodeArena, keycolids: Vec<ColId>,
        aggcolids: Vec<(AggType, ColId)>, npartitions: usize,
    ) -> &'a Node {
        let aggnode = AggNode::new(keycolids, aggcolids);
        let retval = Node::new1(&arena, self.id(), npartitions, aggnode);
        retval
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn children(&self) -> &Vec<NodeId> {
        &self.children
    }

    pub fn desc(&self) -> String {
        // stupid dispatch, ugh!
        match &self.node_inner {
            NodeInner::CSVNode(inner_node) => inner_node.desc(self),
            NodeInner::EmitNode(inner_node) => inner_node.desc(self),
            NodeInner::ProjectNode(inner_node) => inner_node.desc(self),
            NodeInner::FilterNode(inner_node) => inner_node.desc(self),
            NodeInner::JoinNode(inner_node) => inner_node.desc(self),
            NodeInner::AggNode(inner_node) => inner_node.desc(self),
        }
    }

    pub fn nchildren(&self) -> usize {
        self.base().children.len()
    }

    pub fn child<'a>(&self, flow: &'a Flow, ix: NodeId) -> &'a Node {
        let children = &self.base().children;
        flow.get_node(children[ix])
    }

    pub fn next(
        &self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool,
    ) -> Option<Row> {
        // stupid dispatch, ugh!
        match &self.node_inner {
            NodeInner::CSVNode(inner_node) => {
                inner_node.next(self, flow, stage, task, is_head)
            }
            NodeInner::EmitNode(inner_node) => {
                inner_node.next(self, flow, stage, task, is_head)
            }
            NodeInner::ProjectNode(inner_node) => {
                inner_node.next(self, flow, stage, task, is_head)
            }
            NodeInner::FilterNode(inner_node) => {
                inner_node.next(self, flow, stage, task, is_head)
            }
            NodeInner::JoinNode(inner_node) => {
                inner_node.next(self, flow, stage, task, is_head)
            }
            NodeInner::AggNode(inner_node) => {
                inner_node.next(self, flow, stage, task, is_head)
            }
        }
    }

    pub fn is_endpoint(&self) -> bool {
        match &self.node_inner {
            NodeInner::EmitNode(_) => true,
            NodeInner::AggNode(_) => true,
            _ => false,
        }
    }

    pub fn npartitions(&self) -> usize {
        self.base().npartitions
    }
}

/***************************************************************************************************/
pub enum NodeRuntime {
    Unused,
    CSV { iter: CSVPartitionIter },
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CSVNode {
    filename: String,
    #[serde(skip)]
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
    partitions: Vec<TextFilePartition>,
}

use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

impl CSVNode {
    pub fn new<'a>(
        arena: &'a NodeArena, filename: String, npartitions: usize,
    ) -> &'a Node {
        let (colnames, coltypes) = Self::infer_metadata(&filename);

        let partitions =
            compute_partitions(&filename, npartitions as u64).unwrap();
        let csvnode = NodeInner::CSVNode(CSVNode {
            filename,
            colnames,
            coltypes,
            partitions,
        });
        let node = Node::new0(arena, npartitions, csvnode);
        node
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

impl CSVNode {
    fn desc(&self, supernode: &Node) -> String {
        let filename =
            self.filename.split("/").last().unwrap_or(&self.filename);

        format!(
            "CSVNode-#{} (p={})|{} {:?}",
            supernode.id(),
            supernode.npartitions(),
            filename,
            self.colnames
        )
        .replace("\"", "\\\"")
    }

    fn next(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
        is_head: bool,
    ) -> Option<Row> {
        let partition_id = task.partition_id;
        let runtime =
            task.contexts.entry(supernode.id()).or_insert_with(|| {
                let partition = &self.partitions[partition_id];
                let mut iter = CSVPartitionIter::new(&self.filename, partition);
                if partition_id == 0 {
                    iter.next(); // Consume the header row
                }
                NodeRuntime::CSV { iter }
            });

        if let NodeRuntime::CSV { iter } = runtime {
            if let Some(line) = iter.next() {
                // debug!("line = :{}:", &line.trim_end());
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
#[derive(Debug, Serialize, Deserialize)]
struct ProjectNode {
    colids: Vec<ColId>,
}

impl ProjectNode {
    fn new(colids: Vec<ColId>) -> NodeInner {
        NodeInner::ProjectNode(ProjectNode { colids })
    }

    fn desc(&self, supernode: &Node) -> String {
        format!("ProjectNode-#{}|{:?}", supernode.id(), self.colids)
    }

    fn next(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
        is_head: bool,
    ) -> Option<Row> {
        //let flow = task.stage().flow();
        //let flow = &*(&*task.stage).flow;

        if let Some(row) =
            supernode.child(flow, 0).next(flow, stage, task, false)
        {
            return Some(row.project(&self.colids));
        } else {
            return None;
        }
    }
}

impl ProjectNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
struct FilterNode {
    expr: Expr,
}

impl FilterNode {
    fn desc(&self, supernode: &Node) -> String {
        let s = format!("FilterNode-#{}|{}", supernode.id(), self.expr);
        htmlify(s)
    }

    fn next(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
        is_head: bool,
    ) -> Option<Row> {
        while let Some(e) =
            supernode.child(flow, 0).next(flow, stage, task, false)
        {
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
    fn new(expr: Expr) -> NodeInner {
        if let Expr::RelExpr(..) = expr {
            NodeInner::FilterNode(FilterNode { expr })
        } else {
            panic!("Invalid filter expression")
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
struct JoinNode {
    preds: Vec<JoinPredicate>, // (left-column,[eq],right-column)*
}

type JoinPredicate = (ColId, RelOp, ColId);

impl JoinNode {
    fn new(preds: Vec<JoinPredicate>) -> NodeInner {
        NodeInner::JoinNode(JoinNode { preds })
    }

    fn desc(&self, supernode: &Node) -> String {
        let s = format!("JoinNode-#{}|{:?}", supernode.id(), self.preds);
        htmlify(s)
    }

    fn next(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
        is_head: bool,
    ) -> Option<Row> {
        None
    }
}

impl JoinNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
struct AggNode {
    keycolids: Vec<ColId>,
    aggcolids: Vec<(AggType, ColId)>,
}

impl AggNode {
    fn new(
        keycolids: Vec<ColId>, aggcolids: Vec<(AggType, ColId)>,
    ) -> NodeInner {
        NodeInner::AggNode(AggNode {
            keycolids,
            aggcolids,
        })
    }

    fn desc(&self, supernode: &Node) -> String {
        let s = format!(
            "AggNode-#{} (p={})|by = {:?}, aggs = {:?}",
            supernode.id(),
            supernode.npartitions(),
            self.keycolids,
            self.aggcolids
        );
        s
    }

    fn next(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
        is_head: bool,
    ) -> Option<Row> {
        let htable: HashMap<Row, Row> =
            self.run_producer(supernode, flow, stage, task);
        None
    }

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
                    if curcol.cmp(&acccol) == std::cmp::Ordering::Less {
                        accrow.set_column(ix, &curcol)
                    }
                }
                AggType::MAX => {
                    if curcol.cmp(&acccol) == std::cmp::Ordering::Greater {
                        accrow.set_column(ix, &curcol)
                    }
                }
            }
        }
    }

    fn run_producer(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
    ) -> HashMap<Row, Row> {
        let mut htable: HashMap<Row, Row> = HashMap::new();
        let child = supernode.child(&*flow, 0);

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AggType {
    COUNT,
    MIN,
    MAX,
    SUM,
    //AVG,
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
struct EmitNode {}

impl EmitNode {
    fn new() -> NodeInner {
        NodeInner::EmitNode(EmitNode {})
    }

    fn desc(&self, supernode: &Node) -> String {
        format!("EmitNode-#{}", supernode.id())
    }

    fn next(
        &self, supernode: &Node, flow: &Flow, stage: &Stage, task: &mut Task,
        is_head: bool,
    ) -> Option<Row> {
        supernode.child(flow, 0).next(flow, stage, task, false)
    }
}

impl EmitNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub id: usize,
    pub nodes: Vec<Node>,
}

impl Flow {
    pub fn get_node(&self, node_id: NodeId) -> &Node {
        &self.nodes[node_id]
    }

    pub fn make_stages(&self) -> Vec<Stage> {
        let stages: Vec<_> = self
            .nodes
            .iter()
            .filter(|node| node.is_endpoint())
            .map(|node| Stage::new(node.id(), self))
            .collect();
        for stage in stages.iter() {
            debug!("Stage: head_node_id = {}", stage.head_node_id)
        }
        stages
    }

    pub fn run(&self, ctx: &Env) {
        let stages = self.make_stages();
        for stage in stages {
            stage.run(ctx, self);
        }
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
    // Key: flow-id / node-id / dest-part / src-part.extension
    let npartitions_consumer = stage.npartitions_consumer;
    let dest_partition =
        calculate_hash(row) % stage.npartitions_consumer as u64;

    let dirname = format!(
        "{}/flow-{}/stage-{}/consumer-{}",
        TEMPDIR, flow.id, stage.head_node_id, dest_partition
    );
    let filename = format!("{}/producer-{}.csv", dirname, task.partition_id);
    std::fs::create_dir_all(dirname);

    debug!("Write to {}: {}", filename, row);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)
        .unwrap();

    file.write(format!("{}\n", row).as_bytes());
}
