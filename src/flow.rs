#![allow(warnings)]

use std::collections::HashMap;
use std::hash::Hash;
use std::io::Write;

use crate::graphviz::htmlify;
use crate::metadata::CSVDesc;
use crate::{
    ast::*,
    csv::*,
    graph::{Graph, NodeId},
    includes::*,
    row::*,
    task::*,
};

#[derive(Debug, Serialize, Deserialize)]
enum FlowNodeInner {
    EmitNode(EmitNode),
    CSVNode(CSVNode),
    CSVDirNode(CSVDirNode),
    ProjectNode(ProjectNode),
    FilterNode(FilterNode),
    AggNode(AggNode),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FlowNode {
    id: NodeId,
    npartitions: usize,
    node_inner: FlowNodeInner,
}

/***************************************************************************************************/

impl FlowNode {
    fn new<'a>(
        graph: &'a mut Graph<FlowNode>, children: Vec<NodeId>, npartitions: usize, node_inner: FlowNodeInner,
    ) -> NodeId {
        let id = graph.len();
        graph.add_node(
            FlowNode {
                id,
                npartitions,
                node_inner,
            },
            Some(children),
        )
    }

    pub fn emit<'a>(child_id: NodeId, graph: &'a mut Graph<FlowNode>) -> NodeId {
        let npartitions = 3; /// FIXME self.npartitions;
        FlowNode::new(graph, vec![child_id], npartitions, EmitNode::new())
    }

    
    /* FIXME
    pub fn project<'a>(child_id: NodeId, graph: &'a mut Graph<FlowNode>, colids: Vec<ColId>) -> NodeId {
        let npartitions = self.npartitions;
        FlowNode::new(graph, vec![child_id], npartitions, ProjectNode::new(colids))
    }

    pub fn filter<'a>(child_id: NodeId, graph: &'a mut Graph<FlowNode>, expr: Expr) -> NodeId {
        let npartitions = self.npartitions;
        FlowNode::new(graph, vec![child_id], npartitions, FilterNode::new(expr))
    }
    */

    pub fn agg<'a>(
        child_id: NodeId, graph: &'a mut Graph<FlowNode>, keycols: Vec<(ColId, DataType)>, aggcols: Vec<(AggType, ColId, DataType)>,
        npartitions: usize,
    ) -> NodeId {
        // Turn single Agg into (Agg-producer + CSVDir + Agg-Consumer). Turn COUNT -> SUM as well.
        let aggnode = AggNode::new(keycols.clone(), aggcols.clone(), true);
        let aggnode = FlowNode::new(graph, vec![child_id], npartitions, aggnode);
        let aggnode = graph.get_node(aggnode).0;

        let dirname_prefix = format!("{}/flow-99/stage-{}/consumer", TEMPDIR, aggnode.id);

        let mut csvcoltypes: Vec<DataType> = keycols.iter().map(|(_, tp)| *tp).collect();
        for (aggtype, _, datatype) in aggcols.iter() {
            match *aggtype {
                AggType::COUNT => csvcoltypes.push(DataType::INT),
                _ => csvcoltypes.push(*datatype),
            };
        }
        let colnames = (0..csvcoltypes.len()).map(|ix| format!("agg_col_{}", ix)).collect();
        let csvdirnode = CSVDirNode::new(graph, dirname_prefix, colnames, csvcoltypes, 3); // FIXME self.npartitions);
        //let csvdirnode = graph.get_node_inner(csvdirnode);

        // Re-number keycols
        let keycols: Vec<(ColId, DataType)> = keycols.iter().map(|(id, coltype)| *coltype).enumerate().collect();
        let aggcols: Vec<(AggType, ColId, DataType)> = aggcols
            .iter()
            .enumerate()
            .map(|(id, (aggtype, _, coltype))| {
                // COUNT turns into SUM with a type of INT
                let (aggtype, coltype) = match *aggtype {
                    AggType::COUNT => (AggType::SUM, DataType::INT),
                    _ => (*aggtype, *coltype),
                };
                (aggtype, id + keycols.len(), coltype)
            })
            .collect();

        let aggnode = AggNode::new(keycols, aggcols, false);
        let aggnode = FlowNode::new(graph, vec![csvdirnode], npartitions, aggnode);

        aggnode
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    /*
    pub fn children(&self) -> &Vec<NodeId> {
        &self.children
    }
        pub fn nchildren(&self) -> usize {
        self.children.len()
    }
    */

    pub fn child<'a>(&self, flow: &'a Flow, ix: NodeId) -> &'a FlowNode {
        let children = flow.graph.get_node(ix).1.unwrap();
        flow.get_node(children[ix])
    }

    pub fn desc(&self) -> String {
        // stupid dispatch, ugh!
        match &self.node_inner {
            FlowNodeInner::CSVNode(inner_node) => inner_node.desc(self),
            FlowNodeInner::CSVDirNode(inner_node) => inner_node.desc(self),
            FlowNodeInner::EmitNode(inner_node) => inner_node.desc(self),
            FlowNodeInner::ProjectNode(inner_node) => inner_node.desc(self),
            FlowNodeInner::FilterNode(inner_node) => inner_node.desc(self),
            FlowNodeInner::AggNode(inner_node) => inner_node.desc(self),
        }
    }

    pub fn next(&self, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        // stupid dispatch, ugh!
        let row = match &self.node_inner {
            FlowNodeInner::CSVNode(inner_node) => inner_node.next(self, flow, stage, task, is_head),
            FlowNodeInner::CSVDirNode(inner_node) => inner_node.next(self, flow, stage, task, is_head),
            FlowNodeInner::EmitNode(inner_node) => inner_node.next(self, flow, stage, task, is_head),
            FlowNodeInner::ProjectNode(inner_node) => inner_node.next(self, flow, stage, task, is_head),
            FlowNodeInner::FilterNode(inner_node) => inner_node.next(self, flow, stage, task, is_head),
            FlowNodeInner::AggNode(inner_node) => inner_node.next(self, flow, stage, task, is_head),
        };
        //debug!("Node {}, next() = {:?}", self.id, &row);
        row
    }

    pub fn is_endpoint(&self) -> bool {
        match &self.node_inner {
            FlowNodeInner::EmitNode(_) => true,
            FlowNodeInner::AggNode(aggnode) => aggnode.is_producer == true,
            _ => false,
        }
    }

    pub fn npartitions(&self) -> usize {
        self.npartitions
    }
}

/***************************************************************************************************/
pub enum NodeRuntime {
    Unused,
    CSV { iter: CSVPartitionIter },
    CSVDir { iter: CSVDirIter },
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

impl CSVNode {
    pub fn new<'a>(env: &Env, graph: &'a mut Graph<FlowNode>, name: String, npartitions: usize) -> NodeId {
        let tbldesc = env.metadata.get_tabledesc(&name).unwrap();
        let (colnames, coltypes) = (tbldesc.colnames().clone(), tbldesc.coltypes().clone());

        let filename = tbldesc.filename();
        let partitions = compute_partitions(filename, npartitions as u64).unwrap();
        let csvnode = FlowNodeInner::CSVNode(CSVNode {
            filename: filename.clone(),
            colnames,
            coltypes,
            partitions,
        });
        let node = FlowNode::new(graph, vec![], npartitions, csvnode);
        node
    }
}

impl CSVNode {
    fn desc(&self, supernode: &FlowNode) -> String {
        let filename = self.filename.split("/").last().unwrap_or(&self.filename);

        format!(
            "CSVNode-#{} (p={})|{} {:?}",
            supernode.id(),
            supernode.npartitions(),
            filename,
            self.colnames
        )
        .replace("\"", "\\\"")
    }

    fn next(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        let partition_id = task.partition_id;
        let runtime = task.contexts.entry(supernode.id()).or_insert_with(|| {
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
                    .split('|')
                    .enumerate()
                    .map(|(ix, col)| match self.coltypes[ix] {
                        DataType::INT => {
                            let ival = col.parse::<isize>();
                            if ival.is_err() {
                                panic!("{} is not an INT", &col);
                            } else {
                                Datum::INT(ival.unwrap())
                            }
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
    fn new(colids: Vec<ColId>) -> FlowNodeInner {
        FlowNodeInner::ProjectNode(ProjectNode { colids })
    }

    fn desc(&self, supernode: &FlowNode) -> String {
        format!("ProjectNode-#{}|{:?}", supernode.id(), self.colids)
    }

    fn next(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        //let flow = task.stage().flow();
        //let flow = &*(&*task.stage).flow;

        if let Some(row) = supernode.child(flow, 0).next(flow, stage, task, false) {
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
    fn desc(&self, supernode: &FlowNode) -> String {
        let s = format!("FilterNode-#{}|{}", supernode.id(), self.expr);
        htmlify(s)
    }

    fn next(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        while let Some(e) = supernode.child(flow, 0).next(flow, stage, task, false) {
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
    fn new(expr: Expr) -> FlowNodeInner {
        if let Expr::RelExpr(..) = expr {
            FlowNodeInner::FilterNode(FilterNode { expr })
        } else {
            panic!("Invalid filter expression")
        }
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
struct AggNode {
    keycols: Vec<(ColId, DataType)>,
    aggcols: Vec<(AggType, ColId, DataType)>,
    is_producer: bool,
}

impl AggNode {
    fn new(
        keycols: Vec<(ColId, DataType)>, aggcols: Vec<(AggType, ColId, DataType)>, is_producer: bool,
    ) -> FlowNodeInner {
        FlowNodeInner::AggNode(AggNode {
            keycols,
            aggcols,
            is_producer,
        })
    }

    fn desc(&self, supernode: &FlowNode) -> String {
        let s = format!(
            "AggNode-#{} (p={})|by = {:?}, aggs = {:?}",
            supernode.id(),
            supernode.npartitions(),
            self.keycols,
            self.aggcols
        );
        s
    }

    fn next(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        let htable: HashMap<Row, Row> = self.run_producer(supernode, flow, stage, task);
        None
    }

    fn run_producer_one_row(&self, accrow: &mut Row, currow: &Row) {
        for (ix, &(agg_type, agg_colid, _)) in self.aggcols.iter().enumerate() {
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
                AggType::AVG => {
                    unimplemented!()
                }
                AggType::COUNT_DISTINCT => {
                    unimplemented!()
                }
            }
        }
    }

    fn run_producer(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task) -> HashMap<Row, Row> {
        let mut htable: HashMap<Row, Row> = HashMap::new();
        let child = supernode.child(&*flow, 0);

        while let Some(currow) = child.next(flow, stage, task, false) {
            // build key
            let keycolids = self.keycols.iter().map(|&e| e.0).collect::<Vec<usize>>();
            let key = currow.project(&keycolids);
            //debug!("-- key = {}", key);

            let acc = htable.entry(key).or_insert_with(|| {
                let acc_cols: Vec<Datum> = self
                    .aggcols
                    .iter()
                    .map(|&(aggtype, ix, _)| {
                        // Build an empty accumumator Row
                        match aggtype {
                            AggType::COUNT => Datum::INT(0),
                            AggType::SUM => Datum::INT(0),
                            AggType::MAX | AggType::MIN => currow.get_column(ix).clone(),
                            AggType::AVG | AggType::COUNT_DISTINCT => unimplemented!(),
                        }
                    })
                    .collect();
                Row::from(acc_cols)
            });
            AggNode::run_producer_one_row(self, acc, &currow);
            //debug!("   acc = {}", acc);
        }

        for (key, value) in htable.iter() {
            write_partition(flow, stage, task, key, Some(value));
        }
        htable
    }
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
struct EmitNode {}

impl EmitNode {
    fn new() -> FlowNodeInner {
        FlowNodeInner::EmitNode(EmitNode {})
    }

    fn desc(&self, supernode: &FlowNode) -> String {
        format!("EmitNode-#{}", supernode.id())
    }

    fn next(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        supernode.child(flow, 0).next(flow, stage, task, false)
    }
}

impl EmitNode {}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub id: usize,
    pub graph: Graph<FlowNode>,
}

impl Flow {
    pub fn get_node(&self, node_id: NodeId) -> &FlowNode {
        self.graph.get_node(node_id).0
    }

    pub fn make_stages(&self) -> Vec<Stage> {
        /*
        let stages: Vec<_> = self
            .graph
            .nodes()
            .iter()
            .filter(|node| node.inner().is_endpoint())
            .map(|node| Stage::new(node.inner().id(), self))
            .collect();
        for stage in stages.iter() {
            debug!("Stage: head_node_id = {}", stage.head_node_id)
        }
        stages
        */
        todo!()
    }

    pub fn run(&self, env: &Env) {
        let stages = self.make_stages();
        for stage in stages {
            stage.run(env, self);
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

fn write_partition(flow: &Flow, stage: &Stage, task: &Task, key: &Row, value: Option<&Row>) {
    // Key: flow-id / node-id / dest-part / src-part.extension
    let npartitions_consumer = stage.npartitions_consumer;
    let dest_partition = calculate_hash(key) % stage.npartitions_consumer as u64;

    let dirname = format!(
        "{}/flow-{}/stage-{}/consumer-{}",
        TEMPDIR, flow.id, stage.head_node_id, dest_partition
    );
    let filename = format!("{}/producer-{}.csv", dirname, task.partition_id);
    std::fs::create_dir_all(dirname);

    if value.is_some() {
        debug!("Write to {}: {},{}", filename, key, value.unwrap())
    } else {
        debug!("Write to {}: {}", filename, key)
    }

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(filename)
        .unwrap();

    file.write(format!("{}", key).as_bytes());
    if let Some(row) = value {
        file.write(format!("|{}", row).as_bytes());
    }
    file.write("\n".as_bytes());
    file.flush();
    drop(file);
}

/***************************************************************************************************/
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CSVDirNode {
    dirname_prefix: String, // E.g.: $TEMPDIR/flow-99/stage  i.e. everything except the "-{partition#}"
    #[serde(skip)]
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
}

impl CSVDirNode {
    pub fn new<'a>(
        graph: &'a mut Graph<FlowNode>, dirname_prefix: String, colnames: Vec<String>, coltypes: Vec<DataType>,
        npartitions: usize,
    ) -> NodeId {
        let csvnode = FlowNodeInner::CSVDirNode(CSVDirNode {
            dirname_prefix,
            colnames,
            coltypes,
        });
        let node = FlowNode::new(graph, vec![], npartitions, csvnode);
        node
    }
}

impl CSVDirNode {
    fn desc(&self, supernode: &FlowNode) -> String {
        let filename = self.dirname_prefix.split("/").last().unwrap_or(&self.dirname_prefix);

        format!(
            "CSVDirNode-#{} (p={})|{} {:?}",
            supernode.id(),
            supernode.npartitions(),
            filename,
            self.colnames
        )
        .replace("\"", "\\\"")
    }

    fn next(&self, supernode: &FlowNode, flow: &Flow, stage: &Stage, task: &mut Task, is_head: bool) -> Option<Row> {
        let partition_id = task.partition_id;
        let runtime = task.contexts.entry(supernode.id()).or_insert_with(|| {
            let full_dirname = format!("{}-{}", self.dirname_prefix, partition_id);
            let mut iter = CSVDirIter::new(&full_dirname);
            NodeRuntime::CSVDir { iter }
        });

        if let NodeRuntime::CSVDir { iter } = runtime {
            if let Some(line) = iter.next() {
                // debug!("line = :{}:", &line.trim_end());
                let cols = line
                    .trim_end()
                    .split('|')
                    .enumerate()
                    .map(|(ix, col)| match self.coltypes[ix] {
                        DataType::INT => {
                            let ival = col.parse::<isize>();
                            if ival.is_err() {
                                panic!("{} is not an INT", &col);
                            } else {
                                Datum::INT(ival.unwrap())
                            }
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
