#![allow(warnings)]

use std::{any::Any, rc::Rc};
use std::{cell::RefCell, collections::HashMap};
use typed_arena::Arena;

use crate::consts::*;
use crate::expr::{Expr::*, *};
use crate::graphviz::{htmlify, write_flow_to_graphviz};
use crate::row::*;

/***************************************************************************************************/
type node_id = usize;
type col_id = usize;
type partition_id = usize;

type NodeArena = Arena<Box<dyn Node>>;
pub trait Node {
    fn project<'a>(
        &self, arena: &'a NodeArena, colids: Vec<col_id>,
    ) -> &'a Box<dyn Node> {
        let base = NodeBase::new1(&arena, self.id());
        let retval = arena.alloc(Box::new(ProjectNode { base, colids }));
        retval
    }

    fn filter<'a>(
        &self, arena: &'a NodeArena, expr: Expr,
    ) -> &'a Box<dyn Node> {
        let base = NodeBase::new1(&arena, self.id());
        let retval = arena.alloc(Box::new(FilterNode::new(base, expr)));
        retval
    }

    fn join<'a>(
        &self, arena: &'a NodeArena, other_children: Vec<&Box<dyn Node>>,
        preds: Vec<JoinPredicate>,
    ) -> &'a Box<dyn Node> {
        let base = NodeBase::new(&arena, self.id(), other_children);
        let retval = arena.alloc(Box::new(JoinNode { base, preds }));
        retval
    }

    fn agg<'a>(
        &self, arena: &'a NodeArena, keycolids: Vec<col_id>,
        aggcolids: Vec<(AggType, col_id)>,
    ) -> &'a Box<dyn Node> {
        let base = NodeBase::new1(&arena, self.id());
        let retval = arena.alloc(Box::new(AggNode {
            base,
            keycolids,
            aggcolids,
        }));
        retval
    }

    fn base(&self) -> &NodeBase;

    fn desc(&self) -> String;

    fn id(&self) -> node_id {
        self.base().id
    }

    fn children(&self) -> &Vec<node_id> {
        &self.base().children
    }

    fn nchildren(&self) -> usize {
        self.base().children.len()
    }

    fn child<'a>(&self, flow: &'a Flow, ix: node_id) -> &'a Box<dyn Node> {
        let children = &self.base().children;
        flow.get_node(children[ix])
    }

    fn next(&self, _: &Flow) -> Option<Row>;

    fn needs_shuffle(&self) -> bool {
        false
    }
}

/***************************************************************************************************/
#[derive(Debug)]
pub struct NodeBase {
    id: node_id,
    children: Vec<node_id>,
}

impl NodeBase {
    fn new0(arena: &NodeArena) -> NodeBase {
        let id = arena.len();
        NodeBase {
            id,
            children: vec![],
        }
    }

    fn new1(arena: &NodeArena, child_id: node_id) -> NodeBase {
        let id = arena.len();
        NodeBase {
            id,
            children: vec![child_id],
        }
    }

    fn new(
        arena: &NodeArena, child_id: node_id,
        other_children: Vec<&Box<dyn Node>>,
    ) -> NodeBase {
        let id = arena.len();
        let mut children: Vec<_> =
            other_children.iter().map(|e| e.id()).collect();
        children.push(child_id);
        NodeBase { id, children }
    }
}

/***************************************************************************************************/
struct CSVNode {
    base: NodeBase,
    filename: String,
    colnames: Vec<String>,
    coltypes: Vec<DataType>,
    context: RefCell<CSVNodeContext>,
}

struct CSVNodeContext {
    iter: io::Lines<io::BufReader<File>>,
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
    fn new(arena: &NodeArena, filename: String) -> &Box<dyn Node> {
        let (colnames, coltypes) = Self::infer_metadata(&filename);

        let mut iter = read_lines(&filename).unwrap();
        iter.next(); // Consume the header row

        let context = RefCell::new(CSVNodeContext { iter });

        let base = NodeBase::new0(&arena);

        let retval = arena.alloc(Box::new(CSVNode {
            base,
            filename,
            colnames,
            coltypes,
            context,
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
        dbg!(&colnames);
        dbg!(&coltypes);
        (colnames, coltypes)
    }
}

impl CSVNodeContext {
    fn next(&mut self, node: &CSVNode) -> Option<Row> {
        if let Some(line) = self.iter.next() {
            let line = line.unwrap();
            let cols = line
                .split(',')
                .enumerate()
                .map(|(ix, col)| match node.coltypes[ix] {
                    DataType::INT => {
                        let ival = col.parse::<isize>().unwrap();
                        Datum::INT(ival)
                    }
                    DataType::STR => Datum::STR(Rc::new(col.to_owned())),
                    _ => unimplemented!(),
                })
                .collect::<Vec<Datum>>();
            Some(Row::from(cols))
        } else {
            None
        }
    }
}

impl Node for CSVNode {
    fn desc(&self) -> String {
        let filename =
            self.filename.split("/").last().unwrap_or(&self.filename);

        format!("CSVNode-#{}|{} {:?}", self.id(), filename, self.colnames)
            .replace("\"", "\\\"")
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(&self, _: &Flow) -> Option<Row> {
        let mut context = self.context.borrow_mut();
        context.next(self)
    }
}

/***************************************************************************************************/
#[derive(Debug)]
struct ProjectNode {
    base: NodeBase,
    colids: Vec<col_id>,
}

impl Node for ProjectNode {
    fn desc(&self) -> String {
        format!("ProjectNode-#{}|{:?}", self.id(), self.colids)
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(&self, flow: &Flow) -> Option<Row> {
        if let Some(row) = self.child(flow, 0).next(flow) {
            return Some(row.project(&self.colids));
        } else {
            return None;
        }
    }
}

impl ProjectNode {}

/***************************************************************************************************/
#[derive(Debug)]
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

    fn next(&self, flow: &Flow) -> Option<Row> {
        while let Some(e) = self.child(flow, 0).next(flow) {
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
struct JoinNode {
    base: NodeBase,
    preds: Vec<JoinPredicate>, // (left-column,[eq],right-column)*
}

type JoinPredicate = (col_id, RelOp, col_id);

impl Node for JoinNode {
    fn desc(&self) -> String {
        let s = format!("JoinNode-#{}|{:?}", self.id(), self.preds);
        htmlify(s)
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(&self, _: &Flow) -> Option<Row> {
        None
    }
}

impl JoinNode {}

/***************************************************************************************************/
#[derive(Debug)]
struct AggNode {
    base: NodeBase,
    keycolids: Vec<col_id>,
    aggcolids: Vec<(AggType, col_id)>,
}

impl Node for AggNode {
    fn desc(&self) -> String {
        let s = format!(
            "AggNode-#{}|by = {:?}, aggs = {:?}",
            self.id(),
            self.keycolids,
            self.aggcolids
        );
        s
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(&self, flow: &Flow) -> Option<Row> {
        let htable: HashMap<Row, Row> = self.run_agg(flow);
        None
    }

    fn needs_shuffle(&self) -> bool {
        true
    }
}

use std::cmp::Ordering;

impl AggNode {
    fn run_agg_one_row(&self, accrow: &mut Row, currow: &Row) {
        for ix in 0..accrow.len() {
            let agg_type = self.aggcolids[ix].0;
            let agg_colid = self.aggcolids[ix].1;
            let mut acccol = accrow.get_column_mut(ix);

            match agg_type {
                AggType::COUNT => {
                    let val = acccol.as_int() + 1;
                    *acccol = Datum::INT(val);
                }
                AggType::MIN => {
                    let curcol = currow.get_column(agg_colid);
                    if curcol.cmp(&acccol) == Ordering::Less {
                        accrow.set_column(agg_colid, &curcol)
                    }
                }
                AggType::MAX => {
                    let curcol = currow.get_column(agg_colid);
                    if curcol.cmp(&acccol) == Ordering::Greater {
                        accrow.set_column(agg_colid, &curcol)
                    }
                }
                _ => {}
            }
        }
    }

    fn run_agg(&self, flow: &Flow) -> HashMap<Row, Row> {
        let mut htable: HashMap<Row, Row> = HashMap::new();
        let child = self.child(flow, 0);

        while let Some(mut currow) = child.next(&flow) {
            // build key
            let key = currow.project(&self.keycolids);
            println!("-- key = {}", key);

            let acc = htable.entry(key).or_insert_with(|| {
                let acc_cols: Vec<Datum> = self
                    .aggcolids
                    .iter()
                    .map(|&(aggtype, ix)| {
                        // Build an empty accumumator Row
                        match aggtype {
                            AggType::COUNT => Datum::INT(1),
                            AggType::MAX | AggType::MIN => {
                                currow.get_column(ix).clone()
                            }
                        }
                    })
                    .collect();
                Row::from(acc_cols)
            });
            AggNode::run_agg_one_row(self, acc, &currow);
            println!("   acc = {}", acc);
        }
        for (k,v) in htable.iter() {
            println!("key = {}, value = {}", k, v);
        }
        htable

    }
}

#[derive(Debug, Clone, Copy)]
pub enum AggType {
    COUNT,
    MIN,
    MAX,
    //AVG,
    //SUM,
}

/***************************************************************************************************/
pub struct Flow {
    pub nodes: Vec<Box<dyn Node>>,
}

impl Flow {
    fn get_node(&self, node_id: node_id) -> &Box<dyn Node> {
        &self.nodes[node_id]
    }
}

/***************************************************************************************************/
fn make_join_flow() -> Flow {
    let arena: NodeArena = Arena::new();
    let empfilename = format!("{}/{}", DATADIR, "emp.csv").to_string();
    let deptfilename = format!("{}/{}", DATADIR, "dept.csv").to_string();

    let emp = CSVNode::new(&arena, empfilename)
        .project(&arena, vec![0, 1, 2])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 1)]);

    let dept = CSVNode::new(&arena, deptfilename);

    let join = emp.join(&arena, vec![&dept], vec![(2, RelOp::Eq, 0)]).agg(
        &arena,
        vec![0],
        vec![(AggType::COUNT, 1)],
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
    let ab = CSVNode::new(&arena, csvfilename.to_string())
        .project(&arena, vec![2])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 0)]);

    Flow {
        nodes: arena.into_vec(),
    }
}

fn make_simple_flow() -> Flow {
    let arena: NodeArena = Arena::new();
    let expr = RelExpr(
        Box::new(CID(1)),
        RelOp::Gt,
        Box::new(Literal(Datum::INT(15))),
    );

    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string()) // name,age,dept_id
        //.filter(&arena, expr)
        //.project(&arena, vec![2, 1, 0])
        .agg(&arena, vec![2], vec![(AggType::MIN, 0), (AggType::MAX, 0)]);

    Flow {
        nodes: arena.into_vec(),
    }
}

#[test]
fn test() {
    let flow = make_simple_flow();

    let gvfilename = format!("{}/{}", DATADIR, "flow.dot");

    write_flow_to_graphviz(&flow, &gvfilename, false)
        .expect("Cannot write to .dot file.");

    let node = &flow.nodes[flow.nodes.len() - 1];

    while let Some(row) = node.next(&flow) {
        println!("-- {}", row);
    }

    stage_manager(&flow);
}

fn stage_manager(flow: &Flow) {
    let stages: Vec<_> = flow
        .nodes
        .iter()
        .filter(|node| node.needs_shuffle())
        .map(|node| Stage { top: node.id() })
        .collect();

    dbg!(&stages);

    for stage in stages {
        stage.run_stage(&flow);
    }
}

#[derive(Debug)]
struct Stage {
    top: node_id,
}

impl Stage {
    fn run_stage(&self, flow: &Flow) {}
}

#[derive(Debug)]
struct Task {
    top: node_id,
    partition: partition_id,
}

impl Task {
    fn run(&self, flow: &Flow) {}
}
