#![allow(warnings)]

use std::rc::Rc;
use std::{cell::RefCell, collections::HashMap};
use typed_arena::Arena;

use crate::consts::*;
use crate::expr::{Expr::*, *};
use crate::graphviz::write_flow_to_graphviz;
use crate::row::*;

/***************************************************************************************************/
type node_id = usize;
type col_id = usize;

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

    fn union<'a>(
        &self, arena: &'a NodeArena, other_children: Vec<&Box<dyn Node>>,
    ) -> &'a Box<dyn Node> {
        let base = NodeBase::new(&arena, self.id(), other_children);
        let retval = arena.alloc(Box::new(UnionNode { base }));
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

    fn name(&self) -> String;

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
    fn name(&self) -> String {
        let filename =
            self.filename.split("/").last().unwrap_or(&self.filename);

        format!("CSVNode|{} {:?}", filename, self.colnames)
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
    fn name(&self) -> String {
        format!("ProjectNode|{:?}", self.colids)
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
    fn name(&self) -> String {
        format!("FilterNode|{}", self.expr)
            .replace("&", "&amp;")
            .replace(">", "&gt;")
            .replace("<", "&lt;")
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
struct UnionNode {
    base: NodeBase,
}

impl Node for UnionNode {
    fn name(&self) -> String {
        "UnionNode".to_string()
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(&self, _: &Flow) -> Option<Row> {
        unimplemented!()
    }
}

impl UnionNode {}

/***************************************************************************************************/
#[derive(Debug)]
struct AggNode {
    base: NodeBase,
    keycolids: Vec<col_id>,
    aggcolids: Vec<(AggType, col_id)>,
}

impl Node for AggNode {
    fn name(&self) -> String {
        "AggNode".to_string()
    }

    fn base(&self) -> &NodeBase {
        &self.base
    }

    fn next(&self, flow: &Flow) -> Option<Row> {
        let htable: HashMap<Row, Row> = self.run_agg(flow);
        None
    }
}

impl AggNode {
    fn run_one_agg(&self, acc: &mut Row, currow: &Row) {
        for ix in 0..acc.len() {
            match self.aggcolids[ix].0 {
                AggType::COUNT => {
                    let mut col = acc.get_column_mut(ix);
                    *col = Datum::INT(99);
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
                            AggType::COUNT => Datum::INT(0),
                            _ => currow.get_column(ix).clone(),
                        }
                    })
                    .collect();
                Row::from(acc_cols)
            });
            println!("-- acc = {}", acc);
            AggNode::run_one_agg(self, acc, &currow)
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
fn make_complex_flow() -> Flow {
    let arena: NodeArena = Arena::new();
    let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
    let ab = CSVNode::new(&arena, csvfilename.to_string())
        .project(&arena, vec![0, 1, 2]);
    let c = ab.project(&arena, vec![0]);
    let d = ab.project(&arena, vec![1]);
    let e = c.union(&arena, vec![&d]).agg(
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
        .project(&arena, vec![0, 1, 2])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 1)]);

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
    let ab = CSVNode::new(&arena, csvfilename.to_string())
        .filter(&arena, expr)
        .project(&arena, vec![2, 1, 0])
        .agg(&arena, vec![0], vec![(AggType::COUNT, 1)]);

    Flow {
        nodes: arena.into_vec(),
    }
}

#[test]
fn test() {
    let flow = make_simple_flow();

    let gvfilename = format!("{}/{}", DATADIR, "flow.dot");

    write_flow_to_graphviz(&flow, &gvfilename, true)
        .expect("Cannot write to .dot file.");

    let node = &flow.nodes[flow.nodes.len() - 1];

    while let Some(row) = node.next(&flow) {
        println!("-- {}", row);
    }
}

struct Stage {
    bottom: node_id,
    top: node_id
}

fn make_stages(flow: &Flow) {

}

