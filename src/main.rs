// flare
#![allow(warnings)]

use crate::includes::*;

pub mod csv;
pub mod expr;
pub mod flow;
pub mod graphviz;
pub mod includes;
pub mod logging;
pub mod net;
pub mod row;
pub mod task;

use bincode;
use clp::CLParser;
use expr::{Expr::*, *};
use flow::*;
use row::*;
use task::ThreadPool;

pub struct Env {
    thread_pool: ThreadPool,
}

impl Env {
    fn new(nthreads: usize) -> Env {
        // Create thread pool
        let thread_pool = task::ThreadPool::new(nthreads);
        Env { thread_pool }
    }
}

/***************************************************************************************************/
pub fn run_flow(ctx: &mut Env) {
    let flow = make_simple_flow();

    let gvfilename = format!("{}/{}", DATADIR, "flow.dot");

    graphviz::write_flow_to_graphviz(&flow, &gvfilename, false)
        .expect("Cannot write to .dot file.");

    let node = &flow.nodes[flow.nodes.len() - 1];

    let dirname = format!("{}/flow-{}", TEMPDIR, flow.id);
    std::fs::remove_dir_all(dirname);

    // Run the flow
    flow.run(&ctx);

    ctx.thread_pool.close_all();

    ctx.thread_pool.join();
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
        id: 97,
        nodes: arena.into_vec(),
    }
}

pub fn make_simple_flow() -> Flow {
    let arena: NodeArena = Arena::new();

    // Expression: $column-1 > ?
    let expr = RelExpr(
        Box::new(CID(1)),
        RelOp::Gt,
        Box::new(Literal(Datum::INT(10))),
    );

    let use_dir = true;

    let csvnode = if use_dir == false {
        let csvfilename = format!("{}/{}", DATADIR, "emp.csv");
        let csvnode = CSVNode::new(&arena, csvfilename.to_string(), 4);
        csvnode 
    } else {
        let csvnode = CSVDirNode::new(
            &arena,
            format!("{}/{}", DATADIR, "empdir/partition"),
            vec![],
            vec![DataType::STR, DataType::INT, DataType::INT],
            3,
        );
        csvnode
    };

    // name,age,dept_id
    csvnode
        .filter(&arena, expr) // age > ?
        .project(&arena, vec![2, 1, 0]) // dept_id, age, name
        .agg(
            &arena,
            vec![0],
            vec![
                (AggType::COUNT, 0),  // count(dept_id)
                (AggType::SUM, 1),    // sum(age)
                (AggType::MIN, 2),    // min(name)
                (AggType::MAX, 2),    // max(name)
            ],
            3,
        )
        .emit(&arena);

    Flow {
        id: 99,
        nodes: arena.into_vec(),
    }
}

fn main() -> Result<(), String> {
    // Initialize logger with INFO as default
    logging::init();

    info!("FLARE {}", "hello");

    let args = "cmdname --rank 0"
        .split(' ')
        .map(|e| e.to_owned())
        .collect();

    let mut clpr = CLParser::new(&args);

    clpr.define("--rank int")
        .define("--host_list string")
        .define("--workers_per_host int")
        .parse()?;

    // Initialize context
    let mut ctx = Env::new(1);

    run_flow(&mut ctx);

    info!("End of program");

    debug!("sizeof Node: {}", std::mem::size_of::<flow::Node>());
    debug!("sizeof Flow: {}", std::mem::size_of::<flow::Flow>());

    Ok(())
}
