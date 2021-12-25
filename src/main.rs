#![allow(warnings)]

use crate::error::{FlareError, FlareErrorCode, FlareErrorCode::*};
use crate::includes::*;

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

pub mod ast;
pub mod csv;
pub mod error;
pub mod flow;
pub mod graphviz;
pub mod includes;
pub mod logging;
pub mod metadata;
pub mod row;
pub mod task;

use ast::{ParserState, AST};
use clp::CLParser;
use ast::{Expr::*, *};
use flow::*;
use metadata::Metadata;
use row::*;
use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use task::ThreadPool;

pub struct Env {
    thread_pool: ThreadPool,
    metadata: Metadata,
}

impl Env {
    fn new(nthreads: usize) -> Self {
        let thread_pool = task::ThreadPool::new(nthreads);
        let metadata = Metadata::new();
        Env { thread_pool, metadata }
    }
}

/***************************************************************************************************/
pub fn run_flow(env: &mut Env) {
    let flow = make_simple_flow(env);

    let gvfilename = format!("{}/{}", GRAPHVIZDIR, "flow.dot");

    graphviz::write_flow_to_graphviz(&flow, &gvfilename, false).expect("Cannot write to .dot file.");

    let node = &flow.nodes[flow.nodes.len() - 1];

    let dirname = format!("{}/flow-{}", TEMPDIR, flow.id);
    std::fs::remove_dir_all(dirname);

    // Run the flow
    flow.run(&env);

    env.thread_pool.close_all();

    env.thread_pool.join();
}

pub fn make_simple_flow(env: &Env) -> Flow {
    let arena: NodeArena = Arena::new();

    // Expression: $column-1 < 3
    let expr = RelExpr(
        Rc::new(RefCell::new(CID(0))),
        RelOp::Le,
        Rc::new(RefCell::new(Literal(Datum::INT(3)))),
    );

    let use_dir = false;

    let csvnode = if use_dir == false {
        //let csvfilename = format!("{}/{}", DATADIR, "customer.tbl").to_string();

        let csvnode = CSVNode::new(env, &arena, "cust".to_string(), 4);
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
        //.project(&arena, vec![2, 1, 0]) // dept_id, age, name
        .agg(
            &arena,
            vec![(3, DataType::INT)], // dept_id
            vec![
                (AggType::COUNT, 0, DataType::INT), // count(dept_id)
                (AggType::SUM, 0, DataType::INT),   // sum(age)
                (AggType::MIN, 1, DataType::STR),   // min(name)
                (AggType::MAX, 1, DataType::STR),   // max(name)
            ],
            3,
        )
        .emit(&arena);

    Flow {
        id: 99,
        nodes: arena.into_vec(),
    }
}

fn stringify<E:std::fmt::Debug>(e: E) -> String { format!("xerror: {:?}", e) }


/*
 * Run a job from a file
 */
fn run_job(env: &mut Env, filename: &str) -> Result<(), FlareError> {
    let contents = fs::read_to_string(filename).expect(&format!("Cannot open file: {}", &filename));

    let mut parser_state = ParserState::new();

    let qgmfilename = format!("{}/{}", GRAPHVIZDIR, "qgm.dot");

    // Remove commented lines
    let astlist: Vec<AST> = sqlparser::JobParser::new().parse(&mut parser_state, &contents).unwrap();
    for (ix, ast) in astlist.into_iter().enumerate() {
        println!("--- Parsed stmt {} ---", ix);
        match ast {
            AST::CatalogTable { name, options } => {
                env.metadata.catalog_table(name, options)?;
            }
            AST::DescribeTable { name } => {
                env.metadata.describe_table(name)?;
            }
            AST::QGM(qgm) => {
                qgm.write_to_graphviz(&qgmfilename, false);
            }
            _ => unimplemented!(),
        }
    }
    //dbg!(&env.metadata);
    Ok(())
}

fn main() -> Result<(), String> {
    // Initialize logger with INFO as default
    logging::init();
    
    info!("FLARE {}", "hello");

    let args = "cmdname --rank 0".split(' ').map(|e| e.to_owned()).collect();

    let mut clpr = CLParser::new(&args);

    clpr.define("--rank int")
        .define("--host_list string")
        .define("--workers_per_host int")
        .parse()?;

    // Initialize context
    let mut env = Env::new(1);

    let filename = "/Users/adarshrp/Projects/flare/sql/scratch.sql";
    //let filename = "/Users/adarshrp/tmp/first.sql";

    let jobres = run_job(&mut env, filename);
    if let Err(flare_err) = jobres {
        let errstr = format!("{:?}", &flare_err);
        error!("{}", errstr);
        return Err(errstr);
    }

    //run_flow(&mut env);

    info!("End of program");

    Ok(())
}
