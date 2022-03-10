#![allow(warnings)]

use crate::includes::*;

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

pub mod ast;
pub mod csv;
pub mod flow;
pub mod graphviz;
pub mod includes;
pub mod logging;
pub mod metadata;
pub mod row;
pub mod task;
pub mod qst;
pub mod graph;
pub mod aps;
pub mod compiler;
//pub mod bitmap;

use ast::{Expr::*, *};
use ast::{ParserState, AST};
use clp::CLParser;
use flow::*;
use graph::Graph;
use metadata::Metadata;
use compiler::Compiler;
use row::*;
use aps::*;
pub mod scratch;

use std::collections::HashMap;
use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use task::ThreadPool;
use slotmap::SlotMap;

pub struct Env {
    thread_pool: ThreadPool,
    metadata: Metadata,
    options: HashMap<String, String>
}

impl Env {
    fn new(nthreads: usize) -> Self {
        let thread_pool = task::ThreadPool::new(nthreads);
        let metadata = Metadata::new();
        Env { thread_pool, metadata, options: HashMap::new() }
    }

    fn set_option(&mut self, name: String, value: String) {
        debug!("SET {} = {}", &name, &value);
        self.options.insert(name.to_uppercase(), value.to_uppercase());
    }

    fn get_boolean_option(&self, name: &str) -> bool {
        if let Some(opt) = self.options.get(name) {
            return match opt.as_str() {
                "TRUE" | "YES" => true,
                _ => false
            }
        } else {
            return false
        }
    }
}

/***************************************************************************************************/
pub fn run_flow(env: &mut Env, flow: &Flow) {

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

/*
pub fn make_simple_flow(env: &Env) -> Flow {
    let arena: NodeArena = Arena::new();

    let mut qgm: Graph<Expr, ExprProps> = Graph::new();

    // Expression: $column-1 < 25
    let lhs = qgm.add_node(CID(0), None);
    let rhs = qgm.add_node(Literal(Datum::INT(25)), None);
    let expr = qgm.add_node(RelExpr(RelOp::Le), Some(vec![lhs, rhs]));
    //let expr = qgm.get_node(expr);

    let use_dir = false;

    let csvnode = if use_dir == false {
        let csvnode = CSVNode::new(env, &arena, "emp".to_string(), 4, HashMap::new());
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
        //.filter(&arena, expr) // age > ?
        .project(&arena, vec![2, 1, 0]) // dept_id, age, name
        .agg(
            &arena,
            vec![(0, DataType::INT)], // dept_id
            vec![
                (AggType::COUNT, 0, DataType::INT), // count(dept_id)
                (AggType::SUM, 1, DataType::INT),   // sum(age)
                (AggType::MIN, 2, DataType::STR),   // min(name)
                (AggType::MAX, 2, DataType::STR),   // max(name)
            ],
            3,
        )
        .emit(&arena, vec![]);

    Flow {
        id: 99,
        nodes: arena.into_vec(),
        graph: qgm
    }
}
*/

fn stringify<E: std::fmt::Debug>(e: E) -> String {
    format!("xerror: {:?}", e)
}

/*
 * Run a job from a file
 */
fn run_job(env: &mut Env, filename: &str) -> Result<(), String> {
    let contents = fs::read_to_string(filename).expect(&format!("Cannot open file: {}", &filename));

    let mut parser_state = ParserState::new();

    let qgm_raw_filename = format!("{}/{}", GRAPHVIZDIR, "qgm_raw.dot");
    let qgm_resolved_filename = format!("{}/{}", GRAPHVIZDIR, "qgm_resolved.dot");

    // Remove commented lines
    let astlist: Vec<AST> = sqlparser::JobParser::new().parse(&mut parser_state, &contents).unwrap();
    for (ix, mut ast) in astlist.into_iter().enumerate() {
        match ast {
            AST::CatalogTable { name, options } => {
                env.metadata.catalog_table(name, options)?;
            }
            AST::DescribeTable { name } => {
                env.metadata.describe_table(name)?;
            }
            AST::SetOption { name, value } => {
                env.set_option(name, value);
            }
            AST::QGM(mut qgm) => {
                qgm.write_qgm_to_graphviz(&qgm_raw_filename, false);
                qgm.resolve(&env)?;
                qgm.write_qgm_to_graphviz(&qgm_resolved_filename, false);

                if ! env.get_boolean_option("PARSE_ONLY") {
                    //let flow = Compiler::compile(env, &mut qgm).unwrap();
                    //run_flow(env, &flow);
                    APS::find_best_plan(env, &mut qgm);
                }
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

    let args = "cmdname --rank 0".split(' ').map(|e| e.to_owned()).collect();

    let mut clpr = CLParser::new(&args);

    clpr.define("--rank int")
        .define("--host_list string")
        .define("--workers_per_host int")
        .parse()?;

    // Initialize context
    let mut env = Env::new(1);

    let filename = "/Users/adarshrp/Projects/flare/sql/rst.fsql";
    //let filename = "/Users/adarshrp/tmp/first.sql";

    let jobres = run_job(&mut env, filename);
    if let Err(flare_err) = jobres {
        let errstr = format!("{}", &flare_err);
        error!("{}", errstr);
        return Err(errstr);
    }

    //let flow = make_simple_flow(env);
    //run_flow(&mut env, &flow);

    info!("End of program");

    Ok(())
}
