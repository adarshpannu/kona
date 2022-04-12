#![allow(warnings)]

use std::collections::HashMap;
use std::fs;
use task::ThreadPool;
use std::rc::Rc;

use crate::includes::*;
use clp::CLParser;

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

pub mod includes;

pub mod ast;
pub mod bitset;
pub mod expr;
pub mod graph;
pub mod qgm;
pub mod qgmiter;

pub mod compiler;
pub mod lop;
pub mod qst;

pub mod graphviz;
pub mod logging;

pub mod csv;
pub mod metadata;
pub mod pop;
pub mod flow;
pub mod row;
pub mod scratch;
pub mod task;

use compiler::*;
use lop::*;
use metadata::*;
use pop::*;
use flow::*;
use qgm::*;

pub struct Env {
    thread_pool: ThreadPool,
    metadata: Metadata,
    options: HashMap<String, String>,
}

impl Env {
    fn new(nthreads: usize) -> Self {
        let thread_pool = task::ThreadPool::new(nthreads);
        let metadata = Metadata::new();
        Env {
            thread_pool,
            metadata,
            options: HashMap::new(),
        }
    }

    fn set_option(&mut self, name: String, value: String) {
        debug!("SET {} = {}", &name, &value);
        self.options.insert(name.to_uppercase(), value.to_uppercase());
    }

    fn get_boolean_option(&self, name: &str) -> bool {
        if let Some(opt) = self.options.get(name) {
            return match opt.as_str() {
                "TRUE" | "YES" => true,
                _ => false,
            };
        } else {
            return false;
        }
    }
}

/***************************************************************************************************/
pub fn run_flow(env: &mut Env, flow: &Flow) {
    // Clear output directories
    let dirname = format!("{}/flow", TEMPDIR);
    std::fs::remove_dir_all(dirname);

    // Run the flow
    flow.run(&env);

    env.thread_pool.close_all();
    env.thread_pool.join();
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

                if !env.get_boolean_option("PARSE_ONLY") {
                    let flow = Flow::compile(env, &mut qgm).unwrap();
                    run_flow(env, &flow);
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

    let filename = "/Users/adarshrp/Projects/flare/sql/repartition.fsql";

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
