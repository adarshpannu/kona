// flare
#![allow(warnings)]

use crate::includes::*;

pub mod csv;
pub mod expr;
pub mod flow;
pub mod graphviz;
pub mod includes;
pub mod net;
pub mod row;
pub mod task;

use clp::CLParser;
use env_logger::Env;
use task::ThreadPool;

pub struct Context {
    thread_pool: ThreadPool,
}

impl Context {
    fn new() -> Context {
        // Create thread pool
        let thread_pool = task::ThreadPool::new(4);
        Context { thread_pool }
    }
}

/***************************************************************************************************/
pub fn run_flow(ctx: &Context) {
    let flow = flow::make_simple_flow();

    let gvfilename = format!("{}/{}", DATADIR, "flow.dot");

    // graphviz::write_flow_to_graphviz(&flow, &gvfilename, false)
    //  .expect("Cannot write to .dot file.");

    let node = &flow.nodes[flow.nodes.len() - 1];

    /*
    while let Some(row) = node.next(&flow, true) {
        debug!("-- {}", row);
    }
    */

    // Run the flow
    flow.run(&ctx);
}

fn main() -> Result<(), String> {
    // Initialize logger with INFO as default
    env_logger::Builder::from_env(Env::default().default_filter_or("debug"))
        .init();

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
    let ctx = Context::new();

    run_flow(&ctx);

    info!("End of program");

    Ok(())
}
