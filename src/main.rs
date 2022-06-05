// main

//#![allow(unused_variables)]

use std::fs;

use crate::includes::*;

#[macro_use]
extern crate lalrpop_util;

#[macro_use]
extern crate fstrings;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

pub mod includes;
pub mod env;
pub mod metadata;
pub mod bitset;
pub mod graph;
pub mod graphviz;
pub mod logging;

pub mod ast;
pub mod expr;
pub mod qgm;
pub mod qgmiter;

pub mod qst;
pub mod lop;

pub mod csv;
pub mod flow;
pub mod pop;
pub mod row;
pub mod stage;
pub mod task;
pub mod pcode;
pub mod scratch;

use ast::*;
use flow::*;
use qgm::*;

/***************************************************************************************************/
pub fn run_flow(env: &mut Env, flow: &Flow) -> Result<(), String> {
    // Clear output directories
    let dirname = format!("{}/flow", TEMPDIR);
    //std::fs::remove_dir_all(&dirname).map_err(|err| format!("Cannot remove temporary directory: {}", dirname))?;
    std::fs::remove_dir_all(&dirname).unwrap_or_default();

    // Run the flow
    flow.run(&env);

    env.thread_pool.close_all();
    env.thread_pool.join();
    Ok(())
}

fn run_job(env: &mut Env) -> Result<(), String> {
    let pathname = env.input_pathname.as_str();
    let contents = fs::read_to_string(pathname).expect(&format!("Cannot open file: {}", &pathname));

    let mut parser_state = ParserState::new();

    let qgm_raw_pathname = format!("{}/{}", env.output_dir, "qgm_raw.dot");
    let qgm_resolved_pathname = format!("{}/{}", env.output_dir, "qgm_resolved.dot");

    // Remove commented lines
    let astlist: Vec<AST> = sqlparser::JobParser::new().parse(&mut parser_state, &contents).unwrap();
    for ast in astlist.into_iter() {
        match ast {
            AST::CatalogTable { name, options } => {
                env.metadata.catalog_table(name, options)?;
            }
            AST::DescribeTable { name } => {
                env.metadata.describe_table(name)?;
            }
            AST::SetOption { name, value } => {
                env.set_option(name, value)?;
            }
            AST::QGM(mut qgm) => {
                qgm.write_qgm_to_graphviz(&qgm_raw_pathname, false)?;
                qgm.resolve(&env)?;
                qgm.write_qgm_to_graphviz(&qgm_resolved_pathname, false)?;
                let flow = Flow::compile(env, &mut qgm).unwrap();

                if !env.settings.parse_only.unwrap_or(false) {
                    run_flow(env, &flow)?;
                }
            }
        }
    }
    Ok(())
}

/*
********************************** main ****************************************************************
*/
fn main() -> Result<(), String> {

    std::env::set_var("RUST_LOG", "flare::pcode=info");

    std::env::set_var("RUST_LOG", "flare=info,flare::pop=debug,flare::flow=debug");

    // Initialize logger with default setting. This is overridden by RUST_LOG?
    logging::init("debug");

    let input_pathname = "/Users/adarshrp/Projects/flare/sql/join.fsql".to_string();
    let output_dir = "/Users/adarshrp/Projects/flare/tmp".to_string();
    let mut env = Env::new(1, input_pathname, output_dir);

    let jobres = run_job(&mut env);
    if let Err(errstr) = jobres {
        let errstr = format!("{}", &errstr);
        error!("{}", errstr);
        return Err(errstr);
    }
    Ok(())
}
/*
********************************** run_unit_tests *********************************************************
*/
#[test]
fn run_unit_tests() -> Result<(), String> {
    use std::process::Command;

    // Initialize logger with INFO as default
    logging::init("error");
    let mut npassed = 0;
    let mut ntotal = 0;
    //let diffcmd = "/Applications/DiffMerge.app/Contents/MacOS/DiffMerge";
    let diffcmd = "diff";

    for test in vec!["rst", "repartition", "groupby", "spja"] {
        let input_pathname = f!("/Users/adarshrp/Projects/flare/sql/{test}.fsql");
        let output_dir = f!("/Users/adarshrp/Projects/flare/tests/output/{test}/");

        println!("---------- Running subtest {}", input_pathname);
        std::fs::remove_dir_all(&output_dir).map_err(stringify)?;
        std::fs::create_dir_all(&output_dir).map_err(stringify)?;

        ntotal = ntotal + 1;
        let mut env = Env::new(1, input_pathname, output_dir.clone());

        let jobres = run_job(&mut env);
        if let Err(errstr) = jobres {
            let errstr = format!("{}", &errstr);
            error!("{}", errstr);
        }
        // Compare with gold output
        let gold_dir = f!("/Users/adarshrp/Projects/flare/tests/gold/{test}/");

        let output = Command::new(diffcmd).arg(gold_dir).arg(output_dir).output().expect("failed to execute process");

        let mut mismatch = false;
        for (tag, buf) in vec![("out", output.stdout), ("err", output.stderr)].iter() {
            if buf.len() > 0 {
                mismatch = true;
                let s = String::from_utf8_lossy(buf);
                println!("{}: {}", tag, s);
            }
        }
        if !mismatch {
            npassed = npassed + 1
        }
    }

    println!("---------- Completed: {}/{} subtests passed", npassed, ntotal);
    Ok(())
}
