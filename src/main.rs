// main

#![allow(warnings)]
#![allow(clippy::too_many_arguments)]

use crate::{includes::*, qgm::ParserState};
use std::fs;
use std::{process::Command, rc::Rc};

#[macro_use]
extern crate lalrpop_util;

#[macro_use]
extern crate fstrings;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

pub mod bitset;
pub mod env;
pub mod graph;
pub mod graphviz;
pub mod includes;
pub mod logging;
pub mod metadata;

pub mod ast;
pub mod expr;
pub mod qgm;
pub mod qgmiter;

pub mod lop;
pub mod lop_repartition;
pub mod qst;

pub mod flow;
pub mod pcode;
pub mod pop;
pub mod pop_aggregation;
pub mod pop_compile;
pub mod pop_csv;
pub mod pop_hashjoin;
pub mod pop_repartition;
pub mod pop_run;

pub mod datum;
pub mod scheduler;
pub mod scratch;
pub mod stage;
pub mod task;

pub mod print;

use ast::AST;
use flow::Flow;
use pop::POP;
use qgm::QGM;

/***************************************************************************************************/
pub fn run_flow(env: &mut Env, flow: &Flow) -> Result<(), String> {
    // Clear output directories
    let dirname = format!("{}/flow", TEMPDIR);
    //std::fs::remove_dir_all(&dirname).map_err(|err| format!("Cannot remove temporary directory: {}", dirname))?;
    std::fs::remove_dir_all(dirname).unwrap_or_default();

    // Run the flow
    env.scheduler.run_flow(env, flow)?;

    env.scheduler.end_all_threads();
    env.scheduler.join();
    Ok(())
}

fn run_job(env: &mut Env) -> Result<(), String> {
    let pathname = &env.input_pathname;
    let contents = fs::read_to_string(pathname).map_err(|err| stringify1("Cannot open file: {}", err))?;

    let mut parser_state = ParserState::default();

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
                // Resolve QGM
                qgm.write_qgm_to_graphviz(&qgm_raw_pathname, false)?;
                qgm.resolve(env)?;
                qgm.write_qgm_to_graphviz(&qgm_resolved_pathname, false)?;

                // Build LOPs
                let (lop_graph, lop_key) = qgm.build_logical_plan(env)?;

                if !env.settings.parse_only.unwrap_or(false) {
                    let flow = POP::compile_flow(env, &mut qgm, &lop_graph, lop_key).unwrap();

                    // Build POPs
                    run_flow(env, &flow)?;

                    display_output_dir(&flow);
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
    //std::env::set_var("RUST_LOG", "yard::pcode=info");

    //std::env::set_var("RUST_LOG", "yard=info,yard::pop_repartition=debug,yard::flow=debug");
    //std::env::set_var("RUST_LOG", "yard=info,yard::pop_compile=debug,yard::pop_repartition=debug");

    // Initialize logger with default setting. This is overridden by RUST_LOG?
    logging::init("info");

    let input_pathname = f!("{TOPDIR}/sql/tpch-q3.fsql");
    let output_dir = f!("{TOPDIR}/tmp");

    let mut env = Env::new(99, 1, input_pathname, output_dir);

    let jobres = run_job(&mut env);
    if let Err(errstr) = &jobres {
        let errstr = errstr.to_string();
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
    // Initialize logger with INFO as default
    logging::init("error");
    let mut npassed = 0;
    let mut ntotal = 0;
    //let diffcmd = "/Applications/DiffMerge.app/Contents/MacOS/DiffMerge";
    let diffcmd = "diff";

    for (id, test) in vec!["rst", "repartition", "groupby", "spja"].iter().enumerate() {
        let input_pathname = f!("{TOPDIR}/sql/{test}.fsql");
        let output_dir = f!("{TOPDIR}/tests/output/{test}/");

        println!("---------- Running subtest {}", input_pathname);
        std::fs::remove_dir_all(&output_dir).map_err(stringify)?;
        std::fs::create_dir_all(&output_dir).map_err(stringify)?;

        ntotal = ntotal + 1;
        let mut env = Env::new(id, 1, input_pathname, output_dir.clone());
        env.set_option("PARSE_ONLY".to_string(), datum::Datum::STR(Rc::new("true".to_string())))
            .unwrap();

        let jobres = run_job(&mut env);
        if let Err(errstr) = jobres {
            let errstr = format!("{}", &errstr);
            error!("{}", errstr);
        }
        // Compare with gold output
        let gold_dir = f!("{TOPDIR}/tests/gold/{test}/");

        let output = Command::new(diffcmd).arg(gold_dir).arg(output_dir).output().expect("failed to execute process");

        let mut mismatch = false;
        for (tag, buf) in vec![("out", output.stdout), ("err", output.stderr)].iter() {
            if !buf.is_empty() {
                mismatch = true;
                let s = String::from_utf8_lossy(buf);
                println!("{}:\n{}", tag, s);
            }
        }
        if !mismatch {
            npassed = npassed + 1
        }
    }

    println!("---------- Completed: {}/{} subtests passed", npassed, ntotal);
    Ok(())
}

fn display_output_dir(flow: &Flow) {
    println!("---------- output ----------");
    let output_dir = get_output_dir(flow.id);
    let files = list_files(&output_dir).unwrap();
    for file_path in files.iter() {
        let contents = fs::read_to_string(file_path).expect("Should have been able to read the file");
        println!("{}", contents);
    }
}
