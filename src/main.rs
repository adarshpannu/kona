// main

#![allow(warnings)]
#![allow(clippy::too_many_arguments)]
#![deny(elided_lifetimes_in_paths)]

#[cfg(test)]
use std::{fs, process::Command};

use ast::AST;
use flow::Flow;
use pop::POP;
use qgm::QGM;

use crate::{includes::*, qgm::ParserState};

#[macro_use]
extern crate lalrpop_util;

#[macro_use]
extern crate fstrings;

extern crate lazy_static;

extern crate tracing;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

#[macro_use]
extern crate derivative;

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
pub mod pop_compile;
pub mod pop_csv;
pub mod pop_hash;
pub mod pop_hashagg;
pub mod pop_hashmatch;
pub mod pop_parquet;
pub mod pop_repartition;
pub mod pop_run;

pub mod datum;
pub mod scheduler;
pub mod stage;
pub mod task;

pub mod print;

pub use tracing::{debug, event, info, Level};

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/***************************************************************************************************/
pub fn run_flow(env: &mut Env, flow: &Flow) -> Result<(), String> {
    // Clear output directories
    let dirname = format!("{}/flow", TEMPDIR);
    //std::fs::remove_dir_all(&dirname).map_err(|err| format!("Cannot remove temporary directory: {}", dirname))?;
    std::fs::remove_dir_all(dirname).unwrap_or_default();

    // Run the flow
    env.scheduler.run_flow(env, flow)?;
    Ok(())
}

pub fn enable_tracing(env: &mut Env, astlist: &mut Vec<AST>, run_trace: bool) -> Result<(), String> {
    let mut ix_trace = None;
    for (ix, ast) in astlist.iter().enumerate() {
        match ast {
            AST::SetOption { name, value } => {
                if name.to_uppercase() == "TRACE" {
                    if ix_trace.is_some() {
                        return Err("Multiple SET TRACE statements found.".to_owned());
                    }
                    if run_trace {
                        env.set_option(name.clone(), value.clone())?;
                    }
                    ix_trace = Some(ix);
                }
            }
            _ => {}
        }
    }

    if let Some(ix_trace) = ix_trace {
        astlist.remove(ix_trace);
    } else if run_trace {
        // Default trace setting
        logging::init("info");
    }
    Ok(())
}

fn run_job(env: &mut Env, run_trace: bool) -> Result<(), String> {
    let pathname = &env.input_pathname;
    let contents = fs::read_to_string(pathname).map_err(|err| stringify1("Cannot open file: {}", err))?;

    let mut parser_state = ParserState::default();

    let qgm_raw_pathname = format!("{}/{}", env.output_dir, "qgm_raw.dot");
    let qgm_resolved_pathname = format!("{}/{}", env.output_dir, "qgm_resolved.dot");

    let mut astlist: Vec<AST> = sqlparser::JobParser::new().parse(&mut parser_state, &contents).unwrap();

    // Run any SET TRACE statement right away, if required. Additionally, ensure only one such statement exists in the job.
    enable_tracing(env, &mut astlist, run_trace)?;

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

    env.scheduler.end_all_threads()?;
    env.scheduler.join()?;

    Ok(())
}

/*
********************************** main ****************************************************************
*/

fn main() -> Result<(), String> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    //std::env::set_var("RUST_LOG", "kona::pcode=info");

    //std::env::set_var("RUST_LOG", "kona=info,kona::pop_repartition=debug,kona::flow=debug");
    //std::env::set_var("RUST_LOG", "kona=info,kona::pop_compile=debug,kona::pop_repartition=debug");

    // Initialize logger with default setting. This is overridden by RUST_LOG?
    //logging::init("debug");

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!("\n\nUsage: kona sqlfilename");
        println!("... exiting");
        return Ok(());
    }

    let input_pathname = args[1].clone();
    let output_dir = f!("tmp");

    let mut env = Env::new(99, 1, input_pathname, output_dir);

    let jobres = run_job(&mut env, true);
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
    //logging::init("error");
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
        env.set_option("PARSE_ONLY".to_string(), Utf8(String::from("true"))).unwrap();

        let jobres = run_job(&mut env, false);
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
        let lines = contents.split('\n').collect::<Vec<_>>();
        for line in lines.iter().take(10) {
            print!("{}\n", line);
        }
        if lines.len() > 10 {
            println!("[{} lines not shown]", lines.len());
        }
    }
    println!("----------------------------");
    println!("");
}

/*
fn test(from: PrimitiveArray<i128>) {
    cast::integer_to_decimal(&from, 5, 2);
}
*/
