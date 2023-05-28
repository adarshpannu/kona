// main

//#![allow(unused_variables)]

use std::fs;

use crate::{includes::*, qgm::ParserState};

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
    std::fs::remove_dir_all(&dirname).unwrap_or_default();

    // Run the flow
    env.scheduler.run_flow(env, flow, &flow.stage_graph);

    env.scheduler.close_all();
    env.scheduler.join();
    Ok(())
}

fn run_job(env: &mut Env) -> Result<(), String> {
    let pathname = &env.input_pathname;
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
                // Resolve QGM
                qgm.write_qgm_to_graphviz(&qgm_raw_pathname, false)?;
                qgm.resolve(&env)?;
                qgm.write_qgm_to_graphviz(&qgm_resolved_pathname, false)?;

                // Build LOPs
                let (lop_graph, lop_key) = qgm.build_logical_plan(env)?;

                if !env.settings.parse_only.unwrap_or(false) {
                    let flow = POP::compile(env, &mut qgm, &lop_graph, lop_key).unwrap();

                    // Build POPs
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
    //std::env::set_var("RUST_LOG", "flare::pcode=info");

    //std::env::set_var("RUST_LOG", "flare=info,flare::pop=debug,flare::flow=debug");
    std::env::set_var("RUST_LOG", "debug");

    // Initialize logger with default setting. This is overridden by RUST_LOG?
    logging::init("debug");

    let input_pathname = "/Users/adarshrp/Projects/flare/sql/rst.fsql".to_string();
    let output_dir = "/Users/adarshrp/Projects/flare/tmp".to_string();
    let mut env = Env::new(1, input_pathname, output_dir);

    let jobres = run_job(&mut env);
    if let Err(errstr) = &jobres {
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
    use std::{process::Command, rc::Rc};

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
        env.set_option("PARSE_ONLY".to_string(), datum::Datum::STR(Rc::new("true".to_string())))
            .unwrap();

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

use arrow2::{error::Result as A2Result, io::csv::read};

#[allow(dead_code)]
fn read_path(path: &str, projection: Option<&[usize]>) -> A2Result<Chunk<Box<dyn Array>>> {
    // Create a CSV reader. This is typically created on the thread that reads the file and
    // thus owns the read head.
    let mut reader: read::Reader<fs::File> = read::ReaderBuilder::new().from_path(path)?;

    // Infers the fields using the default inferer. The inferer is just a function that maps bytes
    // to a `DataType`.
    let (fields, _) = read::infer_schema(&mut reader, None, true, &read::infer)?;

    println!("Fields: {:?}", &fields);

    // allocate space to read from CSV to. The size of this vec denotes how many rows are read.
    let mut rows = vec![read::ByteRecord::default(); 100];

    // skip 0 (excluding the header) and read up to 100 rows.
    // this is IO-intensive and performs minimal CPU work. In particular,
    // no deserialization is performed.
    let rows_read = read::read_rows(&mut reader, 0, &mut rows)?;
    let rows = &rows[..rows_read];

    //let projection: Option<&[usize]> = projection.map(|v| &v);
    //let projection: Option<&[usize]> = if let Some(projection) = projection.as_ref() { Some(projection) } else { None };

    // parse the rows into a `Chunk`. This is CPU-intensive, has no IO,
    // and can be performed on a different thread by passing `rows` through a channel.
    // `deserialize_column` is a function that maps rows and a column index to an Array
    read::deserialize_batch(rows, &fields, projection, 0, read::deserialize_column)
}

#[test]
fn test_arrow2_csv_reader() -> A2Result<()> {
    let file_path = "/Users/adarshrp/Projects/flare/data/emp.csv";

    let projection: Vec<usize> = vec![2, 0];

    let batch = read_path(file_path, Some(&projection))?;
    println!("{:?}", batch);
    Ok(())
}

/*
use arrow2::datatypes::DataType as A2DataType;

#[derive(Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Clone, Hash)]
pub struct DataType0 {
    dt: A2DataType
}
*/
