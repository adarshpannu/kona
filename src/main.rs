#![allow(warnings)]

use crate::error::{FlareError, FlareErrorCode, FlareErrorCode::*};
use crate::includes::*;

pub mod ast;
pub mod csv;
pub mod error;
pub mod expr;
pub mod flow;
pub mod graphviz;
pub mod includes;
pub mod logging;
pub mod metadata;
pub mod net;
pub mod row;
pub mod task;

use ast::AST;
use clp::CLParser;
use expr::{Expr::*, *};
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
    fn new(nthreads: usize) -> Env {
        let thread_pool = task::ThreadPool::new(nthreads);
        let metadata = Metadata::new();
        Env {
            thread_pool,
            metadata,
        }
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
/*
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
*/

pub fn make_simple_flow() -> Flow {
    let arena: NodeArena = Arena::new();

    // Expression: $column-1 < 3
    let expr = RelExpr(
        Rc::new(RefCell::new(CID(0))),
        RelOp::Le,
        Rc::new(RefCell::new(Literal(Datum::INT(3)))),
    );

    let use_dir = false;

    let csvnode = if use_dir == false {
        let csvfilename = format!("{}/{}", DATADIR, "customer.tbl");
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

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub sqlparser); // synthesized by LALRPOP

#[test]
fn sqlparser() {
    /*
    assert!(sqlparser::ExprParser::new().parse("22").is_ok());
    assert!(sqlparser::ExprParser::new().parse("(22)").is_ok());
    assert!(sqlparser::ExprParser::new().parse("((((22))))").is_ok());
    assert!(sqlparser::ExprParser::new().parse("((22)").is_err());
    */

    assert!(sqlparser::LogExprParser::new().parse("col1 = 10").is_ok());
    assert!(sqlparser::LogExprParser::new().parse("(col1 = 10)").is_ok());

    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 and col2 < 20")
        .is_ok());
    assert!(sqlparser::LogExprParser::new().parse("(col2 < 20)").is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 or (col2 < 20)")
        .is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 and (col2 < 20)")
        .is_ok());

    assert!(sqlparser::LogExprParser::new()
        .parse("col1 >= 10 or col2 <= 20")
        .is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 > 10 and col2 < 20 or col3 != 30")
        .is_ok());
    assert!(sqlparser::LogExprParser::new()
        .parse("col1 = 10 or col2 = 20 and col3 > 30")
        .is_ok());

    //let expr = sqlparser::LogExprParser::new().parse("col1 = 10 and col2 = 20 and (col3 > 30)").unwrap();
    let expr: ExprLink = sqlparser::LogExprParser::new()
        .parse("(col2 > 20) and (col3 > 30) or (col4 < 40)")
        .unwrap();
    //let mut exprvec= vec![];
    //dbg!(normalize(&expr, &mut exprvec));
}

/*
fn normalize(expr: &Box<Expr>, exprvec: &mut Vec<&Box<Expr>>) -> bool {
    if let LogExpr(left, LogOp::And, right) = **expr {
        normalize(&left, exprvec);
        normalize(&right, exprvec);
    } else {
        println!("{:?}", expr);
        exprvec.push(expr);
    }
    false
}
*/

#[test]
fn stmtparser() {
    let stmtstr = r#"CATALOG TABLE emp FROM "emp.csv" ( "TYPE" = "CSV", "HEADER" = "YES" ) "#;

    println!("Parsing :{}:", stmtstr);

    sqlparser::StatementParser::new().parse(stmtstr).unwrap();
}

/*
 * Run a job from a file
 */
fn run_job(env: &mut Env, filename: &str) -> Result<(), FlareError> {
    let contents = fs::read_to_string(filename).expect("Cannot open file");

    println!("Job = :{}:", contents);

    let astlist: Vec<AST> =
        sqlparser::JobParser::new().parse(&contents).unwrap();
    for ast in astlist {
        println!("{:?}", ast);
        match ast {
            AST::CatalogTable { name, options } => {
                env.metadata.register_table(name, options)?;
            }
            AST::Query {
                select_list,
                from_list,
                where_clause,
            } => {}
            _ => unimplemented!(),
        }
    }
    dbg!(&env.metadata);
    Ok(())
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

    let filename = "/Users/adarshrp/Projects/flare/data/first.sql";

    //run_flow(&mut ctx);
    let jobres = run_job(&mut ctx, filename);
    if let Err(flare_err) = jobres {
        let errstr = format!("{:?}", &flare_err);
        error!("{}", errstr);
        return Err(errstr)
    }
    info!("End of program");

    Ok(())
}
