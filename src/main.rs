// flare

pub mod net;
pub mod flow;
pub mod expr;
pub mod row;
pub mod consts;
pub mod graphviz;


use clp::CLParser;

fn main() -> Result<(), String> {
    let args = "cmdname --rank 0"
        .split(' ')
        .map(|e| e.to_owned())
        .collect();

    let mut clpr = CLParser::new(&args);

    clpr.define("--rank int")
        .define("--host_list string")
        .define("--workers_per_host int")
        .parse()?;

    Ok(())
}
