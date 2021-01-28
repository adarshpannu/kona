// flare

pub mod net;

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

    dbg!(clpr.get("rank"));
    Ok(())
}
