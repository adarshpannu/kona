// logging

use std::env;

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init(trace_cmd: &str) {
    env::set_var("RUST_LOG", "debug,[{tag}]=debug");

    //env::set_var("RUST_LOG", "info,kona[inc{myfield1=1}]=debug");
    //env::set_var("RUST_LOG", "kona=debug");

    //let env_filter = EnvFilter::from_default_env();
    let env_filter = EnvFilter::builder().parse_lossy(trace_cmd);

    tracing_subscriber::registry()
        .with(fmt::layer()) //.pretty())
        .with(env_filter)
        .init();
}

