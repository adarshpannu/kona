extern crate lalrpop;

use lalrpop::Configuration;

fn main() {
    println!("--- Running build.rs");
    let mut cfg = Configuration::new();

    // Print rerun-if-changed directives to stdout. This ensures that Cargo will only rerun the build script if any of the processed .lalrpop files are changed.
    cfg.emit_rerun_directives(true);

    // Process all lalrpop files in the current directory, which is typically the root of the crate being compiled.
    cfg.process_current_dir().unwrap();
}
