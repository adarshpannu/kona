extern crate lalrpop;

fn main() {
    println!("--- Running build.rs");
    lalrpop::process_root().unwrap();
}
