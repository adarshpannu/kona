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

#[test]
fn test() {
    dbg!(first_word("hello world. how are you?"));
    dbg!(first_word("hello world how are you?"));
}

fn first_word(s: &str) -> &str {
    let ix = s.chars().position(|ch| ch == '.').unwrap_or(s.len());
    &s[..ix]
}

struct A {
    b: B
}

#[derive(Clone, Copy)]
struct B {
    c: C
}

#[derive(Clone, Copy)]
struct C<'a> {
    _s: &'a str
}
