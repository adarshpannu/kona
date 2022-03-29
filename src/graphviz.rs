use std::io::Write;
use std::process::Command;

use crate::flow::Flow;

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

pub fn htmlify(s: String) -> String {
    s.replace("&", "&amp;").replace(">", "&gt;").replace("<", "&lt;")
}

