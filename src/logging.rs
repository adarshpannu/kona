use chrono::{DateTime, Local};
use env_logger::fmt::Color;
use env_logger::Env;
use log::Level;
use std::io::Write;
use std::thread;

use crate::includes::*;

pub fn init(default_log_level: &str) {
    let mut builder = env_logger::Builder::from_env(
        Env::default().default_filter_or(default_log_level),
    );
    let builder = builder.format(|buf, record| {
        let now: DateTime<Local> = Local::now();
        let mut level_style = buf.style();

        let level = record.level();
        if level == Level::Error || level == Level::Warn {
            level_style.set_color(Color::Red).set_bold(true);
        } else if level == Level::Info {
            level_style.set_color(Color::Green).set_bold(false);
        } else if level == Level::Debug {
            level_style.set_color(Color::Blue).set_bold(false);
        }

        writeln!(
            buf,
            "[{} {} {} {}] - {}",
            now.to_rfc3339(),
            level_style.value(record.level()),
            record.module_path().unwrap_or("<no-mod>"),
            thread::current().name().unwrap_or("<unnamed-thread>"),
            record.args()
        )
    });
    builder.init();
}
