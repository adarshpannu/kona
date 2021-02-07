#![allow(warnings)]

use std::fmt;

#[derive(Debug)]
pub enum Column {
    IntegerLiteral(usize),
    StringLiteral(String),
}



impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::IntegerLiteral(il) => write!(f, "{}", il),
            Column::StringLiteral(sl) => write!(f, "{}", sl),
        }
    }
}

#[derive(Debug)]
pub struct Row {
    cols: Vec<Column>,
}

impl Row {
    pub fn from_csv_line(line: &String) -> Row {
        let cols: Vec<Column> = line
            .split(",")
            .map(|e| Column::StringLiteral(e.to_owned()))
            .collect();
        Row { cols }
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.cols.iter().for_each(|col| {
            write!(f, "{}, ", col);
        });
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}
