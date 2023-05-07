
// Print: Diagnostics, Graphviz,

use std::fs::File;
use std::io::Write;
use std::process::Command;
use regex::Regex;

pub use crate::{bitset::*, csv::*, expr::*, flow::*, graph::*, includes::*, lop::*, metadata::*, pcode::*, pcode::*, qgm::*, row::*, stage::*, task::*};

impl QGM {
    pub fn write_logical_plan_to_graphviz(self: &QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pathname: &str) -> Result<(), String> {
        let mut file = std::fs::File::create(pathname).map_err(|err| f!("{:?}: {}", err, pathname))?;

        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        self.write_lop_to_graphviz(lop_graph, lop_key, &mut file)?;

        fprint!(file, "}}\n");

        drop(file);

        let oflag = format!("-o{}.jpg", pathname);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(pathname)
            .status()
            .expect("failed to execute process");

        Ok(())
    }

    pub fn write_lop_to_graphviz(self: &QGM, lop_graph: &LOPGraph, lop_key: LOPKey, file: &mut File) -> Result<(), String> {
        let id = lop_key.printable_key();
        let (lop, props, children) = lop_graph.get3(lop_key);

        if let Some(children) = children {
            for &child_key in children.iter() {
                let child_name = child_key.printable_key();
                fprint!(file, "    lopkey{} -> lopkey{};\n", child_name, id);
                self.write_lop_to_graphviz(lop_graph, child_key, file)?;
            }
        }
        let colstring = if let Some(emitcols) = props.emitcols.as_ref() {
            //let emitcols = emitcols.iter().map(|e| e.expr_key).collect::<Vec<_>>();
            printable_emitcols(&emitcols, self, true)
        } else {
            props.cols.printable(self)
        };

        let predstring = props.preds.printable(self, true);

        let (label, extrastr) = match &lop {
            LOP::TableScan { input_cols } => {
                let input_cols = input_cols.printable(self);
                let extrastr = format!("(input = {})", input_cols);
                (String::from("TableScan"), extrastr)
            }
            LOP::HashJoin { equi_join_preds } => {
                let equi_join_preds = equi_join_preds.iter().map(|e| e.0).collect::<Vec<_>>();
                let extrastr = printable_preds(&equi_join_preds, self, true);
                (String::from("HashJoin"), extrastr)
            }
            LOP::Repartition { cpartitions } => {
                let extrastr = format!("c = {}", cpartitions);
                (String::from("Repartition"), extrastr)
            }
            LOP::Aggregation { group_by_len } => {
                let extrastr = format!("by_len = {}", group_by_len);
                (String::from("Aggregation"), extrastr)
            }
        };

        fprint!(
            file,
            "    lopkey{}[label=\"{}-{}|{:?}|{}|{}|{}|{}\"];\n",
            id,
            label,
            lop_key.printable_id(),
            props.quns.elements(),
            colstring,
            predstring,
            props.partdesc.printable(&self.expr_graph, true),
            extrastr
        );

        Ok(())
    }
}


impl QGM {
    pub fn write_physical_plan_to_graphviz(self: &QGM, pop_graph: &POPGraph, pop_key: POPKey, pathname: &str) -> Result<(), String> {
        let mut file = std::fs::File::create(pathname).map_err(|err| f!("{:?}: {}", err, pathname))?;

        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        self.write_pop_to_graphviz(pop_graph, pop_key, &mut file)?;

        fprint!(file, "}}\n");

        drop(file);

        let oflag = format!("-o{}.jpg", pathname);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(pathname)
            .status()
            .expect("failed to execute process");

        Ok(())
    }

    pub fn write_pop_to_graphviz(self: &QGM, pop_graph: &POPGraph, pop_key: POPKey, file: &mut File) -> Result<(), String> {
        let id = pop_key.printable_key();
        let (pop, props, children) = pop_graph.get3(pop_key);

        if let Some(children) = children {
            for &child_key in children.iter() {
                let child_name = child_key.printable_key();
                fprint!(file, "    popkey{} -> popkey{};\n", child_name, id);
                self.write_pop_to_graphviz(pop_graph, child_key, file)?;
            }
        }

        let (label, extrastr) = match &pop {
            POP::CSV(csv) => {
                let pathname = csv.pathname.split("/").last().unwrap_or(&csv.pathname);
                let mut projection = csv.projection.clone();
                projection.sort_by(|a, b| a.cmp(b));
                let extrastr = format!("file: {}, map: {:?}", pathname, projection).replace("{", "(").replace("}", ")");
                (String::from("CSV"), extrastr)
            }
            POP::CSVDir(csvdir) => {
                let dirname = csvdir.dirname_prefix.split("/").last().unwrap_or(&csvdir.dirname_prefix);
                let mut projection = csvdir.projection.clone();
                projection.sort_by(|a, b| a.cmp(b));
                let extrastr = format!("file: {}, map: {:?}", dirname, projection).replace("{", "(").replace("}", ")");
                (String::from("CSVDir"), extrastr)
            }
            POP::HashJoin { .. } => {
                let extrastr = format!("");
                (String::from("HashJoin"), extrastr)
            }
            POP::Repartition(inner) => {
                let extrastr = format!("output_map = {:?}", inner.output_map);
                (String::from("Repartition"), extrastr)
            }
            POP::Aggregation(_) => {
                let extrastr = format!("");
                (String::from("Aggregation"), extrastr)
            }
        };

        let label = label.replace("\"", "").replace("{", "").replace("}", "");
        fprint!(
            file,
            "    popkey{}[label=\"{}-{}|p = {}|{}\"];\n",
            id,
            label,
            pop_key.printable_id(),
            props.npartitions,
            extrastr
        );

        Ok(())
    }
}


impl Bitset<QunCol> {
    fn printable(&self, qgm: &QGM) -> String {
        let cols = self
            .elements()
            .iter()
            .map(|&quncol| {
                let colname = qgm.metadata.get_colname(quncol);
                format!("{} ({}.{})", colname, quncol.0, quncol.1)
            })
            .collect::<Vec<String>>();
        let mut colstring = String::from("");
        for col in cols {
            colstring.push_str(&col);
            colstring.push_str(" ");
        }
        colstring
    }
}

impl Bitset<ExprKey> {
    fn printable(&self, qgm: &QGM, do_escape: bool) -> String {
        printable_preds(&self.elements(), qgm, do_escape)
    }
}

pub fn printable_preds(preds: &Vec<ExprKey>, qgm: &QGM, do_escape: bool) -> String {
    let mut predstring = String::from("{");
    for (ix, &pred_key) in preds.iter().enumerate() {
        let predstr = pred_key.printable(&qgm.expr_graph, do_escape);
        predstring.push_str(&predstr);
        if ix < preds.len() - 1 {
            predstring.push_str("|")
        }
    }
    predstring.push_str("}");
    predstring
}

pub fn printable_emitcols(preds: &Vec<EmitCol>, qgm: &QGM, do_escape: bool) -> String {
    let mut predstring = String::from("{");
    for (ix, EmitCol { quncol, expr_key }) in preds.iter().enumerate() {
        let predstr = expr_key.printable(&qgm.expr_graph, do_escape);
        predstring.push_str(&predstr);
        if quncol.0 > 0 {
            predstring.push_str(&format!(" [${}.{}] ", quncol.0, quncol.1));
        }

        if ix < preds.len() - 1 {
            predstring.push_str("|")
        }
    }
    predstring.push_str("}");
    predstring
}



impl POPKey {
    pub fn printable_key(&self) -> String {
        format!("{:?}", *self).replace("(", "").replace(")", "")
    }

    pub fn printable(&self, pop_graph: &POPGraph) -> String {
        let pop = &pop_graph.get(*self).value;
        format!("{:?}-{:?}", *pop, *self)
    }

    pub fn printable_id(&self) -> String {
        let re1 = Regex::new(r"^.*\(").unwrap();
        let re2 = Regex::new(r"\).*$").unwrap();

        let id = format!("{:?}", *self);
        let id = re1.replace_all(&id, "");
        let id = re2.replace_all(&id, "");
        id.to_string()
    }
}
