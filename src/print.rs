// Print: Diagnostics, Graphviz,

use std::{collections::HashMap, fs::File, io::Write, process::Command};

use regex::Regex;

use crate::{
    bitset::Bitset,
    expr::Expr,
    graph::{ExprKey, LOPKey, POPKey},
    includes::*,
    lop::{LOPGraph, VirtCol, LOP},
    pop::{POPGraph, POP},
    qgm::QueryBlock,
    stage::{Stage, StageGraph},
    QGM,
};

impl QGM {
    pub fn write_expr_to_graphvis(qgm: &QGM, expr_key: ExprKey, file: &mut File, order_ix: Option<usize>) -> Result<(), String> {
        let id = expr_key.to_string();
        let (expr, _, children) = qgm.expr_graph.get3(expr_key);
        let ix_str = if let Some(ix) = order_ix { format!(": {}", ix) } else { String::from("") };

        fprint!(file, "    exprnode{}[label=\"{}{}\"];\n", id, expr.name(), ix_str);

        if let Expr::Subquery(qbkey) = expr {
            let qblock = &qgm.qblock_graph.get(*qbkey).value;
            fprint!(file, "    \"{}_selectlist\" -> \"exprnode{}\";\n", qblock.name(), id);
        }

        if let Some(children) = children {
            for &childid in children {
                let childid_name = childid.to_string();

                fprint!(file, "    exprnode{} -> exprnode{};\n", childid_name, id);
                Self::write_expr_to_graphvis(qgm, childid, file, None)?;
            }
        }
        Ok(())
    }

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
        let mut colstring = props.cols.printable(self);

        if let Some(virtcols) = props.virtcols.as_ref() {
            colstring = format!("{{{}|{}}}", colstring, printable_virtcols(&virtcols, self, true));
        };

        let predstring = props.preds.printable(self, true);

        let (label, extrastr) = match &lop {
            LOP::TableScan { input_projection } => {
                let input_projection = input_projection.printable(self);
                let extrastr = format!("(input = {})", input_projection);
                (String::from("TableScan"), extrastr)
            }
            LOP::HashJoin { lhs_join_keys, rhs_join_keys } => {
                let lhsstr = printable_preds(&lhs_join_keys, self, true, false);
                let rhsstr = printable_preds(&rhs_join_keys, self, true, false);
                let extrastr = format!("{} = {}", lhsstr, rhsstr);
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

    pub fn write_physical_plan_to_graphviz(self: &QGM, stage_graph: &StageGraph, pathname: &str) -> Result<(), String> {
        let mut file = std::fs::File::create(pathname).map_err(|err| f!("{:?}: {}", err, pathname))?;

        fprint!(file, "digraph example1 {{\n");
        fprint!(file, "    node [shape=record];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        // Write stages
        for stage in &stage_graph.stages {
            fprint!(file, "  subgraph cluster_stage_{} {{\n", stage.stage_id);
            //fprint!(file, "    \"stage_{}_stub\"[label=\"select_list\",shape=box,style=filled];\n", stage.stage_id);

            self.write_pop_to_graphviz(stage, stage.root_pop_key.unwrap(), &mut file)?;
            fprint!(file, "    color = \"red\"\n");
            fprint!(file, "}}\n");

            if let Some(pop_key) = stage.root_pop_key {
                let pop = stage.pop_graph.get_value(pop_key);

                if let POP::RepartitionRead(rpr) = &pop {
                    let child_name = rpr.child_pop_key().printable_key(*rpr.child_stage_id());
                    let name = pop_key.printable_key(stage.stage_id);

                    fprint!(file, "    popkey{} -> popkey{};\n", child_name, name);
                }
            }
        }

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

    pub fn write_pop_to_graphviz(self: &QGM, stage: &Stage, pop_key: POPKey, file: &mut File) -> Result<(), String> {
        let id = pop_key.printable_key(stage.stage_id);
        let pop_graph = &stage.pop_graph;
        let (pop, props, children) = pop_graph.get3(pop_key);

        if let Some(children) = children {
            for &child_key in children.iter() {
                let child_name = child_key.printable_key(stage.stage_id);
                fprint!(file, "    popkey{} -> popkey{};\n", child_name, id);
                self.write_pop_to_graphviz(stage, child_key, file)?;
            }
        }

        let color = if stage.root_pop_key.unwrap() == pop_key { "red" } else { "black" };

        let (label, extrastr) = match &pop {
            POP::CSV(csv) => {
                let pathname = csv.pathname.split("/").last().unwrap_or(&csv.pathname);
                //let mut projection = csv.projection.clone();
                //projection.sort_by(|a, b| a.cmp(b));
                let extrastr = format!("file: {}, input_projection: {:?}", pathname, &csv.input_projection)
                    .replace("{", "(")
                    .replace("}", ")");
                (String::from("CSV"), extrastr)
            }
            POP::HashJoin { .. } => {
                let extrastr = format!("");
                (String::from("HashJoin"), extrastr)
            }
            POP::RepartitionWrite(rpw) => {
                let extrastr = format!("c = {}", rpw.cpartitions());
                (String::from("RepartitionWrite"), extrastr)
            }
            POP::RepartitionRead(_) => {
                let extrastr = format!("");
                (String::from("RepartitionRead"), extrastr)
            }
            POP::Aggregation(_) => {
                let extrastr = format!("");
                (String::from("Aggregation"), extrastr)
            }
        };

        let label = label.replace("\"", "").replace("{", "").replace("}", "");
        let colstr = if let Some(cols) = &props.cols {
            format!("{:?}", cols).replace("{", "(").replace("}", ")")
        } else {
            format!("")
        };

        fprint!(
            file,
            "    popkey{}[label=\"{}-{}|p = {}|cols = {}, vcols = #{}|{}\", color=\"{}\"];\n",
            id,
            label,
            pop_key.printable_id(),
            props.npartitions,
            colstr,
            props.virtcols.as_ref().map_or(0, |v| v.len()),
            extrastr,
            color
        );

        Ok(())
    }
}

impl QueryBlock {
    pub fn write_qblock_to_graphviz(&self, is_main_qb: bool, qgm: &QGM, file: &mut File) -> Result<(), String> {
        // Write current query block first

        // --- begin query block cluster ---
        fprint!(file, "  subgraph cluster_{} {{\n", self.name());
        fprint!(file, "    \"{}_selectlist\"[label=\"select_list\",shape=box,style=filled];\n", self.name());
        if is_main_qb {
            fprint!(file, "    color = \"red\"\n");
        }

        // Write select_list
        fprint!(file, "  subgraph cluster_select_list{} {{\n", self.name());
        for (ix, nexpr) in self.select_list.iter().enumerate() {
            let expr_key = nexpr.expr_key;
            QGM::write_expr_to_graphvis(qgm, expr_key, file, Some(ix))?;
            let childid_name = expr_key.to_string();
            fprint!(file, "    exprnode{} -> \"{}_selectlist\";\n", childid_name, self.name());
        }
        fprint!(file, "}}\n");

        // Write quns
        for qun in self.quns.iter().rev() {
            fprint!(
                file,
                "    \"{}\"[label=\"{}\", fillcolor=black, fontcolor=white, style=filled]\n",
                qun.name(),
                qun.display()
            );
        }

        // Write pred_list
        if let Some(pred_list) = self.pred_list.as_ref() {
            fprint!(file, "  subgraph cluster_pred_list{} {{\n", self.name());

            for &expr_key in pred_list {
                QGM::write_expr_to_graphvis(qgm, expr_key, file, None)?;
                let id = expr_key.to_string();
                fprint!(file, "    exprnode{} -> {}_pred_list;\n", id, self.name());
            }
            fprint!(file, "    \"{}_pred_list\"[label=\"pred_list\",shape=box,style=filled];\n", self.name());
            fprint!(file, "}}\n");
        }

        // Write group_by
        if let Some(group_by) = self.group_by.as_ref() {
            fprint!(file, "  subgraph cluster_group_by{} {{\n", self.name());

            fprint!(file, "    \"{}_group_by\"[label=\"group_by\",shape=box,style=filled];\n", self.name());

            for (ix, &expr_key) in group_by.iter().enumerate() {
                QGM::write_expr_to_graphvis(qgm, expr_key, file, Some(ix))?;
                let childid_name = expr_key.to_string();
                fprint!(file, "    exprnode{} -> \"{}_group_by\";\n", childid_name, self.name());
            }
            fprint!(file, "}}\n");
        }

        // Write having_clause
        if let Some(having_clause) = self.having_clause.as_ref() {
            fprint!(file, "  subgraph cluster_having_clause{} {{\n", self.name());

            for &expr_key in having_clause {
                QGM::write_expr_to_graphvis(qgm, expr_key, file, None)?;

                let id = expr_key.to_string();
                fprint!(file, "    exprnode{} -> {}_having_clause;\n", id, self.name());
            }
            fprint!(file, "    \"{}_having_clause\"[label=\"having_clause\",shape=box,style=filled];\n", self.name());
            fprint!(file, "}}\n");
        }
        fprint!(file, "    label = \"{} type={:?}\";\n", self.name(), self.qbtype);

        fprint!(file, "}}\n");
        // --- end query block cluster ---

        // Write referenced query blocks
        for qun in self.quns.iter().rev() {
            if let Some(qbkey) = qun.get_qblock() {
                let qblock = &qgm.qblock_graph.get(qbkey).value;
                fprint!(file, "    \"{}\" -> \"{}_selectlist\";\n", qun.name(), qblock.name());
                //qblock.write_qblock_to_graphviz(qgm, file)?
            }
        }

        Ok(())
    }
}

impl Bitset<QunCol> {
    fn printable(&self, qgm: &QGM) -> String {
        let cols = self
            .elements()
            .iter()
            .map(|&quncol| {
                let colname = qgm.metadata.get_fieldname(quncol);
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
        printable_preds(&self.elements(), qgm, do_escape, true)
    }
}

pub fn printable_preds(preds: &Vec<ExprKey>, qgm: &QGM, do_escape: bool, do_column_split: bool) -> String {
    let mut predstring = String::from("");

    if do_column_split {
        predstring.push_str("{");
    }
    for (ix, &pred_key) in preds.iter().enumerate() {
        let predstr = pred_key.printable(&qgm.expr_graph, do_escape);
        predstring.push_str(&predstr);
        if ix < preds.len() - 1 {
            if do_column_split {
                predstring.push_str("|")
            } else {
                predstring.push_str(",")
            }
        }
    }
    if do_column_split {
        predstring.push_str("}");
    }
    predstring
}

pub fn printable_virtcols(preds: &Vec<VirtCol>, qgm: &QGM, do_escape: bool) -> String {
    let mut predstring = String::from("");
    for (ix, expr_key) in preds.iter().enumerate() {
        let predstr = expr_key.printable(&qgm.expr_graph, do_escape);
        predstring.push_str(&predstr);
        if ix < preds.len() - 1 {
            predstring.push_str("|")
        }
    }
    predstring.push_str("");
    predstring
}

impl POPKey {
    pub fn printable_key(&self, stage_id: StageId) -> String {
        format!("{:?}_stage{}", *self, stage_id).replace("(", "").replace(")", "")
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
