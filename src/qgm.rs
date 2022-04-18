use crate::includes::*;

use crate::expr::*;
use crate::graph::*;
use crate::metadata::*;

use std::collections::HashMap;

use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::rc::Rc;

use std::fmt;
use std::process::Command;

pub type QueryBlockGraph = Graph<QueryBlockKey, QueryBlock, ()>;

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

pub struct QGMMetadata {
    tabledescmap: HashMap<QunId, Rc<dyn TableDesc>>,
}

impl QGMMetadata {
    pub fn new() -> QGMMetadata {
        QGMMetadata {
            tabledescmap: HashMap::new(),
        }
    }

    pub fn add_tabledesc(&mut self, qunid: QunId, tabledesc: Rc<dyn TableDesc>) {
        self.tabledescmap.insert(qunid, tabledesc);
    }

    pub fn get_colname(&self, quncol: QunCol) -> String {
        if let Some(tabledesc) = self.tabledescmap.get(&quncol.0) {
            tabledesc.columns()[quncol.1].name.clone()
        } else {
            format!("${:?}", quncol.1)
        }
    }

    pub fn get_tabledesc(&self, qunid: QunId) -> Option<&Rc<dyn TableDesc>> {
        self.tabledescmap.get(&qunid)
    }
}

impl fmt::Debug for QGMMetadata {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("QGMMetadata").finish()
    }
}

#[derive(Debug)]
pub struct QGM {
    pub main_qblock_key: QueryBlockKey,
    pub cte_list: Vec<QueryBlockKey>,
    pub qblock_graph: QueryBlockGraph,
    pub expr_graph: ExprGraph,

    pub metadata: QGMMetadata,
}

impl QGM {
    pub fn new(
        main_qblock: QueryBlockKey, cte_list: Vec<QueryBlockKey>, qblock_graph: QueryBlockGraph, expr_graph: ExprGraph,
    ) -> QGM {
        QGM {
            main_qblock_key: main_qblock,
            cte_list,
            qblock_graph,
            expr_graph,
            metadata: QGMMetadata::new(),
        }
    }

    pub fn populate_metadata(&mut self) {
        let mut metadata = &self.metadata;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedExpr {
    pub alias: Option<String>,
    pub expr_key: ExprKey,
}

impl NamedExpr {
    pub fn new(alias: Option<String>, expr_key: ExprKey, graph: &ExprGraph) -> Self {
        let expr = &graph.get(expr_key).value;
        let mut alias = alias;
        if alias.is_none() {
            if let Expr::Column {
                prefix: tablename,
                colname,
                qunid: qunid,
                colid: colid,
            } = expr
            {
                alias = Some(colname.clone())
            } else if let Expr::Star = expr {
                alias = Some("*".to_string())
            }
        }

        NamedExpr { alias, expr_key }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryBlockType {
    Select,
    GroupBy,
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Serialize, Deserialize)]
pub struct Quantifier {
    pub id: QunId,
    pub tablename: Option<String>,
    pub qblock: Option<QueryBlockKey>,
    pub alias: Option<String>,
    pub pred_list: Option<ExprKey>,

    #[serde(skip)]
    pub tabledesc: Option<Rc<dyn TableDesc>>,
}

impl fmt::Debug for Quantifier {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Quantifier")
            .field("id", &self.id)
            .field("name", &self.tablename)
            .field("alias", &self.alias)
            .finish()
    }
}

impl Quantifier {
    pub fn new(id: QunId, name: Option<String>, qblock: Option<QueryBlockKey>, alias: Option<String>) -> Self {
        // Either we have a named base table, or we have a queryblock (but not both)
        assert!((name.is_some() && qblock.is_none()) || (name.is_none() && qblock.is_some()));

        Quantifier {
            id,
            tablename: name,
            qblock,
            alias,
            pred_list: None,
            tabledesc: None,
        }
    }

    pub fn is_base_table(&self) -> bool {
        self.qblock.is_none()
    }

    pub fn matches_name_or_alias(&self, prefix: &String) -> bool {
        self.tablename.as_ref().map(|e| e == prefix).unwrap_or(false)
            || self.alias.as_ref().map(|e| e == prefix).unwrap_or(false)
    }

    pub fn display(&self) -> String {
        format!(
            "QUN_{} {}/{}",
            self.id,
            self.tablename.as_ref().unwrap_or(&"".to_string()),
            self.alias.as_ref().unwrap_or(&"".to_string()),
        )
    }

    pub fn name(&self) -> String {
        format!("QUN_{}", self.id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DistinctProperty {
    All,
    Distinct,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryBlock {
    pub id: QBId,
    pub name: Option<String>,
    pub qbtype: QueryBlockType,
    pub select_list: Vec<NamedExpr>,
    pub quns: Vec<Quantifier>,
    pub pred_list: Option<Vec<ExprKey>>,
    pub group_by: Option<Vec<ExprKey>>,
    pub having_clause: Option<Vec<ExprKey>>,
    pub order_by: Option<Vec<(ExprKey, Ordering)>>,
    pub distinct: DistinctProperty,
    pub topN: Option<usize>,
}

impl QueryBlock {
    pub fn new(
        id: QBId, name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>,
        pred_list: Option<Vec<ExprKey>>, group_by: Option<Vec<ExprKey>>, having_clause: Option<Vec<ExprKey>>,
        order_by: Option<Vec<(ExprKey, Ordering)>>, distinct: DistinctProperty, topN: Option<usize>,
    ) -> Self {
        QueryBlock {
            id,
            name,
            qbtype,
            select_list,
            quns,
            pred_list,
            group_by,
            having_clause,
            order_by,
            distinct,
            topN,
        }
    }

    pub fn new0(id: QBId, qbtype: QueryBlockType) -> Self {
        QueryBlock {
            id,
            name: None,
            qbtype,
            select_list: vec![],
            quns: vec![],
            pred_list: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            distinct: DistinctProperty::All,
            topN: None,
        }
    }

    pub fn name(&self) -> String {
        format!("QB_{}", self.id)
        /*
        if self.name.is_some() {
            format!("{}", self.name.as_ref().unwrap())
        } else if self.qbtype == QueryBlockType::Main {
            format!("QB_main")
        } else {
            format!("QB_{}", self.id)
        }
        */
    }

    pub fn set_name(&mut self, name: Option<String>) {
        self.name = name
    }
}

impl QueryBlock {
    pub fn write_qblock_to_graphviz(&self, qgm: &QGM, file: &mut File) -> Result<(), String> {
        // Write current query block first
        let s = "".to_string();
        let select_names: Vec<&String> = self
            .select_list
            .iter()
            .map(|e| e.alias.as_ref().unwrap_or(&s))
            .collect();
        let select_names = format!("{:?}", select_names).replace("\"", "");

        // --- begin query block cluster ---
        fprint!(file, "  subgraph cluster_{} {{\n", self.name());
        fprint!(
            file,
            "    \"{}_selectlist\"[label=\"select_list\",shape=box,style=filled];\n",
            self.name()
        );

        // Write select_list
        fprint!(file, "  subgraph cluster_select_list{} {{\n", self.name());
        for (ix, nexpr) in self.select_list.iter().enumerate() {
            let expr_key = nexpr.expr_key;
            QGM::write_expr_to_graphvis(qgm, expr_key, file, Some(ix));
            let childid_name = expr_key.to_string();
            fprint!(
                file,
                "    exprnode{} -> \"{}_selectlist\";\n",
                childid_name,
                self.name()
            );
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
            if let Some(qbkey) = qun.qblock {
                let qblock = &qgm.qblock_graph.get(qbkey).value;
                //fprint!(file, "    \"{}\" -> \"{}_selectlist\";\n", qun.name(), qblock.name());
                //qblock.write_qblock_to_graphviz(qgm, file)?
            }
        }

        // Write pred_list
        if let Some(pred_list) = self.pred_list.as_ref() {
            fprint!(file, "  subgraph cluster_pred_list{} {{\n", self.name());

            for &expr_key in pred_list {
                QGM::write_expr_to_graphvis(qgm, expr_key, file, None);
                let id = expr_key.to_string();
                let (expr, _, children) = qgm.expr_graph.get3(expr_key);
                fprint!(file, "    exprnode{} -> {}_pred_list;\n", id, self.name());
            }
            fprint!(
                file,
                "    \"{}_pred_list\"[label=\"pred_list\",shape=box,style=filled];\n",
                self.name()
            );
            fprint!(file, "}}\n");
        }

        // Write group_by
        if let Some(group_by) = self.group_by.as_ref() {
            fprint!(file, "  subgraph cluster_group_by{} {{\n", self.name());

            fprint!(
                file,
                "    \"{}_group_by\"[label=\"group_by\",shape=box,style=filled];\n",
                self.name()
            );

            for (ix, &expr_key) in group_by.iter().enumerate() {
                QGM::write_expr_to_graphvis(qgm, expr_key, file, Some(ix));
                let childid_name = expr_key.to_string();
                fprint!(file, "    exprnode{} -> \"{}_group_by\";\n", childid_name, self.name());
            }
            fprint!(file, "}}\n");
        }

        // Write having_clause
        if let Some(having_clause) = self.having_clause.as_ref() {
            fprint!(file, "  subgraph cluster_having_clause{} {{\n", self.name());

            for &expr_key in having_clause {
                QGM::write_expr_to_graphvis(qgm, expr_key, file, None);

                let id = expr_key.to_string();
                let (expr, _, children) = qgm.expr_graph.get3(expr_key);
                fprint!(file, "    exprnode{} -> {}_having_clause;\n", id, self.name());
            }
            fprint!(
                file,
                "    \"{}_having_clause\"[label=\"having_clause\",shape=box,style=filled];\n",
                self.name()
            );
            fprint!(file, "}}\n");
        }
        fprint!(file, "    label = \"{} type={:?}\";\n", self.name(), self.qbtype);

        fprint!(file, "}}\n");
        // --- end query block cluster ---

        // Write referenced query blocks
        for qun in self.quns.iter().rev() {
            if let Some(qbkey) = qun.qblock {
                let qblock = &qgm.qblock_graph.get(qbkey).value;
                fprint!(file, "    \"{}\" -> \"{}_selectlist\";\n", qun.name(), qblock.name());
                qblock.write_qblock_to_graphviz(qgm, file)?
            }
        }

        Ok(())
    }
}

pub struct ParserState {
    pub qblock_graph: QueryBlockGraph,
    pub expr_graph: ExprGraph,
}

impl ParserState {
    pub fn new() -> Self {
        ParserState {
            qblock_graph: Graph::new(),
            expr_graph: Graph::new(),
        }
    }
}

impl QGM {
    pub fn main_qblock(&self) -> &QueryBlock {
        let qbkey = self.main_qblock_key;
        let qblock = &self.qblock_graph.get(qbkey).value;
        qblock
    }

    pub fn write_qgm_to_graphviz(&self, filename: &str, open_jpg: bool) -> Result<(), String> {
        let mut file = std::fs::File::create(filename).map_err(|err| f!("{:?}: {}", err, filename))?;
        
        fprint!(file, "digraph example1 {{\n");
        //fprint!(file, "    node [style=filled,color=white];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        //fprint!(file, "    splines=polyline;\n");
        //fprint!(file, "    style=filled;\n");
        //fprint!(file, "    color=lightgrey;\n");
        //fprint!(file, "    node [style=filled,color=white];\n");

        self.main_qblock().write_qblock_to_graphviz(self, &mut file)?;

        // Write subqueries (CTEs)
        for &qbkey in self.cte_list.iter() {
            let qblock = &self.qblock_graph.get(qbkey).value;
            qblock.write_qblock_to_graphviz(self, &mut file)?;
        }

        fprint!(file, "}}\n");

        drop(file);

        let ofilename = format!("{}.jpg", filename);
        let oflag = format!("-o{}.jpg", filename);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(filename)
            .status()
            .expect("failed to execute process");

        if open_jpg {
            let _cmd = Command::new("open")
                .arg(ofilename)
                .status()
                .expect("failed to execute process");
        }
        Ok(())
    }

    fn write_expr_to_graphvis(
        qgm: &QGM, expr_key: ExprKey, file: &mut File, order_ix: Option<usize>,
    ) -> std::io::Result<()> {
        let id = expr_key.to_string();
        let (expr, _, children) = qgm.expr_graph.get3(expr_key);
        let ix_str = if let Some(ix) = order_ix {
            format!(": {}", ix)
        } else {
            String::from("")
        };

        fprint!(file, "    exprnode{}[label=\"{}{}\"];\n", id, expr.name(), ix_str);
        if let Some(children) = children {
            for &childid in children {
                let childid_name = childid.to_string();

                fprint!(file, "    exprnode{} -> exprnode{};\n", childid_name, id);
                Self::write_expr_to_graphvis(qgm, childid, file, None)?;
            }
        }
        Ok(())
    }
}
