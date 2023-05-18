// qgm: query graph model

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::Write;
use std::process::Command;
use std::rc::Rc;

use crate::expr::*;
use crate::graph::*;
use crate::includes::*;
use crate::metadata::*;

pub type QueryBlockGraph = Graph<QueryBlockKey, QueryBlock, ()>;

pub struct QGMMetadata {
    tabledescmap: HashMap<QunId, Rc<dyn TableDesc>>,
}

impl QGMMetadata {
    pub fn new() -> QGMMetadata {
        QGMMetadata { tabledescmap: HashMap::new() }
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

    pub fn get_tabledesc(&self, qunid: QunId) -> Option<Rc<dyn TableDesc>> {
        self.tabledescmap.get(&qunid).map(|td| Rc::clone(td))
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
    pub fn new(main_qblock: QueryBlockKey, cte_list: Vec<QueryBlockKey>, qblock_graph: QueryBlockGraph, expr_graph: ExprGraph) -> QGM {
        QGM {
            main_qblock_key: main_qblock,
            cte_list,
            qblock_graph,
            expr_graph,
            metadata: QGMMetadata::new(),
        }
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
            if let Expr::Column { colname, .. } = expr {
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
pub enum QuantifierSource {
    Basename(String),
    QueryBlock(QueryBlockKey),
    AnsiJoin(AnsiJoin),
}

#[derive(Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Serialize, Deserialize)]
pub struct AnsiJoin {
    pub join_type: JoinType,
    pub left: Box<Quantifier>,
    pub right: Box<Quantifier>,
}

#[derive(Serialize, Deserialize)]
pub struct Quantifier {
    pub id: QunId,
    source: QuantifierSource,
    alias: Option<String>,
    #[serde(skip)]
    pub tabledesc: Option<Rc<dyn TableDesc>>,
}

impl fmt::Debug for Quantifier {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Quantifier")
            .field("id", &self.id)
            .field("name", &self.get_basename())
            .field("alias", &self.get_alias())
            .finish()
    }
}

impl Quantifier {
    pub fn new(id: QunId, source: QuantifierSource, alias: Option<String>) -> Self {
        Quantifier {
            id,
            source,
            alias,
            tabledesc: None,
        }
    }

    pub fn new_base(id: QunId, name: String, alias: Option<String>) -> Self {
        let source = QuantifierSource::Basename(name);
        Quantifier::new(id, source, alias)
    }

    pub fn new_qblock(id: QunId, qblock: QueryBlockKey, alias: Option<String>) -> Self {
        let source = QuantifierSource::QueryBlock(qblock);
        Quantifier::new(id, source, alias)
    }

    pub fn new_ansijoin(id: QunId, ansi_join: AnsiJoin, alias: Option<String>) -> Self {
        let source = QuantifierSource::AnsiJoin(ansi_join);
        Quantifier::new(id, source, alias)
    }

    pub fn is_base_table(&self) -> bool {
        matches!(self.source, QuantifierSource::Basename(_))
    }

    pub fn matches_name_or_alias(&self, prefix: &String) -> bool {
        self.get_basename().map(|e| e == prefix).unwrap_or(false) || self.get_alias().map(|e| e == prefix).unwrap_or(false)
    }

    pub fn display(&self) -> String {
        format!(
            "QUN_{} {}/{}",
            self.id,
            self.get_basename().unwrap_or(&"".to_string()),
            self.get_alias().unwrap_or(&"".to_string()),
        )
    }

    pub fn name(&self) -> String {
        format!("QUN_{}", self.id)
    }

    pub fn get_basename(&self) -> Option<&String> {
        if let QuantifierSource::Basename(basename) = &self.source {
            Some(basename)
        } else {
            None
        }
    }

    pub fn get_qblock(&self) -> Option<QueryBlockKey> {
        if let QuantifierSource::QueryBlock(qblock) = self.source {
            Some(qblock)
        } else {
            None
        }
    }

    pub fn get_alias(&self) -> Option<&String> {
        self.alias.as_ref()
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
    pub top_n: Option<usize>,
}

impl QueryBlock {
    pub fn new(
        id: QBId, name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>, pred_list: Option<Vec<ExprKey>>,
        group_by: Option<Vec<ExprKey>>, having_clause: Option<Vec<ExprKey>>, order_by: Option<Vec<(ExprKey, Ordering)>>, distinct: DistinctProperty,
        top_n: Option<usize>,
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
            top_n,
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
            top_n: None,
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

    pub fn is_main_qblock(&self, qbkey: QueryBlockKey) -> bool {
        self.main_qblock_key == qbkey
    }

    pub fn write_qgm_to_graphviz(&self, pathname: &str, open_jpg: bool) -> Result<(), String> {
        let mut file = std::fs::File::create(pathname).map_err(|err| f!("{:?}: {}", err, pathname))?;

        fprint!(file, "digraph example1 {{\n");
        //fprint!(file, "    node [style=filled,color=white];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        //fprint!(file, "    splines=polyline;\n");
        //fprint!(file, "    style=filled;\n");
        //fprint!(file, "    color=lightgrey;\n");
        //fprint!(file, "    node [style=filled,color=white];\n");

        // self.main_qblock().write_qblock_to_graphviz(self, &mut file)?;

        // Write all query blocks
        for qbkey in self.iter_qblocks() {
            let qblock = &self.qblock_graph.get(qbkey).value;
            qblock.write_qblock_to_graphviz(self.is_main_qblock(qbkey), self, &mut file)?;
        }

        fprint!(file, "}}\n");

        drop(file);

        let opathname = format!("{}.jpg", pathname);
        let oflag = format!("-o{}.jpg", pathname);

        // dot -Tjpg -oex.jpg exampl1.dot
        let _cmd = Command::new("dot")
            .arg("-Tjpg")
            .arg(oflag)
            .arg(pathname)
            .status()
            .expect("failed to execute process");

        if open_jpg {
            let _cmd = Command::new("open").arg(opathname).status().expect("failed to execute process");
        }
        Ok(())
    }

    fn write_expr_to_graphvis(qgm: &QGM, expr_key: ExprKey, file: &mut File, order_ix: Option<usize>) -> Result<(), String> {
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
}
