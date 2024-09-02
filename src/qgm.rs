// qgm: query graph model

use std::{collections::HashMap, fmt, io::Write, process::Command, rc::Rc};

use crate::{
    expr::ExprGraph,
    graph::{ExprKey, Graph, QueryBlockKey},
    includes::*,
    metadata::TableDesc,
};

pub type QueryBlockGraph = Graph<QueryBlockKey, QueryBlock, ()>;

#[derive(Default)]
pub struct QGMMetadata {
    tabledescmap: HashMap<QunId, Rc<dyn TableDesc>>,
}

impl QGMMetadata {
    pub fn add_tabledesc(&mut self, qunid: QunId, tabledesc: Rc<dyn TableDesc>) {
        self.tabledescmap.insert(qunid, tabledesc);
    }

    pub fn get_tabledesc(&self, qunid: QunId) -> Option<Rc<dyn TableDesc>> {
        self.tabledescmap.get(&qunid).cloned()
    }

    pub fn get_fieldname(&self, quncol: QunCol) -> String {
        if let Some(tabledesc) = self.tabledescmap.get(&quncol.0) {
            tabledesc.fields()[quncol.1].name.clone()
        } else {
            format!("${:?}", quncol.1)
        }
    }

    pub fn get_field(&self, quncol: QunCol) -> Option<&Field> {
        self.tabledescmap.get(&quncol.0).map(|tabledesc| &tabledesc.fields()[quncol.1])
    }

    pub fn get_fieldtype(&self, quncol: QunCol) -> Option<DataType> {
        let fopt = self.tabledescmap.get(&quncol.0).map(|tabledesc| &tabledesc.fields()[quncol.1]);
        fopt.map(|f| f.data_type().clone())
    }
}

impl fmt::Debug for QGMMetadata {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        QGM { main_qblock_key: main_qblock, cte_list, qblock_graph, expr_graph, metadata: QGMMetadata::default() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedExpr {
    pub alias: Option<String>,
    pub expr_key: ExprKey,
}

impl NamedExpr {
    pub fn new(alias: Option<String>, expr_key: ExprKey) -> Self {
        NamedExpr { alias, expr_key }
    }

    pub fn get_name(&self) -> String {
        if let Some(name) = self.alias.as_ref() {
            name.clone()
        } else {
            format!("col_{:?}", self.expr_key.id())
        }
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
    pub on_clause: ExprKey,
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
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Quantifier").field("id", &self.id).field("name", &self.get_basename()).field("alias", &self.get_alias()).finish()
    }
}

impl Quantifier {
    pub fn new(id: QunId, source: QuantifierSource, alias: Option<String>) -> Self {
        let mut alias = alias;
        if alias.is_none() {
            // For unaliased base references, the basename becomes the alias i.e. "SELECT * FROM TABLE" -> "SELECT * FROM TABLE AS TABLE"
            if let QuantifierSource::Basename(bn) = &source {
                alias = Some(bn.clone())
            }
        }
        Quantifier { id, source, alias, tabledesc: None }
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
        format!("QUN_{} {}/{}", self.id, self.get_basename().unwrap_or(&"".to_string()), self.get_alias().unwrap_or(&"".to_string()),)
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
        group_by: Option<Vec<ExprKey>>, having_clause: Option<Vec<ExprKey>>, order_by: Option<Vec<(ExprKey, Ordering)>>, distinct: DistinctProperty, top_n: Option<usize>,
    ) -> Self {
        QueryBlock { id, name, qbtype, select_list, quns, pred_list, group_by, having_clause, order_by, distinct, top_n }
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

#[derive(Default)]
pub struct ParserState {
    pub qblock_graph: QueryBlockGraph,
    pub expr_graph: ExprGraph,
}

impl QueryBlockGraph {
    pub fn foo(&self) {
        println!("Hello")
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
        fprint!(file, "    node [shape=record];\n");

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
        let _cmd = Command::new("/usr/local/bin/dot").arg("-Tjpg").arg(oflag).arg(pathname).status().expect("failed to execute process");

        if open_jpg {
            let _cmd = Command::new("open").arg(opathname).status().expect("failed to execute process");
        }
        Ok(())
    }
}
