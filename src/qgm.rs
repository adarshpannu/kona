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

macro_rules! fprint {
    ($file:expr, $($args:expr),*) => {{
        $file.write_all(format!($($args),*).as_bytes());
    }};
}

#[derive(Debug)]
pub enum AST {
    CatalogTable {
        name: String,
        options: Vec<(String, String)>,
    },
    DescribeTable {
        name: String,
    },
    QGM(QGM),
    SetOption {
        name: String,
        value: String,
    },
}

pub enum UIE {
    Union,
    UnionAll,
    Intersect,
    Except
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
            debug!("COLS = {:?}, quncol = {:?}", tabledesc.columns(), quncol);
            tabledesc.columns()[quncol.1].name.clone()
        } else {
            format!("CID({:?})", quncol.1)
        }
    }
}

impl fmt::Debug for QGMMetadata {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("QGMMetadata").finish()
    }
}

#[derive(Debug)]
pub struct QGM {
    pub main_qblock: QueryBlock,
    pub cte_list: Vec<QueryBlockLink>,
    pub graph: ExprGraph,
    pub metadata: QGMMetadata,
}

impl QGM {
    pub fn new(main_qblock: QueryBlock, cte_list: Vec<QueryBlockLink>, graph: ExprGraph) -> QGM {
        QGM {
            main_qblock,
            cte_list,
            graph,
            metadata: QGMMetadata::new()
        }
    }

    pub fn populate_metadata(&mut self) {
        let mut metadata = &self.metadata;

    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamedExpr {
    pub alias: Option<String>,
    pub expr_id: ExprId,
}

impl NamedExpr {
    pub fn new(alias: Option<String>, expr_id: ExprId, graph: &ExprGraph) -> Self {
        let expr = &graph.get(expr_id).contents;
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

        NamedExpr { alias, expr_id }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryBlockType {
    Select,
    GroupBy,
}

pub type QueryBlock0 = (Vec<NamedExpr>, Vec<Quantifier>, Vec<ExprId>);

#[derive(Serialize, Deserialize)]
pub struct Quantifier {
    pub id: QunId,
    pub tablename: Option<String>,
    pub qblock: Option<QueryBlockLink>,
    pub alias: Option<String>,
    pub pred_list: Option<ExprId>,

    #[serde(skip)]
    pub tabledesc: Option<Rc<dyn TableDesc>>,

    #[serde(skip)]
    pub column_read_map: RefCell<HashMap<ColId, ColId>>, // ColID in source tuple -> ColID in target tuple.
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
    pub fn new(id: QunId, name: Option<String>, qblock: Option<QueryBlockLink>, alias: Option<String>) -> Self {
        // Either we have a named base table, or we have a queryblock (but not both)
        assert!((name.is_some() && qblock.is_none()) || (name.is_none() && qblock.is_some()));

        Quantifier {
            id,
            tablename: name,
            qblock,
            alias,
            pred_list: None,
            tabledesc: None,
            column_read_map: RefCell::new(HashMap::new()),
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
        let qbname = if let Some(qblock) = self.qblock.as_ref() {
            let qblock = qblock.borrow();
            format!("{:?}", qblock.qbtype)
        } else {
            String::from("")
        };

        format!(
            "{}/{} {} #{}",
            self.tablename.as_ref().unwrap_or(&"".to_string()),
            self.alias.as_ref().unwrap_or(&"".to_string()),
            qbname,
            self.id
        )
    }

    pub fn name(&self) -> String {
        format!("QUN_{}", self.id)
    }

    pub fn get_column_map(&self) -> HashMap<ColId, ColId> {
        self.column_read_map.borrow().clone()
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
    pub pred_list: Option<Vec<ExprId>>,
    pub group_by: Option<Vec<ExprId>>,
    pub having_clause: Option<Vec<ExprId>>,
    pub order_by: Option<Vec<(ExprId, Ordering)>>,
    pub distinct: DistinctProperty,
    pub topN: Option<usize>,
}

impl QueryBlock {
    pub fn new(
        id: QBId, name: Option<String>, qbtype: QueryBlockType, select_list: Vec<NamedExpr>, quns: Vec<Quantifier>,
        pred_list: Option<Vec<ExprId>>, group_by: Option<Vec<ExprId>>, having_clause: Option<Vec<ExprId>>,
        order_by: Option<Vec<(ExprId, Ordering)>>, distinct: DistinctProperty, topN: Option<usize>,
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
    pub(crate) fn write_qblock_to_graphviz(&self, qgm: &QGM, file: &mut File) -> std::io::Result<()> {
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
            let expr_id = nexpr.expr_id;
            QGM::write_expr_to_graphvis(qgm, expr_id, file, Some(ix));
            let childid_name = QGM::nodeid_to_str(&expr_id);
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
            if let Some(qblock) = &qun.qblock {
                let qblock = &*qblock.borrow();
                //fprint!(file, "    \"{}\" -> \"{}_selectlist\";\n", qun.name(), qblock.name());
                //qblock.write_qblock_to_graphviz(qgm, file)?
            }
        }

        // Write pred_list
        if let Some(pred_list) = self.pred_list.as_ref() {
            fprint!(file, "  subgraph cluster_pred_list{} {{\n", self.name());

            for &expr_id in pred_list {
                QGM::write_expr_to_graphvis(qgm, expr_id, file, None);
                let id = QGM::nodeid_to_str(&expr_id);
                let (expr, _, children) = qgm.graph.get3(expr_id);
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

            for (ix, &expr_id) in group_by.iter().enumerate() {
                QGM::write_expr_to_graphvis(qgm, expr_id, file, Some(ix));
                let childid_name = QGM::nodeid_to_str(&expr_id);
                fprint!(file, "    exprnode{} -> \"{}_group_by\";\n", childid_name, self.name());
            }
            fprint!(file, "}}\n");
        }

        // Write having_clause
        if let Some(having_clause) = self.having_clause.as_ref() {
            fprint!(file, "  subgraph cluster_having_clause{} {{\n", self.name());

            for &expr_id in having_clause {
                QGM::write_expr_to_graphvis(qgm, expr_id, file, None);

                let id = QGM::nodeid_to_str(&expr_id);
                let (expr, _, children) = qgm.graph.get3(expr_id);
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
            if let Some(qblock) = &qun.qblock {
                let qblock = &*qblock.borrow();
                fprint!(file, "    \"{}\" -> \"{}_selectlist\";\n", qun.name(), qblock.name());
                qblock.write_qblock_to_graphviz(qgm, file)?
            }
        }

        Ok(())
    }
}

pub struct ParserState {
    pub graph: ExprGraph,
}

impl ParserState {
    pub fn new() -> Self {
        ParserState { graph: Graph::new() }
    }
}

impl QGM {
    pub(crate) fn write_qgm_to_graphviz(&self, filename: &str, open_jpg: bool) -> std::io::Result<()> {
        let mut file = std::fs::File::create(filename)?;
        fprint!(file, "digraph example1 {{\n");
        //fprint!(file, "    node [style=filled,color=white];\n");
        fprint!(file, "    rankdir=BT;\n"); // direction of DAG
        fprint!(file, "    nodesep=0.5;\n");
        fprint!(file, "    ordering=\"in\";\n");

        //fprint!(file, "    splines=polyline;\n");
        //fprint!(file, "    style=filled;\n");
        //fprint!(file, "    color=lightgrey;\n");
        //fprint!(file, "    node [style=filled,color=white];\n");

        self.main_qblock.write_qblock_to_graphviz(self, &mut file);

        // Write subqueries (CTEs)
        for qblock in self.cte_list.iter() {
            let qblock = &*qblock.borrow();
            qblock.write_qblock_to_graphviz(self, &mut file);
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

    fn nodeid_to_str(nodeid: &ExprId) -> String {
        format!("{:?}", nodeid).replace("(", "").replace(")", "")
    }

    fn write_expr_to_graphvis(
        qgm: &QGM, expr: ExprId, file: &mut File, order_ix: Option<usize>,
    ) -> std::io::Result<()> {
        let id = Self::nodeid_to_str(&expr);
        let (expr, _, children) = qgm.graph.get3(expr);
        let ix_str = if let Some(ix) = order_ix {
            format!(": {}", ix)
        } else {
            String::from("")
        };

        fprint!(file, "    exprnode{}[label=\"{}{}\"];\n", id, expr.name(), ix_str);
        if let Some(children) = children {
            for &childid in children {
                let childid_name = Self::nodeid_to_str(&childid);

                fprint!(file, "    exprnode{} -> exprnode{};\n", childid_name, id);
                Self::write_expr_to_graphvis(qgm, childid, file, None)?;
            }
        }
        Ok(())
    }
}

/*
pub struct QueryBlockIter<'a> {
    queue: Vec<&'a QueryBlock>
}

impl<'a> Iterator for QueryBlockIter<'a> {
    type Item = &'a QueryBlock;
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl QGM {
    pub fn iter_qblock(&self) -> QueryBlockIter {
        let mut queue = vec![&self.main_qblock];
        for cte in self.cte_list.iter() {
            let qblock = &*cte.borrow();
            queue.push(qblock);
        }

        QueryBlockIter { queue }
    }
}
*/

