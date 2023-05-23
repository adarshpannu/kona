// Compile

use crate::{
    bitset::Bitset,
    flow::Flow,
    graph::{ExprKey, Graph, LOPKey, POPKey},
    includes::*,
    lop::{VirtCol, LOPGraph, LOP},
    metadata::{PartType, TableType},
    pcode::PCode,
    pop::{ColumnPosition, ColumnPositionTable, POPGraph, POPProps, POP},
    pop_aggregation,
    pop_csv::{CSVDir, CSV},
    pop_hashjoin, pop_repartition,
    qgm::QGM,
    stage::StageGraph,
};

impl POP {
    pub fn compile(env: &Env, qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey) -> Result<Flow, String> {
        // Build physical plan
        let mut pop_graph: POPGraph = Graph::new();
        let mut stage_graph = StageGraph::new();

        let root_stage_id = stage_graph.add_stage(lop_key, None);
        let root_pop_key = Self::compile_lop(qgm, &lop_graph, lop_key, &mut pop_graph, &mut stage_graph, root_stage_id)?;
        stage_graph.set_pop_key(&pop_graph, root_stage_id, root_pop_key);

        // Diagnostics
        stage_graph.print();

        let plan_pathname = format!("{}/{}", env.output_dir, "pop.dot");
        QGM::write_physical_plan_to_graphviz(qgm, &stage_graph, &pop_graph, root_pop_key, &plan_pathname)?;

        // Build flow (POPs + Stages)
        let flow = Flow { pop_graph, stage_graph };
        return Ok(flow);
    }

    pub fn compile_lop(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, stage_graph: &mut StageGraph, stage_id: StageId,
    ) -> Result<POPKey, String> {
        let (lop, _, lop_children) = lop_graph.get3(lop_key);

        // Do we have a new stage?
        let effective_stage_id = if matches!(lop, LOP::Repartition { .. }) {
            stage_graph.add_stage(lop_key, Some(stage_id))
        } else {
            stage_id
        };

        // Compile children first
        let mut pop_children = vec![];
        if let Some(lop_children) = lop_children {
            for lop_child_key in lop_children {
                let pop_key = Self::compile_lop(qgm, lop_graph, *lop_child_key, pop_graph, stage_graph, effective_stage_id)?;
                pop_children.push(pop_key);
            }
        }

        let pop_key: POPKey = match lop {
            LOP::TableScan { .. } => Self::compile_scan(qgm, lop_graph, lop_key, pop_graph)?,
            LOP::HashJoin { .. } => Self::compile_join(qgm, lop_graph, lop_key, pop_graph, pop_children)?,
            LOP::Repartition { .. } => Self::compile_repartition(qgm, lop_graph, lop_key, pop_graph, pop_children)?,
            LOP::Aggregation { .. } => Self::compile_aggregation(qgm, lop_graph, lop_key, pop_graph, pop_children)?,
        };

        if stage_id != effective_stage_id {
            stage_graph.set_pop_key(pop_graph, effective_stage_id, pop_key)
        }

        // Assign indexes to POP, increment stage pop count
        let new_pop_count: usize = stage_graph.increment_pop(effective_stage_id);
        let mut props = &mut pop_graph.get_mut(pop_key).properties;
        props.index_in_stage = new_pop_count - 1;

        // Add RepartionRead
        if matches!(lop, LOP::Repartition { .. }) {
            let pop_key: POPKey = Self::compile_repartition_read(qgm, lop_graph, lop_key, pop_graph, vec![pop_key])?;

            let new_pop_count: usize = stage_graph.increment_pop(stage_id);
            let mut props = &mut pop_graph.get_mut(pop_key).properties;
            props.index_in_stage = new_pop_count - 1;

            return Ok(pop_key);
        }
        Ok(pop_key)
    }

    pub fn compile_scan(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph) -> Result<POPKey, String> {
        let (lop, lopprops, ..) = lop_graph.get3(lop_key);
        let cpt: ColumnPositionTable = Self::compute_column_position_table(qgm, lop_graph, lop_key);

        let qunid = lopprops.quns.elements()[0];
        let tbldesc = qgm.metadata.get_tabledesc(qunid).unwrap();

        //let coltypes = columns.iter().map(|col| col.data_type.clone()).collect();

        // Build input map
        let projection: Vec<ColId> = if let LOP::TableScan { projection } = lop {
            projection.elements().iter().map(|&quncol| quncol.1).collect()
        } else {
            return Err(format!("Internal error: compile_scan() received a POP that isn't a TableScan"));
        };
        debug!("Compile projection lopkey: {:?}", projection);

        // Compile predicates
        debug!("Compile predicate for lopkey: {:?}", lop_key);

        let predicates = Self::compile_predicates(qgm, &lopprops.preds, &cpt);

        let pop = match tbldesc.get_type() {
            TableType::CSV => {
                let inner = CSV::new(
                    tbldesc.pathname().clone(),
                    tbldesc.columns().clone(),
                    tbldesc.header(),
                    tbldesc.separator(),
                    lopprops.partdesc.npartitions,
                    projection,
                );
                POP::CSV(inner)
            }
            TableType::CSVDIR => {
                let inner = CSVDir::new(
                    tbldesc.pathname().clone(),
                    tbldesc.columns().clone(),
                    tbldesc.header(),
                    tbldesc.separator(),
                    lopprops.partdesc.npartitions,
                    projection,
                );
                POP::CSVDir(inner)
            }
        };

        // Compile virtcols
        debug!("Compile vcols for lopkey: {:?}", lop_key);
        let virtcols = Self::compile_virtcols(qgm, lopprops.virtcols.as_ref(), &cpt);

        let props = POPProps::new(predicates, virtcols, lopprops.partdesc.npartitions);

        let pop_key = pop_graph.add_node_with_props(pop, props, None);
        Ok(pop_key)
    }

    pub fn compute_column_position_table(_qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey) -> ColumnPositionTable {
        let lop = &lop_graph.get(lop_key).value;
        let mut cpt = ColumnPositionTable::new();

        if let LOP::TableScan { projection: input_cols } = lop {
            for (ix, &quncol) in input_cols.elements().iter().enumerate() {
                let cp = ColumnPosition { column_position: ix };
                cpt.set(quncol, cp);
            }
        } else {
        }
        cpt
    }

    pub fn compile_predicates(qgm: &QGM, preds: &Bitset<ExprKey>, cpt: &ColumnPositionTable) -> Option<Vec<PCode>> {
        if preds.len() > 0 {
            let exprs = preds.elements();
            Self::compile_exprs(qgm, &exprs, cpt)
        } else {
            None
        }
    }

    pub fn compile_exprs(qgm: &QGM, exprs: &Vec<ExprKey>, cpt: &ColumnPositionTable) -> Option<Vec<PCode>> {
        let mut pcodevec = vec![];
        if exprs.len() > 0 {
            for expr_key in exprs.iter() {
                debug!("Compile expression: {:?}", expr_key.printable(&qgm.expr_graph, false));

                let mut pcode = PCode::new();
                expr_key.compile(&qgm.expr_graph, &mut pcode, &cpt);
                pcodevec.push(pcode);
            }
            Some(pcodevec)
        } else {
            None
        }
    }

    pub fn compile_virtcols(qgm: &QGM, virtcols: Option<&Vec<VirtCol>>, cpt: &ColumnPositionTable) -> Option<Vec<PCode>> {
        if let Some(virtcols) = virtcols {
            let pcodevec = virtcols
                .iter()
                .map(|ne| {
                    let mut pcode = PCode::new();
                    ne.expr_key.compile(&qgm.expr_graph, &mut pcode, &cpt);
                    pcode
                })
                .collect::<Vec<_>>();
            Some(pcodevec)
        } else {
            None
        }
    }

    pub fn compile_repartition(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>,
    ) -> Result<POPKey, String> {
        // Repartition split into Repartition + CSVDirScan
        let lopprops = &lop_graph.get(lop_key).properties;

        // We shouldn't have any predicates
        let predicates = None;
        assert!(lopprops.preds.len() == 0);

        let cpt: ColumnPositionTable = Self::compute_column_position_table(qgm, lop_graph, lop_key);

        // Compile cols or virtcols. We will have one or the other
        let virtcols = Self::compile_virtcols(qgm, lopprops.virtcols.as_ref(), &cpt);

        let output_map = None;
        /*
        let output_map: Option<Vec<RegisterId>> = if virtcols.is_none() {
            let output_map = lopprops
                .cols
                .elements()
                .iter()
                .map(|&quncol| {
                    let regid = ra.get_id(quncol);
                    regid
                })
                .collect();
            Some(output_map)
        } else {
            None
        };
        */

        let props = POPProps::new(predicates, virtcols, lopprops.partdesc.npartitions);

        let repart_key = if let PartType::HASHEXPR(_) = &lopprops.partdesc.part_type {
            debug!("Compile pkey start");
            vec![]
            //Self::compile_exprs(qgm, &exprs, &cpt).unwrap()
        } else {
            panic!("Invalid partitioning type")
        };
        debug!("Compile pkey end");

        let pop_inner = pop_repartition::Repartition { output_map, repart_key };
        let pop_key = pop_graph.add_node_with_props(POP::Repartition(pop_inner), props, Some(pop_children));

        Ok(pop_key)
    }

    pub fn compile_repartition_read(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>,
    ) -> Result<POPKey, String> {
        let lopprops = &lop_graph.get(lop_key).properties;

        // We shouldn't have any predicates
        let predicates = None;
        assert!(lopprops.preds.len() == 0);

        let cpt: ColumnPositionTable = Self::compute_column_position_table(qgm, lop_graph, lop_key);

        // Compile cols or virtcols. We will have one or the other
        let virtcols = Self::compile_virtcols(qgm, lopprops.virtcols.as_ref(), &cpt);

        let props = POPProps::new(predicates, virtcols, lopprops.partdesc.npartitions);

        let pop_inner = pop_repartition::RepartitionRead {};
        let pop_key = pop_graph.add_node_with_props(POP::RepartitionRead(pop_inner), props, Some(pop_children));

        Ok(pop_key)
    }

    pub fn compile_join(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>) -> Result<POPKey, String> {
        let lopprops = &lop_graph.get(lop_key).properties;

        // Compile predicates
        //debug!("Compile predicate for lopkey: {:?}", lop_key);
        let _cpt: ColumnPositionTable = Self::compute_column_position_table(qgm, lop_graph, lop_key);

        //let predicates = Self::compile_predicates(qgm, &lopprops.preds, &cpt);
        let predicates = None;

        // Compile virtcols
        //debug!("Compile vcols for lopkey: {:?}", lop_key);
        //let virtcols = Self::compile_virtcols(qgm, lopprops.virtcols.as_ref(), &cpt);
        let virtcols = None;

        let props = POPProps::new(predicates, virtcols, lopprops.partdesc.npartitions);

        let pop_inner = pop_hashjoin::HashJoin {};
        let pop_key = pop_graph.add_node_with_props(POP::HashJoin(pop_inner), props, Some(pop_children));

        Ok(pop_key)
    }

    pub fn compile_aggregation(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, pop_graph: &mut POPGraph, pop_children: Vec<POPKey>,
    ) -> Result<POPKey, String> {
        let lopprops = &lop_graph.get(lop_key).properties;
        let _cpt: ColumnPositionTable = Self::compute_column_position_table(qgm, lop_graph, lop_key);

        // Compile predicates
        debug!("Compile predicate for lopkey: {:?}", lop_key);
        let predicates = None; // todo Self::compile_predicates(qgm, &lopprops.preds, &cpt);

        // Compile virtcols
        debug!("Compile vcols for lopkey: {:?}", lop_key);
        let virtcols = None; // todo Self::compile_virtcols(qgm, lopprops.virtcols.as_ref(), &cpt);

        let props = POPProps::new(predicates, virtcols, lopprops.partdesc.npartitions);

        let pop_inner = pop_aggregation::Aggregation {};
        let pop_key = pop_graph.add_node_with_props(POP::Aggregation(pop_inner), props, Some(pop_children));

        Ok(pop_key)
    }
}
