// Compile

use std::rc::Rc;

use crate::{
    bitset::Bitset,
    expr::Expr,
    flow::Flow,
    graph::{ExprKey, LOPKey, POPKey},
    includes::*,
    lop::{LOPGraph, LOPProps, VirtCol, LOP},
    metadata::{PartType, TableType},
    pcode::PCode,
    pop::{Agg, POPProps, Projection, ProjectionMap, POP},
    pop_csv::CSV,
    pop_hashagg, pop_hashmatch, pop_repartition,
    qgm::QGM,
    stage::{StageGraph, StageLink},
};

impl POP {
    pub fn compile_flow(env: &Env, qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey) -> Result<Flow, String> {
        // Build physical plan
        let mut stage_graph = StageGraph::default();

        let root_stage_id = stage_graph.add_stage(lop_key, None);
        let root_pop_key = Self::compile_lop(qgm, lop_graph, lop_key, &mut stage_graph, root_stage_id)?;
        stage_graph.set_root_pop_key(root_stage_id, root_pop_key);

        // Diagnostics
        stage_graph.print();

        let plan_pathname = format!("{}/{}", env.output_dir, "pop.dot");
        QGM::write_physical_plan_to_graphviz(qgm, &stage_graph, &plan_pathname)?;

        // Get schema
        let schema = lop_key.get_schema(qgm, lop_graph);

        // Build flow (POPs + Stages)
        let flow = Flow { id: env.id, stage_graph, schema };

        Ok(flow)
    }

    pub fn compile_lop(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, stage_graph: &mut StageGraph, stage_id: StageId) -> Result<POPKey, String> {
        let (lop, _, lop_children) = lop_graph.get3(lop_key);

        // Do we have a new stage?
        let (effective_stage_id, stage_link) = if matches!(lop, LOP::Repartition { .. }) {
            let child_stage_id = stage_graph.add_stage(lop_key, Some(stage_id));
            let stage_link = Some(StageLink(child_stage_id, stage_id));
            (child_stage_id, stage_link)
        } else {
            (stage_id, None)
        };

        // Compile children first
        let mut pop_children = vec![];
        if let Some(lop_children) = lop_children {
            for lop_child_key in lop_children {
                let pop_key = Self::compile_lop(qgm, lop_graph, *lop_child_key, stage_graph, effective_stage_id)?;
                pop_children.push(pop_key);
            }
        }

        // Get schema to write+read repartitioning files to disk (arrow2)
        let schema = if matches!(lop, LOP::Repartition { .. }) { Some(Rc::new(lop_key.get_schema(qgm, lop_graph))) } else { None };

        let pop_children_clone = pop_children.clone();

        let pop_key: POPKey = match lop {
            LOP::TableScan { .. } => Self::compile_scan(qgm, lop_graph, lop_key, stage_graph, effective_stage_id)?,
            LOP::HashJoin { .. } => Self::compile_join(qgm, lop_graph, lop_key, stage_graph, effective_stage_id, pop_children)?,
            LOP::Repartition { cpartitions } => {
                Self::compile_repartition_write(qgm, lop_graph, lop_key, stage_graph, stage_link.unwrap(), pop_children, schema.clone().unwrap(), *cpartitions)?
            }
            LOP::Aggregation { .. } => Self::compile_aggregation(qgm, lop_graph, lop_key, stage_graph, effective_stage_id, pop_children)?,
        };

        debug!("[{:?}] compiled to {:?} in stage {}", lop_key, pop_key, effective_stage_id);
        debug!("[{:?}] children = {:?}", lop_key, pop_children_clone);

        // Add RepartionRead
        if let LOP::Repartition { cpartitions } = lop {
            let read_pop_key: POPKey = Self::compile_repartition_read(qgm, lop_graph, lop_key, stage_graph, stage_link.unwrap(), schema.unwrap(), *cpartitions)?;
            debug!("[{:?}] compiled to {:?} in stage {}", lop_key, read_pop_key, stage_id);

            stage_graph.set_root_pop_key(effective_stage_id, pop_key);
            stage_graph.set_parent_pop_key(effective_stage_id, read_pop_key);

            return Ok(read_pop_key);
        }
        Ok(pop_key)
    }

    pub fn compile_scan(qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, stage_graph: &mut StageGraph, stage_id: StageId) -> Result<POPKey, String> {
        debug!("[{:?}] begin compile_scan", lop_key);

        let (lop, lopprops, ..) = lop_graph.get3(lop_key);

        let qunid = lopprops.quns.elements()[0];
        let tbldesc = qgm.metadata.get_tabledesc(qunid).unwrap();

        // Build input map
        let (input_projection, mut input_proj_map) = if let LOP::TableScan { input_projection } = lop {
            let proj_map: ProjectionMap = Self::compute_projection_map(input_projection, None);
            let input_projection = input_projection.elements().iter().map(|&quncol| quncol.1).collect::<Vec<ColId>>();
            (input_projection, proj_map)
        } else {
            return Err(String::from("Internal error: compile_scan() received a POP that isn't a TableScan"));
        };
        debug!("[{:?}] input_projection: {:?}", lop_key, input_projection);

        // Compile predicates
        let predicates = Self::compile_predicates(qgm, &lopprops.preds, &mut input_proj_map);
        debug!("[{:?}] predicates {:?}", lop_key, predicates);

        let pop = match tbldesc.get_type() {
            TableType::CSV => {
                let inner =
                    CSV::new(tbldesc.pathname().clone(), tbldesc.fields().clone(), tbldesc.header(), tbldesc.separator(), lopprops.partdesc.npartitions, input_projection);
                POP::CSV(inner)
            }
        };

        // Compile real + virt columns
        let (cols, virtcols) = Self::compile_projection(qgm, lop_key, lopprops, &mut input_proj_map);
        let props = POPProps::new(predicates, cols, virtcols, lopprops.partdesc.npartitions);

        let pop_graph = &mut stage_graph.stages[stage_id].pop_graph;
        let pop_key = pop_graph.add_node_with_props(pop, props, None);

        debug!("[{:?}] end compile_scan", lop_key);

        Ok(pop_key)
    }

    pub fn compile_projection(qgm: &QGM, lop_key: LOPKey, lopprops: &LOPProps, proj_map: &mut ProjectionMap) -> (Option<Vec<ColId>>, Option<Vec<PCode>>) {
        let cols = lopprops
            .cols
            .elements()
            .iter()
            .map(|&quncol| {
                let prj = Projection::QunCol(quncol);
                proj_map.get(prj).unwrap()
            })
            .collect::<Vec<ColId>>();
        let cols = if !cols.is_empty() { Some(cols) } else { None };
        debug!("[{:?}] real cols = {:?}", lop_key, cols);

        let virtcols = Self::compile_virtcols(qgm, lopprops.virtcols.as_ref(), proj_map);
        debug!("[{:?}] virt cols = {:?}", lop_key, virtcols);

        (cols, virtcols)
    }

    pub fn compile_virtcols(qgm: &QGM, virtcols: Option<&Vec<VirtCol>>, proj_map: &mut ProjectionMap) -> Option<Vec<PCode>> {
        if let Some(virtcols) = virtcols {
            let pcodevec = virtcols
                .iter()
                .map(|expr_key| {
                    debug!("Compile virtcol: {:?}", expr_key.printable(&qgm.expr_graph, false));
                    let mut pcode = PCode::default();
                    expr_key.compile(&qgm.expr_graph, &mut pcode, proj_map);
                    pcode
                })
                .collect::<Vec<_>>();
            Some(pcodevec)
        } else {
            None
        }
    }

    pub fn compute_projection_map(cols: &Bitset<QunCol>, virtcols: Option<&Vec<VirtCol>>) -> ProjectionMap {
        let mut proj_map = ProjectionMap::default();

        // Add singleton columns
        for (ix, &quncol) in cols.elements().iter().enumerate() {
            let prj = Projection::QunCol(quncol);
            proj_map.set(prj, ix);
        }

        let nrealcols = cols.len();

        // Add virtual columns
        if let Some(virtcols) = virtcols {
            for (ix, &virtcol) in virtcols.iter().enumerate() {
                let prj = Projection::VirtCol(virtcol);
                proj_map.set(prj, nrealcols + ix);
            }
        }
        proj_map
    }

    pub fn compile_predicates(qgm: &QGM, preds: &Bitset<ExprKey>, proj_map: &mut ProjectionMap) -> Option<Vec<PCode>> {
        if !preds.is_empty() {
            let exprs = preds.elements();
            Self::compile_exprs(qgm, &exprs, proj_map)
        } else {
            None
        }
    }

    pub fn compile_exprs(qgm: &QGM, exprs: &Vec<ExprKey>, proj_map: &mut ProjectionMap) -> Option<Vec<PCode>> {
        let mut pcodevec = vec![];
        if !exprs.is_empty() {
            for expr_key in exprs.iter() {
                debug!("Compile expression: {:?}", expr_key.printable(&qgm.expr_graph, false));

                let mut pcode = PCode::default();
                expr_key.compile(&qgm.expr_graph, &mut pcode, proj_map);
                pcodevec.push(pcode);
            }
            Some(pcodevec)
        } else {
            None
        }
    }

    pub fn compile_repartition_write(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, stage_graph: &mut StageGraph, stage_link: StageLink, pop_children: Vec<POPKey>, schema: Rc<Schema>,
        cpartitions: usize,
    ) -> Result<POPKey, String> {
        debug!("[{:?}] begin compile_repartition_write", lop_key);

        let stage_id = stage_link.0;
        let (_, lopprops, children) = lop_graph.get3(lop_key);

        // We shouldn't have any predicates
        let predicates = None;
        assert!(lopprops.preds.is_empty());

        // Build projection map of child. This will be used to resolve any column references in this LOP
        let child_lop_key = children.unwrap()[0];
        let child_lopprops = lop_graph.get_properties(child_lop_key);
        let mut proj_map: ProjectionMap = Self::compute_projection_map(&child_lopprops.cols, child_lopprops.virtcols.as_ref());

        // Compile real + virt columns
        let (cols, virtcols) = Self::compile_projection(qgm, lop_key, lopprops, &mut proj_map);
        let props = POPProps::new(predicates, cols, virtcols, lopprops.partdesc.npartitions);

        let repart_key = if let PartType::HASHEXPR(partkey) = &lopprops.partdesc.part_type {
            debug!("Compile pkey start");
            Self::compile_exprs(qgm, partkey, &mut proj_map).unwrap()
        } else {
            panic!("Invalid partitioning type")
        };
        debug!("Compile pkey end");

        debug!("[{:?}] compile_repartition_write: schema = {:?}", lop_key, &schema);

        let pop_inner = pop_repartition::RepartitionWrite::new(repart_key, schema, stage_link, cpartitions);
        let pop_graph = &mut stage_graph.stages[stage_id].pop_graph;
        let pop_key = pop_graph.add_node_with_props(POP::RepartitionWrite(pop_inner), props, Some(pop_children));

        debug!("[{:?}] end compile_repartition_write", lop_key);

        Ok(pop_key)
    }

    pub fn compile_repartition_read(
        _qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, stage_graph: &mut StageGraph, stage_link: StageLink, schema: Rc<Schema>, npartitions: usize,
    ) -> Result<POPKey, String> {
        debug!("[{:?}] begin compile_repartition_read", lop_key);

        debug!("[{:?}] compile_repartition_read: schema = {:?}", lop_key, &schema);

        let stage_id = stage_link.1;
        let lopprops = &lop_graph.get(lop_key).properties;

        // No predicates
        let predicates = None;

        // Compile cols
        let ncols = lopprops.cols.len() + lopprops.virtcols.as_ref().map_or(0, |v| v.len());
        let cols = Some((0..ncols).collect::<Vec<ColId>>());

        // No virtcols
        let virtcols = None;

        let props = POPProps::new(predicates, cols, virtcols, npartitions);

        let pop_inner = pop_repartition::RepartitionRead::new(schema, stage_link);
        let pop_graph = &mut stage_graph.stages[stage_id].pop_graph;

        let pop_key = pop_graph.add_node_with_props(POP::RepartitionRead(pop_inner), props, None);

        debug!("[{:?}] end compile_repartition_read", lop_key);

        Ok(pop_key)
    }

    pub fn compile_join(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, stage_graph: &mut StageGraph, stage_id: StageId, pop_children: Vec<POPKey>,
    ) -> Result<POPKey, String> {
        debug!("[{:?}] begin compile_join", lop_key);

        let (lop, lopprops, children) = lop_graph.get3(lop_key);
        if let LOP::HashJoin { lhs_join_keys, rhs_join_keys } = lop {
            let keyexprs = [lhs_join_keys, rhs_join_keys];
            let keycols = [0, 1]
                .iter()
                .map(|&child_ix| {
                    // Build projection map of child. This will be used to resolve any column references in this LOP
                    let child_lop_key = children.unwrap()[child_ix];
                    let child_lopprops = lop_graph.get_properties(child_lop_key);
                    let child_proj_map: ProjectionMap = Self::compute_projection_map(&child_lopprops.cols, child_lopprops.virtcols.as_ref());

                    keyexprs[child_ix]
                        .iter()
                        .map(|&expr_key| {
                            let expr = qgm.expr_graph.get_value(expr_key);
                            let prj = match expr {
                                Expr::Column { qunid, colid, .. } => Projection::QunCol(QunCol(*qunid, *colid)),
                                Expr::CID(qunid, colid) => Projection::QunCol(QunCol(*qunid, *colid)),
                                _ => Projection::VirtCol(expr_key),
                            };

                            let colid = child_proj_map.get(prj);
                            if let Some(colid) = colid {
                                colid
                            } else {
                                panic!("compile_join: LOP {:?}, join key {:?} not found in child LOP's projection", lop_key, expr_key.printable(&qgm.expr_graph, false));
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            // Compute child projection maps and consolidate them into one
            let left_child_lop_props = lop_graph.get_properties(children.unwrap()[0]);
            let left_child_proj_map = Self::compute_projection_map(&left_child_lop_props.cols, left_child_lop_props.virtcols.as_ref());
            let right_child_lop_props = lop_graph.get_properties(children.unwrap()[1]);
            let right_child_proj_map = Self::compute_projection_map(&right_child_lop_props.cols, right_child_lop_props.virtcols.as_ref());
            let mut proj_map = left_child_proj_map.append(right_child_proj_map);

            // Compile real + virt columns
            let (cols, virtcols) = Self::compile_projection(qgm, lop_key, lopprops, &mut proj_map);

            let predicates = Self::compile_predicates(qgm, &lopprops.preds, &mut proj_map);
            debug!("[{:?}] predicates {:?}", lop_key, predicates);

            let props = POPProps::new(predicates, cols, virtcols, lopprops.partdesc.npartitions);

            let children_data_types = children.unwrap().iter().map(|child_lop_key| child_lop_key.get_types(qgm, lop_graph)).collect::<Vec<_>>();

            let pop_inner = pop_hashmatch::HashMatch { keycols, children_data_types };
            let pop_graph = &mut stage_graph.stages[stage_id].pop_graph;

            let pop_key = pop_graph.add_node_with_props(POP::HashMatch(pop_inner), props, Some(pop_children));

            debug!("[{:?}] end compile_join", lop_key);
            Ok(pop_key)
        } else {
            panic!("Bad LOP")
        }
    }

    pub fn compile_aggregation(
        qgm: &mut QGM, lop_graph: &LOPGraph, lop_key: LOPKey, stage_graph: &mut StageGraph, stage_id: StageId, pop_children: Vec<POPKey>,
    ) -> Result<POPKey, String> {
        debug!("[{:?}] begin compile_aggregation", lop_key);

        let (lop, lopprops, children) = lop_graph.get3(lop_key);
        if let LOP::Aggregation { key_len } = lop {
            let qunid = lopprops.quns.elements()[0];

            // Populate internal projection-map with key columns.
            let mut internal_proj_map = Self::compute_initial_agg_projection_map(qunid, *key_len);

            // Compile real + virt columns + predicates. As aggregate expressions (e.g. SUM(col) are visited, they are added to the projection map
            // at column offsets beyond the # of key columns.
            let (cols, virtcols) = Self::compile_projection(qgm, lop_key, lopprops, &mut internal_proj_map);
            assert!(cols.is_none());
            let predicates = Self::compile_predicates(qgm, &lopprops.preds, &mut internal_proj_map);
            debug!("[{:?}] predicates {:?}", lop_key, predicates);

            let props = POPProps::new(predicates, cols, virtcols, lopprops.partdesc.npartitions);

            let aggs = Self::build_agg_list(&internal_proj_map);
            debug!("aggs = {:?}", &aggs);

            // The first `key_len` columns are keycols
            let keycols: Vec<Vec<ColId>> = vec![(0..*key_len).collect()];

            let child_lop_key = children.unwrap()[0];
            let child_data_types = child_lop_key.get_types(qgm, lop_graph);

            let pop_inner = pop_hashagg::HashAgg { keycols, child_data_types, aggs };

            let pop_graph = &mut stage_graph.stages[stage_id].pop_graph;
            let pop_key = pop_graph.add_node_with_props(POP::HashAgg(pop_inner), props, Some(pop_children));

            debug!("[{:?}] end compile_aggregation", lop_key);

            Ok(pop_key)
        } else {
            panic!("Bad LOP")
        }
    }

    pub fn compute_initial_agg_projection_map(qunid: QunId, key_len: usize) -> ProjectionMap {
        let mut proj_map = ProjectionMap::default();
        for colid in 0..key_len {
            proj_map.set(Projection::QunCol(QunCol(qunid, colid)), colid);
        }
        proj_map
    }

    pub fn build_agg_list(projmap: &ProjectionMap) -> Vec<(Agg, ColId)> {
        let mut aggs = projmap.hashmap.iter().filter_map(|(k, v)| if let Projection::AggCol(agg) = *k { Some((agg, *v)) } else { None }).collect::<Vec<_>>();
        aggs.sort_by_key(|(_, colid)| *colid);
        aggs
    }
}
