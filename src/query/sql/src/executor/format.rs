// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_ast::ast::FormatTreeNode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::FunctionContext;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use super::AggregateFinal;
use super::AggregateFunctionDesc;
use super::AggregatePartial;
use super::EvalScalar;
use super::Exchange;
use super::Filter;
use super::HashJoin;
use super::Limit;
use super::PhysicalPlan;
use super::Project;
use super::Sort;
use super::TableScan;
use super::UnionAll;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::FragmentKind;
use crate::planner::MetadataRef;
use crate::planner::DUMMY_TABLE_INDEX;
use crate::ColumnEntry;

impl PhysicalPlan {
    pub fn format(&self, metadata: MetadataRef) -> Result<String> {
        to_format_tree(self, &metadata)?.format_pretty()
    }
}

fn to_format_tree(plan: &PhysicalPlan, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    match plan {
        PhysicalPlan::TableScan(plan) => table_scan_to_format_tree(plan, metadata),
        PhysicalPlan::Filter(plan) => filter_to_format_tree(plan, metadata),
        PhysicalPlan::Project(plan) => project_to_format_tree(plan, metadata),
        PhysicalPlan::EvalScalar(plan) => eval_scalar_to_format_tree(plan, metadata),
        PhysicalPlan::AggregatePartial(plan) => aggregate_partial_to_format_tree(plan, metadata),
        PhysicalPlan::AggregateFinal(plan) => aggregate_final_to_format_tree(plan, metadata),
        PhysicalPlan::Sort(plan) => sort_to_format_tree(plan, metadata),
        PhysicalPlan::Limit(plan) => limit_to_format_tree(plan, metadata),
        PhysicalPlan::HashJoin(plan) => hash_join_to_format_tree(plan, metadata),
        PhysicalPlan::Exchange(plan) => exchange_to_format_tree(plan, metadata),
        PhysicalPlan::UnionAll(plan) => union_all_to_format_tree(plan, metadata),
        PhysicalPlan::ExchangeSource(_)
        | PhysicalPlan::ExchangeSink(_)
        | PhysicalPlan::DistributedInsertSelect(_) => {
            Err(ErrorCode::Internal("Invalid physical plan"))
        }
    }
}

fn table_scan_to_format_tree(
    plan: &TableScan,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    if plan.table_index == DUMMY_TABLE_INDEX {
        return Ok(FormatTreeNode::new("DummyTableScan".to_string()));
    }
    let table = metadata.read().table(plan.table_index).clone();
    let table_name = format!("{}.{}.{}", table.catalog(), table.database(), table.name());
    let filters = plan
        .source
        .push_downs
        .as_ref()
        .map_or("".to_string(), |extras| {
            extras
                .filters
                .iter()
                .map(|f| {
                    let expr = f.as_expr(&BUILTIN_FUNCTIONS).unwrap();
                    let (new_expr, _) =
                        ConstantFolder::fold(&expr, FunctionContext::default(), &BUILTIN_FUNCTIONS);
                    new_expr.sql_display()
                })
                .collect::<Vec<_>>()
                .join(", ")
        });

    let limit = plan
        .source
        .push_downs
        .as_ref()
        .map_or("NONE".to_string(), |extras| {
            extras
                .limit
                .map_or("NONE".to_string(), |limit| limit.to_string())
        });

    let mut children = vec![
        FormatTreeNode::new(format!("table: {table_name}")),
        FormatTreeNode::new(format!("read rows: {}", plan.source.statistics.read_rows)),
        FormatTreeNode::new(format!("read bytes: {}", plan.source.statistics.read_bytes)),
        FormatTreeNode::new(format!(
            "partitions total: {}",
            plan.source.statistics.partitions_total
        )),
        FormatTreeNode::new(format!(
            "partitions scanned: {}",
            plan.source.statistics.partitions_scanned
        )),
        FormatTreeNode::new(format!(
            "push downs: [filters: [{filters}], limit: {limit}]"
        )),
    ];

    let mut output_columns: Vec<usize> = Vec::new();
    if let Some(scan_fields) = &plan.source.scan_fields {
        output_columns = scan_fields.keys().cloned().collect();
    };

    // If output_columns is empty, it indicates that scan all fields.
    // Then output_columns won't show in explain
    if !output_columns.is_empty() {
        children.push(FormatTreeNode::new(format!(
            "output columns: [{}]",
            output_columns.iter().join(", ")
        )));
    }

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    Ok(FormatTreeNode::with_children(
        "TableScan".to_string(),
        children,
    ))
}

fn filter_to_format_tree(plan: &Filter, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    let filter = plan
        .predicates
        .iter()
        .map(|scalar| scalar.pretty_display())
        .collect::<Vec<_>>()
        .join(", ");
    let mut children = vec![FormatTreeNode::new(format!("filters: [{filter}]"))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children(
        "Filter".to_string(),
        children,
    ))
}

fn project_to_format_tree(
    plan: &Project,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let columns = plan
        .columns
        .iter()
        .sorted()
        .map(|column| {
            format!(
                "{} (#{})",
                match metadata.read().column(*column) {
                    ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                    ColumnEntry::DerivedColumn { alias, .. } => alias,
                },
                column
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut children = vec![FormatTreeNode::new(format!("columns: [{columns}]"))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children(
        "Project".to_string(),
        children,
    ))
}

fn eval_scalar_to_format_tree(
    plan: &EvalScalar,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let scalars = plan
        .scalars
        .iter()
        .map(|(scalar, _)| scalar.pretty_display())
        .collect::<Vec<_>>()
        .join(", ");
    let mut children = vec![FormatTreeNode::new(format!("expressions: [{scalars}]"))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children(
        "EvalScalar".to_string(),
        children,
    ))
}

pub fn pretty_display_agg_desc(desc: &AggregateFunctionDesc, metadata: &MetadataRef) -> String {
    format!(
        "{}({})",
        desc.sig.name,
        desc.arg_indices
            .iter()
            .map(|&index| {
                let column = metadata.read().column(index).clone();
                match column {
                    ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                    ColumnEntry::DerivedColumn { alias, .. } => alias,
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn aggregate_partial_to_format_tree(
    plan: &AggregatePartial,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let group_by = plan
        .group_by
        .iter()
        .map(|column| {
            let column = metadata.read().column(*column).clone();
            let name = match column {
                ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                ColumnEntry::DerivedColumn { alias, .. } => alias,
            };
            Ok(name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let agg_funcs = plan
        .agg_funcs
        .iter()
        .map(|agg| pretty_display_agg_desc(agg, metadata))
        .collect::<Vec<_>>()
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!("group by: [{group_by}]")),
        FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children(
        "AggregatePartial".to_string(),
        children,
    ))
}

fn aggregate_final_to_format_tree(
    plan: &AggregateFinal,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let group_by = plan
        .group_by
        .iter()
        .map(|column| {
            let column = metadata.read().column(*column).clone();
            let name = match column {
                ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                ColumnEntry::DerivedColumn { alias, .. } => alias,
            };
            Ok(name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let agg_funcs = plan
        .agg_funcs
        .iter()
        .map(|agg| pretty_display_agg_desc(agg, metadata))
        .collect::<Vec<_>>()
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!("group by: [{group_by}]")),
        FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children(
        "AggregateFinal".to_string(),
        children,
    ))
}

fn sort_to_format_tree(plan: &Sort, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    let sort_keys = plan
        .order_by
        .iter()
        .map(|sort_key| {
            let index = sort_key.order_by;
            let column = metadata.read().column(index).clone();
            Ok(format!(
                "{} {} {}",
                match column {
                    ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                    ColumnEntry::DerivedColumn { alias, .. } => alias,
                },
                if sort_key.asc { "ASC" } else { "DESC" },
                if sort_key.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                }
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let mut children = vec![FormatTreeNode::new(format!("sort keys: [{sort_keys}]"))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children("Sort".to_string(), children))
}

fn limit_to_format_tree(plan: &Limit, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    let mut children = vec![
        FormatTreeNode::new(format!(
            "limit: {}",
            plan.limit
                .map_or("NONE".to_string(), |limit| limit.to_string())
        )),
        FormatTreeNode::new(format!("offset: {}", plan.offset)),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(to_format_tree(&plan.input, metadata)?);

    Ok(FormatTreeNode::with_children("Limit".to_string(), children))
}

fn hash_join_to_format_tree(
    plan: &HashJoin,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let build_keys = plan
        .build_keys
        .iter()
        .map(|scalar| scalar.pretty_display())
        .collect::<Vec<_>>()
        .join(", ");
    let probe_keys = plan
        .probe_keys
        .iter()
        .map(|scalar| scalar.pretty_display())
        .collect::<Vec<_>>()
        .join(", ");
    let filters = plan
        .non_equi_conditions
        .iter()
        .map(|filter| filter.pretty_display())
        .collect::<Vec<_>>()
        .join(", ");

    let mut build_child = to_format_tree(&plan.build, metadata)?;
    let mut probe_child = to_format_tree(&plan.probe, metadata)?;

    build_child.payload = format!("{}(Build)", build_child.payload);
    probe_child.payload = format!("{}(Probe)", probe_child.payload);

    let mut children = vec![
        FormatTreeNode::new(format!("join type: {}", plan.join_type)),
        FormatTreeNode::new(format!("build keys: [{build_keys}]")),
        FormatTreeNode::new(format!("probe keys: [{probe_keys}]")),
        FormatTreeNode::new(format!("filters: [{filters}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.push(build_child);
    children.push(probe_child);

    Ok(FormatTreeNode::with_children(
        "HashJoin".to_string(),
        children,
    ))
}

fn exchange_to_format_tree(
    plan: &Exchange,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::with_children("Exchange".to_string(), vec![
        FormatTreeNode::new(format!("exchange type: {}", match plan.kind {
            FragmentKind::Init => "Init-Partition".to_string(),
            FragmentKind::Normal => format!(
                "Hash({})",
                plan.keys
                    .iter()
                    .map(|scalar| { scalar.pretty_display() })
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            FragmentKind::Expansive => "Broadcast".to_string(),
            FragmentKind::Merge => "Merge".to_string(),
        })),
        to_format_tree(&plan.input, metadata)?,
    ]))
}

fn union_all_to_format_tree(
    plan: &UnionAll,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    children.extend(vec![
        to_format_tree(&plan.left, metadata)?,
        to_format_tree(&plan.right, metadata)?,
    ]);

    Ok(FormatTreeNode::with_children(
        "UnionAll".to_string(),
        children,
    ))
}

fn plan_stats_info_to_format_tree(info: &PlanStatsInfo) -> Vec<FormatTreeNode<String>> {
    vec![FormatTreeNode::new(format!(
        "estimated rows: {0:.2}",
        info.estimated_rows
    ))]
}
