// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::arrow::constant_bitmap;
use common_expression::converts::scalar_to_datavalue;
use common_expression::types::DataType;
use common_expression::types::NullableType;
use common_expression::types::Number;
use common_expression::types::NumberDataType;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::Domain;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Literal;
use common_expression::Scalar;
use common_expression::Span;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_sql::executor::PhysicalScalar;
use storages_common_table_meta::meta::V2BloomBlock;
use storages_common_table_meta::meta::Versioned;

use crate::filters::Filter;
use crate::filters::FilterBuilder;
use crate::filters::Xor8Builder;
use crate::filters::Xor8Filter;
use crate::SupportedType;

/// BlockFilter represents multiple per-column filters(bloom filter or xor filter etc) for data block.
///
/// By default we create a filter per column for a parquet data file. For columns whose data_type
/// are not applicable for a filter, we skip the creation.
/// That is to say, it is legal to have a BlockFilter with zero columns.
///
/// For example, for the source data block as follows:
/// ```
///         +---name--+--age--+
///         | "Alice" |  20   |
///         | "Bob"   |  30   |
///         +---------+-------+
/// ```
/// We will create table of filters as follows:
/// ```
///         +---Bloom(name)--+--Bloom(age)--+
///         |  123456789abcd |  ac2345bcd   |
///         +----------------+--------------+
/// ```
pub struct BlockFilter {
    pub func_ctx: FunctionContext,

    /// The schema of the source table, which the filter work for.
    pub source_schema: TableSchemaRef,

    /// The schema of the filter block.
    ///
    /// It is a sub set of `source_schema`.
    pub filter_schema: TableSchemaRef,

    pub version: u64,
    /// Data block of filters.
    pub filter_block: DataBlock,

    /// Approximate distinct count of columns generated by xor hash function.
    pub column_distinct_count: HashMap<usize, usize>,
}

/// FilterExprEvalResult represents the evaluation result of an expression by a filter.
///
/// For example, expression of 'age = 12' should return false is the filter are sure
/// of the nonexistent of value '12' in column 'age'. Otherwise should return 'Maybe'.
///
/// If the column is not applicable for a filter, like TypeID::struct, Uncertain is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterEvalResult {
    MustFalse,
    Uncertain,
}

impl BlockFilter {
    /// Load a filter directly from the source table's schema and the corresponding filter parquet file.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn from_filter_block(
        func_ctx: FunctionContext,
        source_schema: TableSchemaRef,
        filter_schema: TableSchemaRef,
        filter_block: DataBlock,
        version: u64,
    ) -> Result<Self> {
        Ok(Self {
            version,
            func_ctx,
            source_schema,
            filter_schema,
            filter_block,
            column_distinct_count: HashMap::new(),
        })
    }

    /// Create a filter block from source data.
    ///
    /// All input blocks should belong to a Parquet file, e.g. the block array represents the parquet file in memory.
    pub fn try_create(
        func_ctx: FunctionContext,
        source_schema: TableSchemaRef,
        version: u64,
        blocks: &[&DataBlock],
    ) -> Result<Self> {
        if blocks.is_empty() {
            return Err(ErrorCode::BadArguments("block is empty"));
        }

        let mut exprs = Vec::new();
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for i in 0..blocks[0].num_columns() {
            let data_type = &blocks[0].get_by_offset(i).data_type;
            if Xor8Filter::is_supported_type(data_type) {
                let source_field = source_schema.field(i);
                let return_type = if data_type.is_nullable() {
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)))
                } else {
                    DataType::Number(NumberDataType::UInt64)
                };
                let field = DataField::new(source_field.name().as_str(), return_type.clone());
                fields.push(field);

                let arg = PhysicalScalar::IndexedVariable {
                    index: columns.len(),
                    data_type: data_type.clone(),
                    display_name: source_field.name().clone(),
                };
                let scalar = PhysicalScalar::Function {
                    name: "siphash".to_string(),
                    params: vec![],
                    args: vec![arg],
                    return_type,
                };
                let expr = scalar.as_expr()?;
                exprs.push(expr);

                let source_columns = blocks
                    .iter()
                    .map(|block| {
                        let value = &block.get_by_offset(i).value;
                        value.convert_to_full_column(data_type, block.num_rows())
                    })
                    .collect::<Vec<_>>();
                let column = Column::concat(&source_columns);
                columns.push(column);
            }
        }

        let mut filter_fields = vec![];
        let mut filter_columns = vec![];
        let mut column_distinct_count = HashMap::<usize, usize>::new();

        let data_block = DataBlock::new_from_columns(columns);
        let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);
        for (field, expr) in fields.iter().zip(exprs.iter()) {
            let value = evaluator
                .run(expr)
                .map_err(|(_, e)| ErrorCode::Internal(format!("eval siphash failed: {}.", e)))?;
            let col = value.convert_to_full_column(field.data_type(), data_block.num_rows());
            let (column, validity) = if field.data_type().is_nullable() {
                let nullable_column =
                    NullableType::<UInt64Type>::try_downcast_column(&col).unwrap();
                (nullable_column.column, nullable_column.validity)
            } else {
                let column = UInt64Type::try_downcast_column(&col).unwrap();
                let validity = constant_bitmap(true, column.len()).into();
                (column, validity)
            };

            // create filter per column
            let mut filter_builder = Xor8Builder::create();
            let filter = if validity.unset_bits() > 0 {
                let mut digests = Vec::with_capacity(column.len());
                UInt64Type::iter_column(&column)
                    .zip(validity.iter())
                    .for_each(|(v, b)| {
                        if !b {
                            digests.push(0);
                        } else {
                            digests.push(v);
                        }
                    });
                filter_builder.build_from_digests(&digests)?
            } else {
                filter_builder.build_from_digests(column.deref())?
            };

            if let Some(len) = filter.len() {
                let idx = source_schema.index_of(field.name().as_str()).unwrap();
                column_distinct_count.insert(idx, len);
            }

            let filter_name = Self::build_filter_column_name(field.name());
            filter_fields.push(TableField::new(&filter_name, TableDataType::String));
            // create filter column
            let serialized_bytes = filter.to_bytes()?;
            let filter_value = Value::Scalar(Scalar::String(serialized_bytes));
            filter_columns.push(BlockEntry {
                data_type: DataType::String,
                value: filter_value,
            });
        }

        let filter_schema = Arc::new(TableSchema::new(filter_fields));
        let filter_block = DataBlock::new(filter_columns, 1);

        Ok(Self {
            func_ctx,
            version,
            source_schema,
            filter_schema,
            filter_block,
            column_distinct_count,
        })
    }

    /// Apply the predicate expression, return the result.
    /// If we are sure of skipping the scan, return false, e.g. the expression must be false.
    /// This happens when the data doesn't show up in the filter.
    ///
    /// Otherwise return `Uncertain`.
    #[tracing::instrument(level = "debug", name = "block_filter_index_eval", skip_all)]
    pub fn eval(&self, mut expr: Expr<String>) -> Result<FilterEvalResult> {
        visit_expr_column_eq_constant(
            &mut expr,
            &mut |span, col_name, scalar, ty, return_type| {
                // If the column doesn't contain the constant, we rewrite the expression to `false`.
                if self.find(col_name, scalar, ty)? == FilterEvalResult::MustFalse {
                    Ok(Some(Expr::Constant {
                        span,
                        scalar: Scalar::Boolean(false),
                        data_type: return_type.clone(),
                    }))
                } else {
                    Ok(None)
                }
            },
        )?;

        let input_domains = expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                let domain = Domain::full(&ty);
                (name, domain)
            })
            .collect();

        let folder = ConstantFolder::new(input_domains, self.func_ctx, &BUILTIN_FUNCTIONS);
        let (new_expr, _) = folder.fold(&expr);

        match new_expr {
            Expr::Constant {
                scalar: Scalar::Boolean(false),
                ..
            } => Ok(FilterEvalResult::MustFalse),
            _ => Ok(FilterEvalResult::Uncertain),
        }
    }

    /// Find all columns that match the pattern of `col = <constant>` in the expression.
    pub fn find_eq_columns(expr: &Expr<String>) -> Result<Vec<String>> {
        let mut cols = Vec::new();
        visit_expr_column_eq_constant(&mut expr.clone(), &mut |_, col_name, _, _, _| {
            cols.push(col_name.to_string());
            Ok(None)
        })?;
        Ok(cols)
    }

    /// For every applicable column, we will create a filter.
    /// The filter will be stored with field name 'Bloom(column_name)'
    pub fn build_filter_column_name(column_name: &str) -> String {
        format!("Bloom({})", column_name)
    }

    fn find(&self, column_name: &str, target: &Scalar, ty: &DataType) -> Result<FilterEvalResult> {
        let filter_column = &Self::build_filter_column_name(column_name);

        if !self.filter_schema.has_field(filter_column)
            || !Xor8Filter::is_supported_type(ty)
            || target.is_null()
        {
            // The column doesn't have a filter.
            return Ok(FilterEvalResult::Uncertain);
        }
        let filter_bytes = match &self
            .filter_block
            .get_by_offset(self.filter_schema.index_of(filter_column)?)
            .value
        {
            Value::Scalar(s) => s.as_string().unwrap(),
            Value::Column(c) => unsafe { c.as_string().unwrap().index_unchecked(0) },
        };

        let (filter, _size) = Xor8Filter::from_bytes(filter_bytes)?;

        let contains = if self.version == V2BloomBlock::VERSION {
            let datavalue = scalar_to_datavalue(target);
            filter.contains(&datavalue)
        } else {
            let digest = if let Scalar::Null = target {
                0u64
            } else {
                let arg = PhysicalScalar::Constant {
                    value: Literal::try_from(target.clone()).unwrap(),
                    data_type: ty.clone(),
                };
                let scalar = PhysicalScalar::Function {
                    name: "siphash".to_string(),
                    params: vec![],
                    args: vec![arg],
                    return_type: DataType::Number(NumberDataType::UInt64),
                };
                let expr = scalar.as_expr()?;

                let data_block = DataBlock::empty();
                let evaluator = Evaluator::new(&data_block, self.func_ctx, &BUILTIN_FUNCTIONS);
                let value = evaluator.run(&expr).map_err(|(_, e)| {
                    ErrorCode::Internal(format!("eval siphash failed: {}.", e))
                })?;
                let number_scalar = value.into_scalar().unwrap().into_number().unwrap();
                u64::try_downcast_scalar(&number_scalar).unwrap()
            };
            filter.contains_digest(digest)
        };

        if contains {
            Ok(FilterEvalResult::Uncertain)
        } else {
            Ok(FilterEvalResult::MustFalse)
        }
    }
}

fn visit_expr_column_eq_constant(
    expr: &mut Expr<String>,
    visitor: &mut impl FnMut(Span, &str, &Scalar, &DataType, &DataType) -> Result<Option<Expr<String>>>,
) -> Result<()> {
    // Find patterns like `Column = <constant>` or `<constant> = Column`.
    match expr {
        Expr::FunctionCall {
            span,
            function,
            args,
            return_type,
            ..
        } if function.signature.name == "eq" => match args.as_slice() {
            [
                Expr::ColumnRef { id, data_type, .. },
                Expr::Constant { scalar, .. },
            ]
            | [
                Expr::Constant { scalar, .. },
                Expr::ColumnRef { id, data_type, .. },
            ] => {
                // If the visitor returns a new expression, then replace with the current expression.
                if let Some(new_expr) = visitor(span.clone(), id, scalar, data_type, return_type)? {
                    *expr = new_expr;
                    return Ok(());
                }
            }
            _ => (),
        },
        _ => (),
    }

    // Otherwise, rewrite sub expressions.
    match expr {
        Expr::Cast { expr, .. } => {
            visit_expr_column_eq_constant(expr, visitor)?;
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                visit_expr_column_eq_constant(arg, visitor)?;
            }
        }
        _ => (),
    }

    Ok(())
}
