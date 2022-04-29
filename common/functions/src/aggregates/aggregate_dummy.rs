use std::alloc::Layout;
use std::fmt::{Display, Formatter};
use bytes::BytesMut;
use num_format::Locale::se;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::{ColumnRef, DataTypeImpl, MutableColumn};
use crate::aggregates::{AggregateFunction, StateAddr};

pub struct DummyAggregateFunction {
    data_type: DataTypeImpl
}

impl Display for DummyAggregateFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateFunction for DummyAggregateFunction {
    fn name(&self) -> &str {
        "aggregate_function_dummy"
    }

    fn return_type(&self) -> common_exception::Result<DataTypeImpl> {
        Ok(self.data_type.clone())
    }

    fn init_state(&self, place: StateAddr) {

    }

    fn state_layout(&self) -> Layout {
        todo!()
    }

    fn accumulate(&self, _place: StateAddr, _columns: &[ColumnRef], _validity: Option<&Bitmap>, _input_rows: usize) -> common_exception::Result<()> {
        todo!()
    }

    fn accumulate_row(&self, _place: StateAddr, _columns: &[ColumnRef], _row: usize) -> common_exception::Result<()> {
        todo!()
    }

    fn serialize(&self, _place: StateAddr, _writer: &mut BytesMut) -> common_exception::Result<()> {
        todo!()
    }

    fn deserialize(&self, _place: StateAddr, _reader: &mut &[u8]) -> common_exception::Result<()> {
        todo!()
    }

    fn merge(&self, _place: StateAddr, _rhs: StateAddr) -> common_exception::Result<()> {
        todo!()
    }

    fn merge_result(&self, _place: StateAddr, array: &mut dyn MutableColumn) -> common_exception::Result<()> {
        todo!()
    }
}