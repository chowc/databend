// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::RawEntryMut;
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::hash::Hasher;

use ahash::RandomState;
use common_datavalues::series::Series;
use common_datavalues::AlignedVec;
use common_datavalues::DataValue;
use common_datavalues::UInt64Type;
use common_exception::Result;

use crate::DataBlock;

// Table for <group_key, (indices, keys) >
pub type GroupIndicesTable = HashMap<IdxHash, Vec<u32>, ahash::RandomState>;
// Table for <(group_key, keys, block)>
type GroupBlocksTable = Vec<(IdxHash, DataBlock)>;

impl DataBlock {
    /// Hash group based on row index then return indices and keys.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1)
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///
    pub fn group_by_get_indices(
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndicesTable> {
        let mut group_indices = GroupIndicesTable::default();
        let mut hashes = Vec::with_capacity(column_names.len());
        let mut values = Vec::with_capacity(column_names.len());

        let mut hash_builder = RandomState::new();
        // 1. Get group by columns.
        let mut group_series = Vec::with_capacity(column_names.len());
        {
            for col in column_names {
                let column = block.try_column_by_name(&col)?;

                let series = column.to_array()?;
                let hash = series.vec_hash(hash_builder.clone());

                group_series.push(series);
                values.push(hash.downcast_ref().values());
                hashes.push(hash);
            }
        }

        // 2. Build vec hashes
        let mut av = AlignedVec::with_capacity_aligned(block.num_rows());
        unsafe {
            for row_i in 0..av.len() {
                let mut h = *values.get_unchecked(0).get_unchecked(row_i);
                for column_i in 1..hashes.len() {
                    let h_ = values.get_unchecked(column_i).get_unchecked(row_i);
                    h = boost_hash_combine(h, *h_);
                }
                av.push(h);
            }
        }
        let keys = av.into_primitive_array::<UInt64Type>(None);

        // 3. Make group with indices.
        {
            let mut row = 0;
            for hash in keys.values() {
                // TODO improve the conflicts
                let hash_key = IdxHash::new(row, *hash);
                let entry = group_indices
                    .raw_entry_mut()
                    .from_hash(*hash, |idx_hash| unsafe {
                        unsafe {
                            compare_series_row(&group_series, idx_hash.idx as usize, row as usize)
                        }
                    });

                match entry {
                    RawEntryMut::Occupied(entry) => {
                        let v = entry.get_mut();
                        v.push(row as u32);
                    }
                    RawEntryMut::Vacant(entry) => {
                        entry.insert_hashed_nocheck(*hash, IdxHash::new(row, *hash), vec![
                            row as u32,
                        ]);
                    }
                }

                row += 1;
            }
        }

        Ok(group_indices)
    }

    /// Hash group based on row index by column names.
    ///
    /// group_by_get_indices and make blocks.
    pub fn group_by(block: &DataBlock, column_names: &[String]) -> Result<GroupBlocksTable> {
        let group_indices = Self::group_by_get_indices(block, column_names)?;

        let mut group_blocks = GroupBlocksTable::default();
        for (group_key, group_indices) in group_indices {
            let take_block = DataBlock::block_take_by_indices(&block, &group_indices)?;
            group_blocks.push((group_key, take_block));
        }

        Ok(group_blocks)
    }
}

#[inline]
pub unsafe fn compare_series_row(group_series: &[Series], idx_a: usize, idx_b: usize) -> bool {
    for s in group_series {
        if !s.equal_element(idx_a, idx_b, s) {
            return false;
        }
    }
    true
}

// hash combine from c++' boost lib
fn boost_hash_combine(l: u64, r: u64) -> u64 {
    l ^ r.wrapping_add(0x9e3779b9u64.wrapping_add(l << 6).wrapping_add(r >> 2))
}

// Used to to get a u64 from the hashing keys
pub trait AsU64 {
    #[allow(clippy::wrong_self_convention)]
    fn as_u64(self) -> u64;
}

impl AsU64 for u32 {
    fn as_u64(self) -> u64 {
        self as u64
    }
}

impl AsU64 for u64 {
    fn as_u64(self) -> u64 {
        self
    }
}

impl AsU64 for Option<u32> {
    fn as_u64(self) -> u64 {
        match self {
            Some(v) => v as u64,
            // just a number
            None => u64::MAX,
        }
    }
}

impl AsU64 for Option<u64> {
    fn as_u64(self) -> u64 {
        self.unwrap_or(u64::MAX)
    }
}

impl AsU64 for [u8; 9] {
    fn as_u64(self) -> u64 {
        // the last byte includes the null information.
        // that one is skipped. Worst thing that could happen is unbalanced partition.
        u64::from_ne_bytes(self[..8].try_into().unwrap())
    }
}

pub struct IdHasher {
    hash: u64,
}

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdHasher should only be used for integer keys <= 64 bit precision")
    }

    fn write_u32(&mut self, i: u32) {
        self.write_u64(i as u64)
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }

    fn write_i32(&mut self, i: i32) {
        // Safety:
        // same number of bits
        unsafe { self.write_u32(std::mem::transmute::<i32, u32>(i)) }
    }

    fn write_i64(&mut self, i: i64) {
        // Safety:
        // same number of bits
        unsafe { self.write_u64(std::mem::transmute::<i64, u64>(i)) }
    }
}

impl Default for IdHasher {
    fn default() -> Self {
        IdHasher { hash: 0 }
    }
}

pub type IdBuildHasher = BuildHasherDefault<IdHasher>;

#[derive(Debug)]
/// Contains an idx of a row in a DataFrame and the precomputed hash of that row.
/// That hash still needs to be used to create another hash to be able to resize hashmaps without
/// accidental quadratic behavior. So do not use an Identity function!
pub struct IdxHash {
    // idx in row of Series, DataFrame
    pub idx: u32,
    // precomputed hash of T
    hash: u64,
}

impl Hash for IdxHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash)
    }
}

impl IdxHash {
    #[inline]
    pub fn new(idx: u32, hash: u64) -> Self {
        IdxHash { idx, hash }
    }
}

/// Contains a ptr to the string slice an the precomputed hash of that string.
/// During rehashes, we will rehash the hash instead of the string, that makes rehashing
/// cheap and allows cache coherent small hash tables.
#[derive(Eq, Copy, Clone)]
pub struct StrHash<'a> {
    str: Option<&'a str>,
    hash: u64,
}

impl<'a> Hash for StrHash<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash)
    }
}

impl<'a> StrHash<'a> {
    pub fn new(s: Option<&'a str>, hash: u64) -> Self {
        Self { str: s, hash }
    }
}

impl<'a> PartialEq for StrHash<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.str == other.str
    }
}

impl<'a> AsU64 for StrHash<'a> {
    fn as_u64(self) -> u64 {
        self.hash
    }
}
