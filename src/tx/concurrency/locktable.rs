use anyhow::Result;
use std::collections::HashMap;

use crate::file::block_id::BlockId;

const MAX_TIME: i64 = 10_000;

pub struct LockTable {
	locks: HashMap<BlockId, i32>,
}

impl LockTable {
	pub fn s_lock(&self, blk: &BlockId) -> Result<()> {
		panic!("TODO")
	}
	pub fn x_lock(&self, blk: &BlockId) -> Result<()> {
		panic!("TODO")
	}
	pub fn unlock(&self, blk: &BlockId) -> Result<()> {
		panic!("TODO")
	}
	fn has_x_lock(&self, blk: &BlockId) -> Result<bool> {
		panic!("TODO")
	}
	fn has_other_slocks(&self, blk: &BlockId) -> Result<bool> {
		panic!("TODO")
	}
	fn waiting_too_long(starttime: u64) -> bool {
		panic!("TODO")
	}
	fn get_lock_val(&self, blk: &BlockId) -> Result<bool> {
		panic!("TODO")
	}
}
