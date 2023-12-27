use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
	buffer::manager::BufferMgr,
	file::{block_id::BlockId, manager::FileMgr},
	log::manager::LogMgr,
};

// 参考元のだとMutexにしてないが，必要だと思うので追加
pub struct Transaction {
	fm: Arc<Mutex<FileMgr>>,
	lm: Arc<Mutex<LogMgr>>,
	bm: Arc<Mutex<BufferMgr>>,
}

impl Transaction {
	pub fn new(
		fm: Arc<Mutex<FileMgr>>,
		lm: Arc<Mutex<LogMgr>>,
		bm: Arc<Mutex<BufferMgr>>,
	) -> Self {
		Self { fm, lm, bm }
	}

	pub fn commit(&mut self) -> Result<()> {
		panic!("TODO")
	}

	pub fn rollback(&mut self) -> Result<()> {
		panic!("TODO")
	}

	pub fn recover(&mut self) -> Result<()> {
		panic!("TODO")
	}

	pub fn pin(&mut self, blk: BlockId) -> Result<()> {
		panic!("TODO")
	}

	pub fn unpin(&mut self, blk: BlockId) -> Result<()> {
		panic!("TODO")
	}

	pub fn get_i32(&mut self, blk: BlockId, offset: usize) -> Result<()> {
		panic!("TODO")
	}

	pub fn get_string(&mut self, blk: BlockId, offset: usize) -> Result<()> {
		panic!("TODO")
	}

	pub fn set_i32(
		&mut self,
		blk: BlockId,
		offset: usize,
		val: i32,
		ok_to_log: bool,
	) -> Result<()> {
		panic!("TODO")
	}

	pub fn set_string(
		&mut self,
		blk: BlockId,
		offset: usize,
		val: String,
		ok_to_log: bool,
	) -> Result<()> {
		panic!("TODO")
	}

	pub fn size(&self, filename: String) -> u64 {
		panic!("TODO")
	}

	pub fn append(&mut self, filename: String) -> Result<BlockId> {
		panic!("TODO")
	}

	pub fn block_size(&self) -> u64 {
		panic!("TODO")
	}

	pub fn available_buffs(&self) -> Result<usize> {
		panic!("TODO")
	}

	fn next_tx_number(&mut self) -> Result<i32> {
		panic!("TODO")
	}
}
