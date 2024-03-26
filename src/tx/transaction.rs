use anyhow::Result;
use std::sync::{Arc, Mutex, Once};

use crate::{
	buffer::manager::BufferMgr,
	file::{block_id::BlockId, manager::FileMgr},
	log::manager::LogMgr,
};

use super::{
	bufferlist::BufferList,
	concurrency::{
		locktable::LockTable,
		manager::ConcurrencyMgr,
	},
	recovery::manager::RecoveryMgr,
};

// 参考元のだとMutexにしてないが，必要だと思うので追加
pub struct Transaction {
	next_tx_num: Arc<Mutex<u64>>,

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
		static mut SINGLETON: Option<Arc<Mutex<u64>>> = None;
		static ONCE: Once = Once::new();

		unsafe {
			ONCE.call_once(|| {
				let singleton = Arc::new(Mutex::new(1));
				SINGLETON = Some(singleton);
			});
			Self {
				next_tx_num: SINGLETON.clone().unwrap(),
				fm,
				lm,
				bm,
			}
		}
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

	pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
		panic!("TODO")
	}

	pub fn unpin(&mut self, blk: &BlockId) -> Result<()> {
		panic!("TODO")
	}

	pub fn get_i32(&mut self, blk: &BlockId, offset: i32) -> Result<()> {
		panic!("TODO")
	}

	pub fn get_string(&mut self, blk: &BlockId, offset: i32) -> Result<()> {
		panic!("TODO")
	}

	pub fn set_i32(
		&mut self,
		blk: &BlockId,
		offset: i32,
		val: i32,
		ok_to_log: bool,
	) -> Result<()> {
		panic!("TODO")
	}

	pub fn set_string(
		&mut self,
		blk: &BlockId,
		offset: i32,
		val: &str,
		ok_to_log: bool,
	) -> Result<()> {
		panic!("TODO")
	}

	pub fn size(&self, filename: &str) -> u64 {
		panic!("TODO")
	}

	pub fn append(&mut self, filename: &str) -> Result<BlockId> {
		panic!("TODO")
	}

	pub fn block_size(&self) -> u64 {
		panic!("TODO")
	}

	pub fn available_buffs(&self) -> Result<usize> {
		panic!("TODO")
	}

	fn next_tx_number(&mut self) -> Result<u64> {
		let mut next_tx_num = self.next_tx_num.lock().unwrap();
		*(next_tx_num) += 1;

		Ok(*next_tx_num)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use crate::{
		file::manager::FileMgr,
		buffer::manager::BufferMgr,
		log::manager::LogMgr,
	};

	#[test]
	fn test_next_tx_number_is_singleton() {
		let fm = Arc::new(Mutex::new(FileMgr::new("txtest/transactiontest", 200).unwrap()));
		let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testfile").unwrap()));
		let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 10)));
		// マルチスレッドでシングルトンであるかどうかが確認できていない
		let tx1 = Transaction::new(fm.clone(), lm.clone(), bm.clone());
		let tx2 = Transaction::new(fm.clone(), lm.clone(), bm.clone());
		assert!(Arc::ptr_eq(&tx1.next_tx_num, &tx2.next_tx_num));
	}
}
