use anyhow::Result;
use std::sync::{Arc, Mutex, Once};

use crate::{
	buffer::manager::BufferMgr,
	file::{block_id::BlockId, manager::FileMgr, page::PageSetter},
	log::manager::LogMgr,
};

use super::{
	bufferlist::BufferList,
	concurrency::{
		manager::ConcurrencyMgr,
		locktable::LockTableKey,
	},
	recovery::manager::RecoveryMgr,
};

// block_idをunsignedのままにしておきたいが，オーバーフローの検知とかができるi32のが良い？
static END_OF_FILE: u64 = std::u64::MAX;
// next_tx_num をTransactionのメンバ変数にしない
static mut NEXT_TX_NUM: Option<Arc<Mutex<i32>>> = None;
static ONCE: Once = Once::new();

// 参考元のだとMutexにしてないが，必要だと思うので追加
pub struct Transaction {
	recovery_mgr: Option<Arc<Mutex<RecoveryMgr>>>,
	concur_mgr: ConcurrencyMgr,
	fm: Arc<Mutex<FileMgr>>,
	lm: Arc<Mutex<LogMgr>>,
	bm: Arc<Mutex<BufferMgr>>,
	txnum: i32,
	mybuffers: BufferList,
}

impl Transaction {
	pub fn new(
		fm: Arc<Mutex<FileMgr>>,
		lm: Arc<Mutex<LogMgr>>,
		bm: Arc<Mutex<BufferMgr>>,
	) -> Self {

		unsafe {
			ONCE.call_once(|| {
				let singleton = Arc::new(Mutex::new(0));
				NEXT_TX_NUM = Some(singleton);
			});
			Self {
				recovery_mgr: None, // dummy
				concur_mgr: ConcurrencyMgr::new(),
				fm,
				lm,
				bm: bm.clone(),
				txnum: Self::next_tx_number(),
				mybuffers: BufferList::new(bm),
			}
		}
	}

	pub fn commit(&mut self) -> Result<()> {
		self.recovery_mgr
			.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.commit()?;
		self.concur_mgr.release()?;
		self.mybuffers.unpin_all()?;
		println!("transaction {} committed", self.txnum);

		Ok(())
	}

	pub fn rollback(&mut self) -> Result<()> {
		self.recovery_mgr
			.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.rollback()?;
		self.concur_mgr.release()?;
		self.mybuffers.unpin_all()?;
		println!("transaction {} rolled back", self.txnum);

		Ok(())
	}

	pub fn recover(&mut self) -> Result<()> {
		self.bm.lock().unwrap().flush_all(self.txnum)?;
		self.recovery_mgr
			.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.recover()
	}

	pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
		self.mybuffers.pin(blk)
	}

	pub fn unpin(&mut self, blk: &BlockId) -> Result<()> {
		self.mybuffers.unpin(blk)
	}

	pub fn get_i32(&mut self, blk: &BlockId, offset: i32) -> Result<i32> {
		self.concur_mgr.s_lock(&LockTableKey::BID(blk.clone()))?;
		let mut buff = self.mybuffers.get_buffer(blk).unwrap().lock().unwrap();
		buff.contents().get_i32(offset as usize)
	}

	pub fn get_string(&mut self, blk: &BlockId, offset: i32) -> Result<String> {
		self.concur_mgr.s_lock(&LockTableKey::BID(blk.clone()))?;
		let mut buff = self.mybuffers.get_buffer(blk).unwrap().lock().unwrap();
		buff.contents().get_string(offset as usize)
	}

	pub fn set_i32(
		&mut self,
		blk: &BlockId,
		offset: i32,
		val: i32,
		ok_to_log: bool,
	) -> Result<()> {
		self.concur_mgr.x_lock(&LockTableKey::BID(blk.clone()))?;
		let mut buff = self.mybuffers.get_buffer(blk).unwrap().lock().unwrap();
		let mut lsn: i32 = -1;
		if ok_to_log {
			let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
			lsn = rm.set_i32(&mut buff, offset, val)?.try_into().unwrap();
		}
		let p = buff.contents();
		p.set(offset as usize, val)?;
		buff.set_modified(self.txnum, lsn);

		Ok(())
	}

	pub fn set_string(
		&mut self,
		blk: &BlockId,
		offset: i32,
		val: &str,
		ok_to_log: bool,
	) -> Result<()> {
		self.concur_mgr.x_lock(&LockTableKey::BID(blk.clone()))?;
		let mut buff = self.mybuffers.get_buffer(blk).unwrap().lock().unwrap();
		let mut lsn: i32 = -1;
		if ok_to_log {
			let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
			lsn = rm.set_string(&mut buff, offset, val)?.try_into().unwrap();
		}
		let p = buff.contents();
		p.set(offset as usize, val.to_string())?;
		buff.set_modified(self.txnum, lsn);

		Ok(())
	}

	pub fn size(&mut self, filename: &str) -> Result<u64> {
		self.concur_mgr.s_lock(&LockTableKey::DUMMY(END_OF_FILE))?;
		self.fm.lock().unwrap().length(filename)
	}

	pub fn append(&mut self, filename: &str) -> Result<BlockId> {
		self.concur_mgr.x_lock(&LockTableKey::DUMMY(END_OF_FILE))?;
		self.fm.lock().unwrap().append(filename)
	}

	pub fn block_size(&self) -> u64 {
		self.fm.lock().unwrap().blocksize()
	}

	pub fn available_buffs(&self) -> Result<usize> {
		self.bm.lock().unwrap().available()
	}

	fn next_tx_number() -> i32 {
		// next_tx_num をTransactionのメンバ変数にしないため，引数にselfを用いない
		unsafe {
			let next_tx_num_tmp = NEXT_TX_NUM.clone().unwrap();
			let mut next_tx_num = next_tx_num_tmp.lock().unwrap();
			*(next_tx_num) += 1;

			*next_tx_num
		}
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
		unsafe {
			let _ = Transaction::new(fm.clone(), lm.clone(), bm.clone());
			let p1 = NEXT_TX_NUM.clone().unwrap();
			let _ = Transaction::new(fm.clone(), lm.clone(), bm.clone());
			let p2 = NEXT_TX_NUM.clone().unwrap();
			assert!(Arc::ptr_eq(&p1, &p2));
		}
	}

	#[test]
	fn test_txnum_is_increment() {
		let fm = Arc::new(Mutex::new(FileMgr::new("txtest/transactiontest", 200).unwrap()));
		let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testfile").unwrap()));
		let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 10)));

		let tx_base = Transaction::new(fm.clone(), lm.clone(), bm.clone());
		let base = tx_base.txnum;
		for i in 1..11 {
			let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone());
			assert_eq!(tx.txnum, i + base);
		}
	}
}
