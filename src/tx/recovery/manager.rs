use anyhow::Result;
use core::fmt;
use std::{
	sync::{Arc, Mutex},
};

use crate::{
	buffer::{buffer::Buffer, manager::BufferMgr},
	log::manager::LogMgr,
	tx::transaction::Transaction,
};

use super::logrecord::{
	create_log_record,
	CheckpointRecord,
	CommitRecord,
	RollbackRecord,
	StartRecord,
	SetI32Record,
	SetStringRecord,
	AbstractDataRecord,
	TxType,
};

#[derive(Debug)]
enum RecoveryMgrError {
	BufferFailed(String),
}

impl std::error::Error for RecoveryMgrError {}
impl fmt::Display for RecoveryMgrError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::BufferFailed(s) => {
				write!(f, "buffer failed: {}", s)
			}
		}
	}
}

macro_rules! lock {
	($self:ident, $wtl:expr) => ({
		let mut lm = $self.lm.lock().unwrap();
		let mut bm = $self.bm.lock().unwrap();

		bm.flush_all($self.txnum)?;
		let lsn = $wtl;
		lm.flush(lsn)
	})
}

pub struct RecoveryMgr {
	lm: Arc<Mutex<LogMgr>>,
	bm: Arc<Mutex<BufferMgr>>,
	tx: Transaction,
	txnum: i32,
}

impl RecoveryMgr {
	pub fn new(
		tx: Transaction,
		txnum: i32,
		lm: Arc<Mutex<LogMgr>>,
		bm: Arc<Mutex<BufferMgr>>,
	) -> Self {
		StartRecord::write_to_log(Arc::clone(&lm), txnum).unwrap();

		Self { lm, bm, tx, txnum }
	}

	pub fn commit(&mut self) -> Result<()> {
		lock!(self, CommitRecord::write_to_log(Arc::clone(&self.lm), self.txnum)?)
	}

	pub fn rollback(&mut self) -> Result<()> {
		self.do_rollback()?;
		lock!(self, RollbackRecord::write_to_log(Arc::clone(&self.lm), self.txnum)?)
	}

	pub fn recover(&mut self) -> Result<()> {
		self.do_recover()?;
		lock!(self, CheckpointRecord::write_to_log(Arc::clone(&self.lm))?)
	}

	pub fn set_i32(&mut self, buff: &mut Buffer, offset: i32, _new_val: i32) -> Result<u64> {
		let old_val = buff.contents().get_i32(offset as usize)?;
		if let Some(blk) = buff.block() {
			return SetI32Record::write_to_log(
				Arc::clone(&self.lm),
				self.txnum,
				blk,
				offset,
				old_val,
			);
		}

		Err(From::from(RecoveryMgrError::BufferFailed(
			"set_i32".to_string(),
		)))
	}

	pub fn set_string(&mut self, buff: &mut Buffer, offset: i32, _new_val: &str) -> Result<u64> {
		let old_val = buff.contents().get_string(offset as usize)?;

		if let Some(blk) = buff.block() {
			return SetStringRecord::write_to_log(
				Arc::clone(&self.lm),
				self.txnum,
				blk,
				offset,
				old_val,
			);
		}

		Err(From::from(RecoveryMgrError::BufferFailed(
			"set_string".to_string(),
		)))
	}

	fn do_rollback(&mut self) -> Result<()> {
		let mut lm = self.lm.lock().unwrap();
		
		let iter = lm.iterator()?;
		// この辺map等の処理に変えたい
		for bytes in iter {
			let rec = create_log_record(bytes)?;
			if rec.tx_number() == self.txnum {
				if rec.op() == TxType::START {
					return Ok(())
				}

				rec.undo(&mut self.tx)?;
			}
		}

		Ok(())
	}
	fn do_recover(&mut self) -> Result<()> {
		let mut finished_txs = vec![];
		let mut lm = self.lm.lock().unwrap();
		let iter = lm.iterator()?;
		for bytes in iter {
			let rec = create_log_record(bytes)?;
			match rec.op() {
				TxType::CHECKPOINT => return Ok(()),
				TxType::COMMIT | TxType::ROLLBACK => {
					finished_txs.push(rec.tx_number());
				}
				_ => {
					if !finished_txs.contains(&rec.tx_number()) {
						rec.undo(&mut self.tx)?;
					}
				}
			}
		}

		Ok(())
	}
}
