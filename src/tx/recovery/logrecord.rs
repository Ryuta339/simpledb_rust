use core::fmt;
use std::{cell::RefCell, mem, sync::Arc};
use anyhow::Result;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::{
	file::{block_id::BlockId, page::Page},
	log::manager::LogMgr,
};

#[derive(FromPrimitive, Debug, Eq, PartialEq, Clone, Copy)]
pub enum TxType {
	CHECKPOINT = 0,
	START = 1,
	COMMIT = 2,
	ROLLBACK = 3,
	SETI32 = 4,
	SETSTRING = 5,
}

pub trait LogRecord {
	fn op(&self) -> TxType;
	fn tx_number(&self) -> i32;
	fn undo(&self, txnum: i32) -> Option<()>;
}

impl dyn LogRecord {
	pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<Self>> {
		let p = Page::new_from_bytes(bytes);
		let tx_type: i32 = p.get_i32(0)?;

		match FromPrimitive::from_i32(tx_type) {
			Some(TxType::CHECKPOINT) => Ok(Box::new(CheckpointRecord::new(p)?)),
			Some(TxType::START) => Ok(Box::new(StartRecord::new(p)?)),
			Some(TxType::COMMIT) => Ok(Box::new(CommitRecord::new(p)?)),
			Some(TxType::ROLLBACK) => Ok(Box::new(RollbackRecord::new(p)?)),
			Some(TxType::SETI32) => Ok(Box::new(SetI32Record::new(p)?)),
			Some(TxType::SETSTRING) => Ok(Box::new(SetStringRecord::new(p)?)),
			None => panic!("Unsupported TxType found"),
		}
	}
}

pub struct CheckpointRecord {}

impl fmt::Display for CheckpointRecord {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<CHECKPOINT>")
	}
}

impl LogRecord for CheckpointRecord {
	fn op(&self) -> TxType {
		TxType::CHECKPOINT
	}
	fn tx_number(&self) -> i32 {
		-1 // dummy value
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

impl CheckpointRecord {
	pub fn new(p: Page) -> Result<Self> {
		Ok(Self {})
	}

	pub fn write_to_log(lm: Arc<RefCell<LogMgr>>) -> Result<u64> {
		let reclen = mem::size_of::<i32>();

		let mut p = Page::new_from_size(reclen);
		p.set_i32(0, TxType::CHECKPOINT as i32)?;

		lm.borrow_mut().append(p.contents())
	}
}

pub struct StartRecord {
	txnum: i32,
}

impl fmt::Display for StartRecord {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<START {}>", self.txnum)
	}
}

impl LogRecord for StartRecord {
	fn op(&self) -> TxType {
		TxType::START
	}
	fn tx_number(&self) -> i32 {
		self.txnum
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

impl StartRecord {
	pub fn new(p: Page) -> Result<Self> {
		let tpos = mem::size_of::<i32>();
		let txnum = p.get_i32(tpos)?;

		Ok(Self { txnum })
	}

	pub fn write_to_log(lm: Arc<RefCell<LogMgr>>, txnum: i32) -> Result<u64> {
		let tpos = mem::size_of::<i32>();
		let reclen = tpos + mem::size_of::<i32>();

		let mut p = Page::new_from_size(reclen as usize);
		p.set_i32(0, TxType::START as i32)?;
		p.set_i32(tpos, txnum)?;

		lm.borrow_mut().append(p.contents())
	}
}

pub struct CommitRecord {
	txnum: i32,
}

impl fmt::Display for CommitRecord {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<COMIIT {}>", self.txnum)
	}
}

impl LogRecord for CommitRecord {
	fn op(&self) -> TxType {
		TxType::COMMIT
	}
	fn tx_number(&self) -> i32 {
		self.txnum
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

impl CommitRecord {
	pub fn new (p: Page) -> Result<Self> {
		let tpos = mem::size_of::<i32>();
		let txnum = p.get_i32(tpos)?;

		Ok(Self { txnum })
	}

	pub fn write_to_log(lm: Arc<RefCell<LogMgr>>, txnum: i32) -> Result<u64> {
		let tpos = mem::size_of::<i32>();
		let reclen = tpos + mem::size_of::<i32>();

		let mut p = Page::new_from_size(reclen as usize);
		p.set_i32(0, TxType::COMMIT as i32)?;
		p.set_i32(tpos, txnum)?;

		lm.borrow_mut().append(p.contents())
	}
}

pub struct RollbackRecord {
	txnum: i32,
}

impl fmt::Display for RollbackRecord {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<ROLLBACK {}>", self.txnum)
	}
}

impl LogRecord for RollbackRecord {
	fn op(&self) -> TxType {
		TxType::ROLLBACK
	}
	fn tx_number(&self) -> i32 {
		self.txnum
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

impl RollbackRecord {
	pub fn new(p: Page) -> Result<Self> {
		let tpos = mem::size_of::<i32>();
		let txnum = p.get_i32(tpos)?;

		Ok(Self { txnum })
	}

	pub fn write_to_log(lm: Arc<RefCell<LogMgr>>, txnum: i32) -> Result<u64> {
		let tpos = mem::size_of::<i32>();
		let reclen = tpos + mem::size_of::<i32>();

		let mut p = Page::new_from_size(reclen as usize);
		p.set_i32(0, TxType::ROLLBACK as i32)?;
		p.set_i32(tpos, txnum)?;

		lm.borrow_mut().append(p.contents())
	}
}

pub struct SetI32Record {
	txnum: i32,
	offset: i32,
	val: i32,
	blk: BlockId,
}

impl fmt::Display for SetI32Record {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"<SETI32 {} {} {} {}>",
			self.txnum, self.blk, self.offset, self.val,
		)
	}
}

impl LogRecord for SetI32Record {
	fn op(&self) -> TxType {
		TxType::SETI32
	}
	fn tx_number(&self) -> i32 {
		self.txnum
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

impl SetI32Record {
	pub fn new(p: Page) -> Result<Self> {
		let tpos = mem::size_of::<i32>();
		let txnum = p.get_i32(tpos)?;
		let fpos = tpos + mem::size_of::<i32>();
		let filename = p.get_string(fpos)?;
		let bpos = fpos + Page::max_length(filename.len());
		let blknum = p.get_i32(bpos)?;
		let blk = BlockId::new(&filename, blknum as u64);
		let opos = bpos + mem::size_of::<i32>();
		let offset = p.get_i32(opos)?;
		let vpos = opos + mem::size_of::<i32>();
		let val = p.get_i32(vpos)?;

		Ok(Self {
			txnum,
			offset,
			val,
			blk,
		})
	}

	pub fn write_to_log(
		lm: Arc<RefCell<LogMgr>>,
		txnum: i32,
		blk: BlockId,
		offset: i32,
		val: i32,
	) -> Result<u64> {
		let tpos = mem::size_of::<i32>();
		let fpos = tpos + mem::size_of::<i32>();
		let bpos = fpos + Page::max_length(blk.file_name().len());
		let opos = bpos + mem::size_of::<i32>();
		let vpos = opos + mem::size_of::<i32>();
		let reclen = vpos + mem::size_of::<i32>();

		let mut p = Page::new_from_size(reclen as usize);
		p.set_i32(0, TxType::SETI32 as i32)?;
		p.set_i32(tpos, txnum)?;
		p.set_string(fpos, blk.file_name())?;
		p.set_i32(bpos, blk.number() as i32)?;
		p.set_i32(opos, offset)?;
		p.set_i32(vpos, val)?;

		lm.borrow_mut().append(p.contents())
	}
}

pub struct SetStringRecord {
	txnum: i32,
	offset: i32,
	val: String,
	blk: BlockId,
}

impl fmt::Display for SetStringRecord {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"<SETSTRING {} {} {} {}>",
			self.txnum, self.blk, self.offset, self.val
		)
	}
}

impl LogRecord for SetStringRecord {
	fn op(&self) -> TxType {
		TxType::SETSTRING
	}
	fn tx_number(&self) -> i32 {
		self.txnum
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

impl SetStringRecord {
	pub fn new(p: Page) -> Result<Self> {
		let tpos = mem::size_of::<i32>();
		let txnum = p.get_i32(tpos)?;
		let fpos = tpos + mem::size_of::<i32>();
		let filename = p.get_string(fpos)?;
		let bpos = fpos + Page::max_length(filename.len());
		let blknum = p.get_i32(bpos)?;
		let blk = BlockId::new(&filename, blknum as u64);
		let opos = bpos + mem::size_of::<i32>();
		let offset = p.get_i32(opos)?;
		let vpos = opos + mem::size_of::<i32>();
		let val = p.get_string(vpos)?;

		Ok(Self {
			txnum,
			offset,
			val,
			blk,
		})
	}

	pub fn write_to_log(
		lm: Arc<RefCell<LogMgr>>,
		txnum: i32,
		blk: BlockId,
		offset: i32,
		val: String,
	) -> Result<u64> {
		let tpos = mem::size_of::<i32>();
		let fpos = tpos + mem::size_of::<i32>();
		let bpos = fpos + Page::max_length(blk.file_name().len());
		let opos = bpos + mem::size_of::<i32>();
		let vpos = opos + mem::size_of::<i32>();
		let reclen = vpos + Page::max_length(val.len());
		
		let mut p = Page::new_from_size(reclen);
		p.set_i32(0, TxType::SETSTRING as i32)?;
		p.set_i32(tpos, txnum)?;
		p.set_string(fpos, blk.file_name())?;
		p.set_i32(bpos, blk.number() as i32)?;
		p.set_i32(opos, offset)?;
		p.set_string(vpos, val)?;

		lm.borrow_mut().append(p.contents())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_create_log_record() -> Result<()> {
		let tests_list: Vec<(Vec<u8>, TxType, i32)> = vec![
			(vec![0x00, 0x00, 0x00, 0x00], TxType::CHECKPOINT, -1),
			(vec![0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xFF], TxType::START, 255),
			(vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0xDD, 0xFF], TxType::COMMIT, 56831),
			(vec![0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0A], TxType::ROLLBACK, 10),
			// (vec![0x00, 0x00, 0x00, 0x04, 0x00, 0x0B, 0x00, 0x07], TxType::SETI32, 720903),
			// (vec![0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x10, 0x00], TxType::SETSTRING, 16781312),
		];

		tests_list.iter().for_each(|(bytes, expected_txtype, expected_txnum)| {
			let actual: Box<dyn LogRecord> = <dyn LogRecord>::create_log_record(bytes.to_vec()).unwrap();
			assert_eq!(*expected_txtype, actual.op());
			assert_eq!(*expected_txnum, actual.tx_number());
		});

		Ok(())
	}
}
