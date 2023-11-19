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
			Some(TxType::CHECKPOINT) => Ok(Box::new(SetCheckpointRecord {})),
			Some(TxType::START) => Ok(Box::new(SetStartRecord {})),
			Some(TxType::COMMIT) => Ok(Box::new(SetCommitRecord {})),
			Some(TxType::ROLLBACK) => Ok(Box::new(SetRollbackRecord {})),
			Some(TxType::SETI32) => Ok(Box::new(SetI32Record::new(p)?)),
			Some(TxType::SETSTRING) => Ok(Box::new(SetStringRecord::new(p)?)),
			None => panic!("TODO"),
		}
	}
}

pub struct SetCheckpointRecord {}

impl LogRecord for SetCheckpointRecord {
	fn op(&self) -> TxType {
		panic!("TODO");
	}
	fn tx_number(&self) -> i32 {
		panic!("TODO");
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

pub struct SetStartRecord {}

impl LogRecord for SetStartRecord {
	fn op(&self) -> TxType {
		panic!("TODO");
	}
	fn tx_number(&self) -> i32 {
		panic!("TODO");
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

pub struct SetCommitRecord {}

impl LogRecord for SetCommitRecord {
	fn op(&self) -> TxType {
		panic!("TODO");
	}
	fn tx_number(&self) -> i32 {
		panic!("TODO");
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
	}
}

pub struct SetRollbackRecord {}

impl LogRecord for SetRollbackRecord {
	fn op(&self) -> TxType {
		panic!("TODO");
	}
	fn tx_number(&self) -> i32 {
		panic!("TODO");
	}
	fn undo(&self, txnum: i32) -> Option<()> {
		panic!("TODO");
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
