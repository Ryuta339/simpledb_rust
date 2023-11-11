use anyhow::Result;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::file::page::Page;

#[derive(FromPrimitive)]
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
			Some(TxType::SETI32) => Ok(Box::new(SetI32Record{})),
			Some(TxType::SETSTRING) => Ok(Box::new(SetStringRecord {})),
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

pub struct SetI32Record {}

impl LogRecord for SetI32Record {
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

pub struct SetStringRecord {}

impl LogRecord for SetStringRecord {
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

