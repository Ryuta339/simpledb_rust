use anyhow::Result;
use core::fmt;
use std::{
	collections::HashMap,
	sync::{Arc, Mutex},
	thread,
	time::{Duration, SystemTime},
};

use crate::file::block_id::BlockId;

const MAX_TIME: u64 = 10_000;

#[derive(Debug)]
enum LockTableError {
	LockAbort,
	LockFailed(String),
}

impl std::error::Error for LockTableError {}
impl fmt::Display for LockTableError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			LockTableError::LockAbort => {
				write!(f, "lock abort")
			}
			LockTableError::LockFailed(s) => {
				write!(f, "lock failed: {}", s)
			}
		}
	}
}

macro_rules! lock {
	($self:ident, $processing:block, $msg:literal) => {
		if ($self.l.lock().is_ok())
			$processing
		else {
			Err(From::from(LockTableError::LockFailed(String::from($msg))))
		}
	}
}
macro_rules! sleep {
	($cond:expr) => {
		let timestamp = SystemTime::now();
		while $cond {
			if waiting_too_long(timestamp) {
				return Err(From::from(LockTableError::LockAbort));
			}
			thread::sleep(Duration::new(1, 0));
		}
	}
}

pub struct LockTable {
	locks: HashMap<BlockId, i32>,
	l: Arc<Mutex<()>>,
}

impl LockTable {
	pub fn s_lock(&mut self, blk: &BlockId) -> Result<()> {
		lock!(self, {
			sleep!(self.has_x_lock(&blk));
			let val = self.get_lock_val(&blk);
			*self.locks.entry(blk.clone()).or_insert(val.try_into().unwrap()) = val;

			return Ok(());
		}, "s_lock")
	}
	pub fn x_lock(&mut self, blk: &BlockId) -> Result<()> {
		lock!(self, {
			sleep!(self.has_other_s_locks(&blk));
			self.locks.entry(blk.clone()).or_insert(-1);

			return Ok(())
		}, "x_lock")
	}
	pub fn unlock(&mut self, blk: &BlockId) -> Result<()> {
		lock!(self, {
			let val = self.get_lock_val(&blk);
			if val > 1 {
				self.locks.entry(blk.clone()).or_insert(val - 1);
			} else {
				self.locks.remove(&blk);
			}

			return Ok(())
		}, "unlock")
	}
	fn has_x_lock(&self, blk: &BlockId) -> bool {
		self.get_lock_val(&blk) < 0 
	}
	fn has_other_s_locks(&self, blk: &BlockId) -> bool {
		self.get_lock_val(&blk) > 1
	}
	fn get_lock_val(&self, blk: &BlockId) -> i32 {
		match self.locks.get(&blk) {
			Some(&ival) => ival,
			None => 0,
		}
	}
}

fn waiting_too_long(starttime: SystemTime) -> bool {
	let now = SystemTime::now();
	let diff = now.duration_since(starttime).unwrap();
	diff.as_millis() as u64 > MAX_TIME
}
