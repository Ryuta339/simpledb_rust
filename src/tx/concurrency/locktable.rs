use anyhow::Result;
use core::fmt;
use std::{
	collections::HashMap,
	sync::{Arc, Mutex, MutexGuard},
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

#[derive(Debug, Clone)]
pub struct LockTable {
	locks: Arc<Mutex<HashMap<BlockId, i32>>>,
}

impl LockTable {
	pub fn new() -> Self {
		Self {
			locks: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn s_lock(&mut self, blk: &BlockId) -> Result<()> {
		let timestamp = SystemTime::now();

		while !waiting_too_long(timestamp) {
			let mut locks = self.locks.lock().unwrap();
			if !has_x_lock(&locks, &blk) {
				*locks.entry(blk.clone()).or_insert(0) += 1;
				return Ok(());
			}
			drop(locks); // release
			thread::sleep(Duration::new(1, 0));
		}

		Err(From::from(LockTableError::LockAbort))
	}
	pub fn x_lock(&mut self, blk: &BlockId) -> Result<()> {
		let timestamp = SystemTime::now();

		while !waiting_too_long(timestamp) {
			let mut locks = self.locks.lock().unwrap();
			if !has_other_s_locks(&locks, &blk) {
				*locks.entry(blk.clone()).or_insert(-1) = -1;
				return Ok(());
			}
			drop(locks); // release
			thread::sleep(Duration::new(1, 0));
		}

		Err(From::from(LockTableError::LockAbort))
	}
	pub fn unlock(&mut self, blk: &BlockId) -> Result<()> {
		let mut locks = self.locks.lock().unwrap();

		let val = get_lock_val(&locks, &blk);
		if val > 1 {
			locks.entry(blk.clone()).or_insert(val - 1);
		} else {
			locks.remove(&blk);
		}

		return Ok(());
	}
}

fn has_x_lock(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
	get_lock_val(locks, blk) < 0 
}
fn has_other_s_locks(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
	get_lock_val(locks, blk) > 1
}
fn get_lock_val(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> i32 {
	match locks.get(&blk) {
		Some(&ival) => ival,
		None => 0,
	}
}

fn waiting_too_long(starttime: SystemTime) -> bool {
	let now = SystemTime::now();
	let diff = now.duration_since(starttime).unwrap();
	diff.as_millis() as u64 > MAX_TIME
}
