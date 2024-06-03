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

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum LockTableKey {
	BID(BlockId),
	DUMMY(u64),
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
	($self:ident, $processing:block) => {
		let timestamp = SystemTime::now();
		while !waiting_too_long(timestamp) {
			let mut locks = $self.locks.lock().unwrap();
			$processing
			drop(locks);
			thread::sleep(Duration::new(1, 0));
		}
		return Err(From::from(LockTableError::LockAbort));
	}
}

#[derive(Debug, Clone)]
pub struct LockTable {
	locks: Arc<Mutex<HashMap<LockTableKey, i32>>>,
}

impl LockTable {
	pub fn new() -> Self {
		Self {
			locks: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn s_lock(&mut self, key: &LockTableKey) -> Result<()> {
		let timestamp = SystemTime::now();

		while !waiting_too_long(timestamp) {
			let mut locks = self.locks.lock().unwrap();
			if !has_x_lock(&locks, &key) {
				*locks.entry(key.clone()).or_insert(0) += 1;
				return Ok(());
			}
			drop(locks); // release
			thread::sleep(Duration::new(1, 0));
		}

		Err(From::from(LockTableError::LockAbort))
	}
	pub fn x_lock(&mut self, key: &LockTableKey) -> Result<()> {
		let timestamp = SystemTime::now();

		while !waiting_too_long(timestamp) {
			let mut locks = self.locks.lock().unwrap();
			if !has_other_s_locks(&locks, &key) {
				*locks.entry(key.clone()).or_insert(-1) = -1;
				return Ok(());
			}
			drop(locks); // release
			thread::sleep(Duration::new(1, 0));
		}

		Err(From::from(LockTableError::LockAbort))
	}
	pub fn unlock(&mut self, key: &LockTableKey) -> Result<()> {
		let mut locks = self.locks.lock().unwrap();

		let val = get_lock_val(&locks, &key);
		if val > 1 {
			locks.entry(key.clone()).or_insert(val - 1);
		} else {
			locks.remove(&key);
		}

		return Ok(());
	}
}

fn has_x_lock(locks: &MutexGuard<HashMap<LockTableKey, i32>>, key: &LockTableKey) -> bool {
	get_lock_val(locks, key) < 0 
}
fn has_other_s_locks(locks: &MutexGuard<HashMap<LockTableKey, i32>>, key: &LockTableKey) -> bool {
	get_lock_val(locks, key) > 1
}
fn get_lock_val(locks: &MutexGuard<HashMap<LockTableKey, i32>>, key: &LockTableKey) -> i32 {
	match locks.get(&key) {
		Some(&ival) => ival,
		None => 0,
	}
}

fn waiting_too_long(starttime: SystemTime) -> bool {
	let now = SystemTime::now();
	let diff = now.duration_since(starttime).unwrap();
	diff.as_millis() as u64 > MAX_TIME
}
