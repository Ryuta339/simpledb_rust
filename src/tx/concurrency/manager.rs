use anyhow::Result;
use std::{
	collections::HashMap,
	sync::{Arc, Mutex, Once}
};

use super::locktable::{
	LockTable,
	LockTableKey,
};

pub struct ConcurrencyMgr {
	// static member (shared by all ConcurrentMgr)
	locktbl: Arc<Mutex<LockTable>>,
	locks: HashMap<LockTableKey, String>,
}

impl ConcurrencyMgr {
	pub fn new() -> Self {
		static mut SINGLETON: Option<Arc<Mutex<LockTable>>> = None;
		static ONCE: Once = Once::new();

		unsafe {
			ONCE.call_once(|| {
				let singleton = Arc::new(Mutex::new(LockTable::new()));
				SINGLETON = Some(singleton);
			});

			Self {
				locktbl: SINGLETON.clone().unwrap(),
				locks: HashMap::new(),
			}
		}
	}

	pub fn s_lock(&mut self, key: &LockTableKey) -> Result<()> {
		if self.locks.get(&key).is_none() {
			self.locktbl.lock().unwrap().s_lock(key)?;
			self.locks.insert(key.clone(), "S".to_string());
		}

		Ok(())
	}
	pub fn x_lock(&mut self, key: &LockTableKey) -> Result<()> {
		if !self.has_x_lock(key) {
			self.s_lock(key)?;
			self.locktbl.lock().unwrap().x_lock(key)?;
			self.locks.insert(key.clone(), "X".to_string());
		}

		Ok(())
	}
	pub fn release(&mut self) -> Result<()> {
		for key in self.locks.keys() {
			self.locktbl.lock().unwrap().unlock(key)?;
		}
		self.locks.clear();

		Ok(())
	}
	fn has_x_lock(&self, key: &LockTableKey) -> bool {
		let locktype = self.locks.get(key);
		locktype.is_some() && locktype.unwrap().eq("X")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_locktable_is_singleton() {
		// マルチスレッドでシングルトンであるかどうかが確認できていない
		let cm1 = ConcurrencyMgr::new();
		let cm2 = ConcurrencyMgr::new();
		assert!(Arc::ptr_eq(&cm1.locktbl, &cm2.locktbl));
	}
}
