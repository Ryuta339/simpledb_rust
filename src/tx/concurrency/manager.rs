use std::{
	collections::HashMap,
	sync::{Arc, LockResult, Mutex, Once, ONCE_INIT}
};

use super::locktable::LockTable;
use crate::file::block_id::BlockId;
use crate::tx::concurrency::locktable;

pub struct ConcurrencyMgr {
	// static member (shared by all ConcurrentMgr)
	locktbl: Arc<Mutex<LockTable>>,
	locks: HashMap<BlockId, String>,
}

impl ConcurrencyMgr {
	pub fn new() -> Self {
		static mut SINGLETON: Option<Arc<Mutex<LockTable>>> = None;
		static ONCE: Once = ONCE_INIT;

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
}
