use anyhow::Result;
use core::fmt;
use std::{
	sync::{Arc, Mutex},
	thread,
	time::{Duration, SystemTime},
};

use super::buffer::Buffer;
use crate::{
	file::{block_id::BlockId, manager::FileMgr},
	log::manager::LogMgr,
};

const MAX_TIME: i64 = 10_000; // 10 seconds

#[derive(Debug)]
enum BufferMgrError {
	LockFailed(String),
	BufferAbort,
}

impl std::error::Error for BufferMgrError {}
impl fmt::Display for BufferMgrError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			BufferMgrError::LockFailed(s) => {
				write!(f, "lock failed: {}", s)
			}
			BufferMgrError::BufferAbort => {
				write!(f, "buffer abort")
			}
		}
	}
}

pub struct BufferMgr {
	bufferpool: Vec<Arc<Mutex<Buffer>>>,
	num_available: Arc<Mutex<usize>>,
}

impl BufferMgr {
	pub fn new(
		fm: Arc<Mutex<FileMgr>>,
		lm: Arc<Mutex<LogMgr>>,
		numbuffs: usize,
	) -> Self {
		let bufferpool = (0..numbuffs)
			.map(|_| Arc::new(Mutex::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm)))))
			.collect();

		Self {
			bufferpool,
			num_available: Arc::new(Mutex::new(numbuffs)),
		}
	}

	pub fn available(&self) -> Result<usize> {
		let num = self.num_available.lock().unwrap();
		Ok(*num)
	}

	pub fn flush_all(&mut self, txnum: i32) -> Result<()> {
		for i in 0..self.bufferpool.len() {
			let mut buff = self.bufferpool[i].lock().unwrap();
			if buff.modifying_tx() == txnum {
				buff.flush()?;
			}
		}
		Ok(())
	}

	pub fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<()> {
		let mut b = buff.lock().unwrap();
		b.unpin();
		if !b.is_pinned() {
			*(self.num_available.lock().unwrap()) += 1;
		}
		Ok(())
	}

	pub fn pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
		let timestamp = SystemTime::now();
		while !waiting_too_long(timestamp) {
			if let Ok(buff) = self.try_to_pin(blk) {
				return Ok(buff);
			}
			thread::sleep(Duration::new(1, 0))
		}

		Err(From::from(BufferMgrError::BufferAbort))
	}

	fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
		if let Some(buff) = self.pickup_pinnable_buffer(blk) {
			match buff.lock() {
				Err(e) => {
					return Err(From::from(BufferMgrError::LockFailed(
						"try_to_pin".to_string(),
					)));
				}
				Ok(mut b) => {
					if !b.is_pinned() {
						*(self.num_available.lock().unwrap()) -= 1;
					}
					b.pin()
				}
			}

			return Ok(buff);
		}

		Err(From::from(BufferMgrError::BufferAbort))
	}

	fn pickup_pinnable_buffer(&mut self, blk: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
		if let Some(buff) = self.find_existing_buffer(blk) {
			return Some(buff);
		}

		if let Some(buff) = self.choose_unpinned_buffer() {
			let mut b = buff.lock().unwrap();

			if let Err(e) = b.assign_to_block(blk.clone()) {
				eprintln!("failed to assign to block: {}", e);
				return None
			}
			
			drop(b);
			return Some(buff);
		}
		None
	}

	fn find_existing_buffer(&mut self, blk: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
		for i in 0..self.bufferpool.len() {
			let buff = self.bufferpool[i].lock().unwrap();
			if let Some(b) = buff.block() {
				if *b == *blk {
					return Some(Arc::clone(&self.bufferpool[i]))
				}
			}
		}
		None
	}

	fn choose_unpinned_buffer(&mut self) -> Option<Arc<Mutex<Buffer>>> {
		for i in 0..self.bufferpool.len() {
			let buff = self.bufferpool[i].lock().unwrap();
			if !buff.is_pinned() {
				return Some(Arc::clone(&self.bufferpool[i]));
			}
		}

		None
	}
}

fn waiting_too_long(starttime: SystemTime) -> bool {
	let now = SystemTime::now();
	let diff = now.duration_since(starttime).unwrap();

	diff.as_millis() as i64 > MAX_TIME
}


#[cfg(test)]
mod tests {
	use super::*;
	use crate::file::{block_id::BlockId, manager::FileMgr};
	use crate::log::manager::LogMgr;
	use crate::buffer::manager::BufferMgr;

	use std::cell::Ref;

	static LOG_FILE: &str = "simpledb.log";

	#[test]
	fn buffermgr_test() -> Result<()> {
		let fm = FileMgr::new("buffermgrtest", 400).unwrap();
		let fm_arc = Arc::new(Mutex::new(fm));
		let lm = LogMgr::new(Arc::clone(&fm_arc), LOG_FILE).unwrap();
		let lm_arc = Arc::new(Mutex::new(lm));
		let mut bm = BufferMgr::new(fm_arc, lm_arc, 3);
		
		let mut buffs: Vec<Option<Arc<Mutex<Buffer>>>> = vec![None; 6];
		buffs[0] = bm.pin(&BlockId::new("testfile", 0))?.into();
		buffs[1] = bm.pin(&BlockId::new("testfile", 1))?.into();
		buffs[2] = bm.pin(&BlockId::new("testfile", 2))?.into();
		bm.unpin(Arc::clone(&buffs[1].clone().unwrap()))?;
		buffs[1] = None;

		buffs[3] = bm.pin(&BlockId::new("testfile", 0))?.into();
		buffs[4] = bm.pin(&BlockId::new("testfile", 1))?.into();

		assert_eq!(bm.available()?, 0);

		println!("Abailable buffers: {}", bm.available()?);
		println!("Attempting to pin block 3...");
		let result = bm.pin(&BlockId::new("testfile", 3));
		assert!(result.is_err());

		bm.unpin(Arc::clone(&buffs[2].clone().unwrap()))?;
		buffs[2] = None;
		buffs[5] = bm.pin(&BlockId::new("testfile", 3))?.into();

		println!("Check buff");
		let assertions: Vec<&'static dyn BufferAssertion> = vec![
			&HasBlockIdBuffer{ blknum: 0 },
			&NoneBuffer{},
			&NoneBuffer{},
			&HasBlockIdBuffer{ blknum: 0 },
			&HasBlockIdBuffer{ blknum: 1 },
			&HasBlockIdBuffer{ blknum: 3 },
		];

		for ( buff, assertion ) in buffs.iter_mut().zip(assertions.iter()) {
				assertion.assert_buffer(buff);
		}

		Ok(())
	}

	trait BufferAssertion {
		fn assert_buffer(&self, buff: &Option<Arc<Mutex<Buffer>>>);
	}

	struct NoneBuffer {}
	impl BufferAssertion for NoneBuffer {
		fn assert_buffer(&self, buff: &Option<Arc<Mutex<Buffer>>>) {
			assert!(buff.is_none());
		}
	}

	struct HasBlockIdBuffer {
		blknum: u64,
	}
	impl BufferAssertion for HasBlockIdBuffer {
		fn assert_buffer(&self, buff: &Option<Arc<Mutex<Buffer>>>) {
			assert!(buff.is_some());
			{
				let result = buff.as_ref().unwrap().as_ref().lock().unwrap();
				assert_eq!(result.block(), Some(&BlockId::new("testfile", self.blknum)));
			}
		}
	}
}
