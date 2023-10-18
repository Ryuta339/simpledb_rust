use anyhow::Result;
use core::fmt;
use std::error::Error;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

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

impl Error for BufferMgrError {}
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
	bufferpool: Vec<Arc<RefCell<Buffer>>>,
	num_available: usize,
	l: Arc<Mutex<()>>,
}

impl BufferMgr {
	pub fn new(
		fm: Arc<RefCell<FileMgr>>,
		lm: Arc<RefCell<LogMgr>>,
		numbuffs: usize,
	) -> Self {
		let bufferpool = (0..numbuffs).map(|_| Arc::new(RefCell::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm)))))
			.collect();

		Self {
			bufferpool,
			num_available: numbuffs,
			l: Arc::new(Mutex::default()),
		}
	}

	pub fn available(&self) -> Result<usize> {
		if self.l.lock().is_ok() {
			return Ok(self.num_available);
		}

		Err(From::from(BufferMgrError::LockFailed(
			"available".to_string(),
		)))
	}

	pub fn flush_all(&mut self, txnum: i32) -> Result<()> {
		if self.l.lock().is_ok() {
			// TODO: seems good to use filter
			for i in 0..self.bufferpool.len() {
				if self.bufferpool[i].borrow().modifying_tx() == txnum {
					self.bufferpool[i].borrow_mut().flush()?;
				}
			}
		}

		Err(From::from(BufferMgrError::LockFailed(
			"available".to_string(),
		)))
	}

	pub fn unpin(&mut self, buff: Arc<RefCell<Buffer>>) -> Result<()> {
		if self.l.lock().is_ok() {
			buff.borrow_mut().unpin();

			if !buff.borrow().is_pinned() {
				self.num_available += 1;
			}

			return Ok(());
		}

		Err(From::from(BufferMgrError::LockFailed("unpin".to_string())))
	}

	pub fn pin(&mut self, blk: &BlockId) -> Result<Arc<RefCell<Buffer>>> {
		if self.l.lock().is_ok() {
			let timestamp = SystemTime::now();

			while !waiting_too_long(timestamp) {
				if let Ok(buff) = self.try_to_pin(blk) {
					return Ok(buff);
				}
				thread::sleep(Duration::new(1, 0))
			}

			return Err(From::from(BufferMgrError::BufferAbort))
		}

		Err(From::from(BufferMgrError::LockFailed("pin".to_string())))
	}

	fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<RefCell<Buffer>>> {
		if let Some(buff) = self.pickup_pinnable_buffer(blk) {
			if !buff.borrow_mut().is_pinned() {
				self.num_available -= 1;
			}
			buff.borrow_mut().pin();

			return Ok(buff);
		}

		Err(From::from(BufferMgrError::BufferAbort))
	}

	fn pickup_pinnable_buffer(&mut self, blk: &BlockId) -> Option<Arc<RefCell<Buffer>>> {
		self.find_existing_buffer(blk).or_else(|| {
			self.choose_unpinned_buffer().and_then(|buff| {
				if let Err(e) = buff.borrow_mut().assign_to_block(blk.clone()) {
					eprintln!("failed to assign to block: {}", e);
					return None;
				}

				Some(buff)
			})
		})
	}

	fn find_existing_buffer(&mut self, blk: &BlockId) -> Option<Arc<RefCell<Buffer>>> {
		for i in 0..self.bufferpool.len() {
			if let Some(b) = self.bufferpool[i].borrow().block() {
				if *b == *blk {
					return Some(Arc::clone(&self.bufferpool[i]));
				}
			}
		}
		None
	}

	fn choose_unpinned_buffer(&mut self) -> Option<Arc<RefCell<Buffer>>> {
		for i in 0..self.bufferpool.len() {
			if !self.bufferpool[i].borrow().is_pinned() {
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
		let fm_arc = Arc::new(RefCell::new(fm));
		let lm = LogMgr::new(Arc::clone(&fm_arc), LOG_FILE).unwrap();
		let lm_arc = Arc::new(RefCell::new(lm));
		let mut bm = BufferMgr::new(fm_arc, lm_arc, 3);
		
		let mut buffs: Vec<Option<Arc<RefCell<Buffer>>>> = vec![None; 6];
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
		fn assert_buffer(&self, buff: &Option<Arc<RefCell<Buffer>>>);
	}

	struct NoneBuffer {}
	impl BufferAssertion for NoneBuffer {
		fn assert_buffer(&self, buff: &Option<Arc<RefCell<Buffer>>>) {
			assert!(buff.is_none());
		}
	}

	struct HasBlockIdBuffer {
		blknum: u64,
	}
	impl BufferAssertion for HasBlockIdBuffer {
		fn assert_buffer(&self, buff: &Option<Arc<RefCell<Buffer>>>) {
			assert!(buff.is_some());
			{
				let result: Ref<Buffer> = buff.as_ref().unwrap().as_ref().borrow();
				assert_eq!(result.block(), Some(&BlockId::new("testfile", self.blknum)));
			}
		}
	}
}
