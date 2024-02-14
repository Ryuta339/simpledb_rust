use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
	file::{block_id::BlockId, manager::FileMgr, page::Page},
	log::manager::LogMgr,
};

#[derive(Debug)]
enum BufferError {
	BlockNotFound,
}

impl std::error::Error for BufferError {}
impl fmt::Display for BufferError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			&BufferError::BlockNotFound => {
				write!(f, "block not found")
			}
		}
	}
}

pub struct Buffer {
	fm: Arc<Mutex<FileMgr>>,
	lm: Arc<Mutex<LogMgr>>,
	contents: Page,
	blk: Option<BlockId>,
	pins: u64,
	txnum: i32,
	lsn: i32,
}

impl Buffer {
	pub fn new(fm: Arc<Mutex<FileMgr>>, lm: Arc<Mutex<LogMgr>>) -> Self {
		let blksize = fm.lock().unwrap().blocksize() as usize;
		let contents = Page::new_from_size(blksize);

		Self {
			fm,
			lm,
			contents,
			blk: None,
			pins: 0,
			txnum: -1,
			lsn: -1,
		}
	}

	pub fn contents(&mut self) -> &mut Page {
		&mut self.contents
	}

	pub fn block(&self) -> Option<&BlockId> {
		self.blk.as_ref()
	}

	pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
		self.txnum = txnum;
		if lsn >= 0 {
			self.lsn = lsn;
		}
	}

	pub fn is_pinned(&self) -> bool {
		self.pins > 0
	}

	pub fn modifying_tx(&self) -> i32 {
		self.txnum
	}

	pub fn assign_to_block(&mut self, b: BlockId) -> Result<()> {
		self.flush()?;
		self.fm.lock().unwrap().read(&b, &mut self.contents)?;
		self.blk = Some(b);
		self.pins = 0;

		Ok(())
	}

	pub fn flush(&mut self) -> Result<()> {
		if self.txnum >= 0 {
			self.lm.lock().unwrap().flush(self.lsn as u64)?;

			match self.blk.as_ref() {
				Some(blk) => {
					self.fm.lock().unwrap().write(blk, &mut self.contents)?;
					self.txnum = -1;
				}
				None => return Err(From::from(BufferError::BlockNotFound)),
			}
		}

		Ok(())
	}

	pub fn pin(&mut self) {
		self.pins += 1;
	}

	pub fn unpin(&mut self) {
		self.pins -= 1;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::file::{block_id::BlockId, manager::FileMgr, page::PageSetter};
	use crate::log::manager::LogMgr;
	use crate::buffer::manager::BufferMgr;

	static LOG_FILE: &str = "simpledb.log";

	#[test]
	fn buffer_test() {
		let fm = FileMgr::new("buffertest", 400).unwrap();
		let fm_arc = Arc::new(Mutex::new(fm));
		let lm = LogMgr::new(Arc::clone(&fm_arc), LOG_FILE).unwrap();
		let lm_arc = Arc::new(Mutex::new(lm));
		let mut bm = BufferMgr::new(fm_arc, lm_arc, 3);

		let buff1 = bm.pin(&BlockId::new("testfile", 1)).unwrap();
		{
			// In this block, buff1 is borrowed and cannot be used
			let mut b1 = buff1.lock().unwrap();
			let p = b1.contents();
			let n = p.get_i32(80).unwrap();
			let _ = p.set(80, n+1);
			b1.set_modified(1, 0);
			println!("The new value is {}", n + 1);
		}
		let _ = bm.unpin(buff1);

		// One of these pins will flush buff1 to disk:
		let buff2 = bm.pin(&BlockId::new("testfile", 2)).unwrap();
		let _buff3 = bm.pin(&BlockId::new("testfile", 3)).unwrap();
		let _buff4 = bm.pin(&BlockId::new("testfile", 4)).unwrap();

		let _ = bm.unpin(buff2);
		let buff2 = bm.pin(&BlockId::new("testfile", 1)).unwrap();
		{
			// In this block, buff2 is borrowed and cannot be used
			let mut b2 = buff2.lock().unwrap();
			let p = b2.contents();
			let _ = p.set(80, 9999);
			b2.set_modified(1, 0);
		}
		let _ = bm.unpin(buff2);
	}
}
