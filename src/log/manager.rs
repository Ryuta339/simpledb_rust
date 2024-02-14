use anyhow::Result;
use core::fmt;
use std::mem;
use std::sync::{Arc, Mutex};

use crate::file::block_id::BlockId;
use crate::file::manager::FileMgr;
use crate::file::page::{Page, PageSetter};

use super::iterator::LogIterator;

#[derive(Debug)]
enum LogMgrError {
	LogPageAccessFailed,
}

impl std::error::Error for LogMgrError {}
impl fmt::Display for LogMgrError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			LogMgrError::LogPageAccessFailed => write!(f, "log access failed"),
		}
	}
}

pub struct LogMgr {
	fm: Arc<Mutex<FileMgr>>,
	logfile: String,
	logpage: Page,
	current_blk: BlockId,
	// latest log sequence number
	latest_lsn: u64,
	last_saved_lsn: u64,
}

impl LogMgr {
	pub fn new(fm: Arc<Mutex<FileMgr>>, logfile: &str) -> Result<Self> {
		let mut filemgr = fm.lock().unwrap();
		let mut logpage = Page::new_from_size(filemgr.blocksize() as usize);
		let logsize = filemgr.length(logfile)?;

		let logmgr;

		if logsize == 0 {
			let blk = filemgr.append(logfile)?;
			logpage.set(0, filemgr.blocksize() as i32)?;
			filemgr.write(&blk, &mut logpage)?;

			drop(filemgr);
			logmgr = Self {
				fm,
				logfile: logfile.to_string(),
				logpage,
				current_blk: blk,
				latest_lsn: 0,
				last_saved_lsn: 0,
			};
		} else {
			let newblk = BlockId::new(logfile, logsize - 1);
			filemgr.read(&newblk, &mut logpage)?;

			drop(filemgr);
			logmgr = Self {
				fm,
				logfile: logfile.to_string(),
				logpage,
				current_blk: newblk,
				latest_lsn: 0,
				last_saved_lsn: 0,
			};
		}

		Ok(logmgr)
	}

	pub fn iterator(&mut self) -> Result<LogIterator> {
		self.flush_to_fm()?;
		let iter = LogIterator::new(Arc::clone(&self.fm), self.current_blk.clone())?;

		Ok(iter)
	}

	pub fn flush(&mut self, lsn: u64) -> Result<()> {
		if lsn > self.last_saved_lsn {
			self.flush_to_fm()?;
		}

		Ok(())
	}

	pub fn append(&mut self, logrec: &mut Vec<u8>) -> Result<u64> {
		let mut boundary = self.logpage.get_i32(0)?;
		let recsize = logrec.len() as i32;
		let int32_size = mem::size_of::<i32>() as i32;
		let bytes_needed = recsize + int32_size;

		if boundary - bytes_needed < int32_size {
			self.flush_to_fm()?;
			self.current_blk = self.append_newblk()?;
			boundary = self.logpage.get_i32(0)?;
		}

		let recpos = (boundary - bytes_needed) as usize;
		self.logpage.set_bytes(recpos, logrec)?;
		self.logpage.set_i32(0, recpos as i32)?;
		self.latest_lsn += 1;

		Ok(self.last_saved_lsn)
	}

	fn flush_to_fm(&mut self) -> Result<()> {
		let mut filemgr = self.fm.lock().unwrap();

		filemgr.write(&self.current_blk, &mut self.logpage)?;

		Ok(())
	}

	fn append_newblk(&mut self) -> Result<BlockId> {
		let mut filemgr = self.fm.lock().unwrap();

		let blk = filemgr.append(self.logfile.as_str())?;
		self.logpage.set_i32(0, filemgr. blocksize() as i32)?;
		filemgr.write(&blk, &mut self.logpage)?;

		Ok(blk)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::Path;
	use std::fs::remove_file;
	use crate::file::manager::FileMgr;

	static LOG_FILE: &str = "simpledb.log";

	#[test]
	fn log_test() {
		let filename = format!("logtest/{}", LOG_FILE);
		let path = Path::new(filename.as_str());
		if path.is_file() {
			let _ = remove_file(path);
		}
		let fm = FileMgr::new("logtest", 400).unwrap();
		let mut lm = LogMgr::new(
			Arc::new(Mutex::new(fm)),
			LOG_FILE
			).unwrap();
		let _ = create_records(&mut lm, 1, 35);
		let _ = print_log_records(&mut lm, "The log file now has these: records:");
		let _ = assert_log_records(&mut lm, 35, 1);
		let _ = create_records(&mut lm, 36, 70);
		let _ = lm.flush(65);
		let _ = print_log_records(&mut lm, "The log file now has these records:");
		let _ = assert_log_records(&mut lm, 70, 1);
	}

	fn print_log_records(lm: &mut LogMgr, msg: &str) -> Result<()> {
		println!("{}", msg);
		let iter = lm.iterator()?;
		for rec in iter {
			let p = Page::new_from_bytes(rec);
			let s = p.get_string(0).unwrap();
			let npos = Page::max_length(s.len());
			let val = p.get_i32(npos).unwrap();
			println!("[{}, {}]", s, val);
		}
		println!();

		Ok(())
	}

	fn assert_log_records(lm: &mut LogMgr, start: i32, end: i32) -> Result<()> {
		let iter = lm.iterator()?;
		let mut i = start;
		for rec in iter {
			let p = Page::new_from_bytes(rec);
			let s = p.get_string(0).unwrap();
			let npos = Page::max_length(s.len());
			let val = p.get_i32(npos).unwrap();
			assert_eq!(format!("record{}", i).as_str(), s);
			assert_eq!(i+100, val);
			i -= 1;
		}
		assert_eq!(end, i+1);
		Ok(())
	}

	fn create_records(lm: &mut LogMgr, start: i32, end: i32) -> Result<()> {
		println!("Creating records:");
		for i in start..(end+1) {
			let mut rec = create_log_record(format!("record{}", i).as_str(), i+100)?;
			let lsn = lm.append(&mut rec)?;
			println!("{} ", lsn);
		}
		println!();
		Ok(())
	}

	fn create_log_record(s: &str, n: i32) -> Result<Vec<u8>> {
		let npos = Page::max_length(s.len());
		// let b = Vec::<u8>::with_capacity(npos + 32);
		let b = vec![0u8; npos+32];
		let mut p = Page::new_from_bytes(b);
		let _ = p.set(0, s.to_string())?;
		let _ = p.set(npos, n)?;
		Ok(p.contents().clone())
	}
}
