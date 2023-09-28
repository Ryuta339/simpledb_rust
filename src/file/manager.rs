use anyhow::Result;
use core::fmt;
use std::{
	collections::HashMap,
	fs::{self, File, OpenOptions},
	io::{Read, Seek, SeekFrom, Write},
	path::Path,
	sync::{Arc, Mutex},
};

use super::{block_id::BlockId, page::Page};

#[derive(Debug)]
enum FileMgrError {
	ParseFailed,
	FileAccessFailed(String),
}

impl std::error::Error for FileMgrError {}
impl fmt::Display for FileMgrError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			FileMgrError::ParseFailed => write!(f, "parse failed"),
			FileMgrError::FileAccessFailed(filename) => {
				write!(f, "file access failed: {}", filename)
			}
		}
	}
}

pub struct FileMgr {
	db_directory: String,
	blocksize: u64,
	is_new: bool,
	open_files: HashMap<String, File>,
	l: Arc<Mutex<()>>,
}

impl FileMgr {
	pub fn new(db_directory: &str, blocksize: u64) -> Result<Self> {
		let path = Path::new(db_directory);
		let is_new = !path.exists();

		if is_new {
			fs::create_dir_all(path)?;
		}

		for entry in fs::read_dir(path)? {
			let entry_path = entry?.path();
			let filename = match entry_path.as_path().to_str() {
				Some(s) => s.to_string(),
				None => return Err(From::from(FileMgrError::ParseFailed)),
			};

			if filename.starts_with("temp") {
				fs::remove_file(entry_path)?;
			}
		}
		
		Ok(Self {
			db_directory: db_directory.to_string(),
			blocksize,
			is_new,
			open_files: HashMap::new(),
			l:Arc::new(Mutex::default()),
		})
	}

	pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> Result<()> {
		if self.l.lock().is_ok() {
			let offset = blk.number() * self.blocksize;
			if let Some(f) = self.get_file(blk.file_name().as_str()) {
				f.seek(SeekFrom::Start(offset))?;

				let read_len = f.read(p.contents())?;
				let p_len = p.contents().len();
				if read_len < p_len {
					let tmp = vec![0; p_len - read_len];
					f.write_all(&tmp)?;

					for i in read_len..p_len {
						p.contents()[i] = 0;
					}
				}

				return Ok(());
			}
		}

		Err(From::from(FileMgrError::FileAccessFailed(blk.file_name())))
	}

	pub fn append(&mut self, filename: &str) -> Result<BlockId> {
		if self.l.lock().is_ok() {
			let new_blknum = self.length(filename)?;
			let blk = BlockId::new(filename, new_blknum);

			let b: Vec<u8> = vec![0u8; self.blocksize as usize];

			let offset = blk.number() * self.blocksize;
			if let Some(f) = self.get_file(blk.file_name().as_str()) {
				f.seek(SeekFrom::Start(offset))?;
				f.write_all(&b)?;

				return Ok(blk);
			}
		}

		Err(From::from(FileMgrError::FileAccessFailed(filename.to_string())))
	}

	pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> Result<()> {
		if self.l.lock().is_ok() {
			let offset = blk.number() * self.blocksize;
			if let Some(f) = self.get_file(blk.file_name().as_str()) {
				f.seek(SeekFrom::Start(offset))?;
				f.write_all(p.contents())?;

				return Ok(());
			}
		}

		Err(From::from(FileMgrError::FileAccessFailed(blk.file_name())))
	}

	pub fn length(&mut self, filename: &str) -> Result<u64> {
		let path = Path::new(&self.db_directory).join(filename);
		let _ = self.get_file(filename).unwrap();
		let meta = fs::metadata(&path)?;

		// ceiling
		Ok((meta.len() + self.blocksize - 1) / self.blocksize)
	}

	pub fn get_file(&mut self, filename: &str) -> Option<&mut File> {
		let path = Path::new(&self.db_directory).join(&filename);

		let f = self.open_files.entry(filename.to_string()).or_insert(
			OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.open(&path)
				.unwrap(),
		);

		Some(f)
	}

	pub fn blocksize(&self) -> u64 {
		self.blocksize
	}

	pub fn is_new(&self) -> bool {
		self.is_new
	}

}


#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn write_and_read() {
		let mut fm = FileMgr::new("filetest", 400).unwrap();
		let blk = BlockId::new("testfile", 2);
		let mut p1 = Page::new_from_size(fm.blocksize() as usize);
		let pos1: usize = 88;
		let _ = p1.set_string(pos1, "abcdefghijklm".to_string());
		let size = Page::max_length("abcdefghijklm".len());
		let pos2: usize = pos1 + size;
		let _ = p1.set_i32(pos2, 345);
		let _ = fm.write(&blk, &mut p1);

		let mut p2 = Page::new_from_size(fm.blocksize() as usize);
		let _ = fm.read(&blk, &mut p2);

		assert_eq!("abcdefghijklm".to_string(), p2.get_string(pos1).unwrap());
		assert_eq!(345, p2.get_i32(pos2).unwrap());
	}
}
