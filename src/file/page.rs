use anyhow::Result;
use core::fmt;
use itertools::izip;
use std::mem;

#[derive(Debug)]
enum PageError {
	BufferSizeExceeded,
}

impl std::error::Error for PageError {}
impl fmt::Display for PageError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			&PageError::BufferSizeExceeded => write!(f, "buffer size exceeded"),
		}
	}
}

pub trait ToPageBytes: Sized {
	fn to_page_bytes(&self) -> Vec<u8>;
}
impl ToPageBytes for i32 {
	fn to_page_bytes(&self) -> Vec<u8> {
		self.to_be_bytes().to_vec()
	}
}
impl ToPageBytes for &[u8] {
	fn to_page_bytes(&self) -> Vec<u8> {
		let mut v = (self.len() as i32).to_page_bytes();
		v.append(&mut self.to_vec());
		v
	}
}
impl ToPageBytes for String {
	fn to_page_bytes(&self) -> Vec<u8> {
		self.as_bytes().to_page_bytes()
	}
}

trait Setter<T: ToPageBytes> {
	fn set (&mut self, offset: usize, t: T) -> Result<usize>;
}

pub struct Page {
	bb: Vec<u8>,
}

impl<T: ToPageBytes> Setter<T> for Page {
	fn set (&mut self, offset: usize, t: T) -> Result<usize> {
		let bytes = t.to_page_bytes();
		self.set_page_bytes(offset, bytes)
	}
}

impl Page {
	pub fn new_from_bytes(b: Vec<u8>) -> Self {
		Self { bb: b }
	}

	pub fn new_from_size(blocksize: usize) -> Self {
		Self {
			bb: vec![0u8; blocksize],
		}
	}

	fn set_page_bytes(&mut self, offset: usize, b: Vec<u8>) -> Result<usize> {
		let size = b.len();
		if offset + size - 1 < self.bb.len() {
			for (p, added) in izip!(&mut self.bb[offset..offset+size], &b) {
				*p = *added;
			}
			Ok(offset + size)
		} else {
			Err(PageError::BufferSizeExceeded.into())
		}
	}

	pub fn get_i32(&self, offset: usize) -> Result<i32> {
		let i32_size = mem::size_of::<i32>();

		if offset + i32_size - 1 < self.bb.len() {
			let bytes = &self.bb[offset..offset + i32_size];
			Ok(i32::from_be_bytes((*bytes).try_into()?))
		} else {
			Err(PageError::BufferSizeExceeded.into())
		}
	}

	pub fn set_i32(&mut self, offset: usize, n: i32) -> Result<usize> {
		self.set(offset, n)
	}

	pub fn get_bytes(&self, offset: usize) -> Result<&[u8]> {
		let len = self.get_i32(offset)? as usize;
		let new_offset = offset + mem::size_of::<i32>();

		if new_offset + len - 1 < self.bb.len() {
			Ok(&self.bb[new_offset..new_offset + len])
		} else {
			Err(PageError::BufferSizeExceeded.into())
		}
	}

	pub fn set_bytes(&mut self, offset: usize, b: &[u8]) -> Result<usize> {
		self.set(offset, b)
	}

	pub fn get_string(&self, offset: usize) -> Result<String> {
		let bytes = self.get_bytes(offset)?;
		let s = String::from_utf8(bytes.to_vec())?;

		Ok(s)
	}

	pub fn set_string(&mut self, offset: usize, s: String) -> Result<usize> {
		self.set(offset, s)
	}

	pub fn max_length(strlen: usize) -> usize {
		mem::size_of::<i32>() + (strlen * mem::size_of::<u8>())
	}

	pub fn contents(&mut self) -> &mut Vec<u8> {
		&mut self.bb
	}

	pub(crate) fn get_bytes_vec(&self, offset: usize) -> Result<Vec<u8>> {
		let len = self.get_i32(offset)? as usize;
		let new_offset = offset + mem::size_of::<i32>();

		if new_offset + len - 1 < self.bb.len() {
			Ok(self.bb[new_offset..new_offset + len].try_into()?)
		} else {
			Err(PageError::BufferSizeExceeded.into())
		}
	}
}
