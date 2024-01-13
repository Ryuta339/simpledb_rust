use anyhow::{Error, Result};
use std::mem;

pub trait ToPageBytes {
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

pub trait FromPageBytes: Sized {
	fn from_page_bytes(b: &[u8], err: Error) -> Result<Self>;
}
impl FromPageBytes for i32 {
	fn from_page_bytes(b: &[u8], err: Error) -> Result<Self> {
		let i32_size = mem::size_of::<i32>();

		if i32_size - 1 < b.len() {
			let bytes = &b[0..i32_size];
			Ok(i32::from_be_bytes((*bytes).try_into()?))
		} else {
			Err(err)
		}
	}
}
impl FromPageBytes for &[u8] {
	fn from_page_bytes(b: &[u8], err: Error) -> Result<Self> {
		let new_offset = mem::size_of::<i32>();
		let len = i32::from_page_bytes(&b, err)? as usize;

		if new_offset + len - 1 < b.len() {
			//Ok(&Vec::from(&b[new_offset..new_offset + len]))
			panic!("How to do?");
		} else {
			Err(err)
		}
	}
}
