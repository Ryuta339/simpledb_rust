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


