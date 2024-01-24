use std::{
	collections::HashMap,
	sync::{Arc, Mutex},
};


use crate::{
	buffer::{buffer::Buffer, manager::BufferMgr},
	file::block_id::BlockId,
};

pub struct BufferList {
	buffers: HashMap<BlockId, Buffer>,
	pins: Vec<BlockId>,
	bm: Arc<Mutex<BufferMgr>>,
}

impl BufferList {
	pub fn new(bm: Arc<Mutex<BufferMgr>>) -> Self {
		Self {
			buffers: HashMap::new(),
			pins: vec![],
			bm,
		}
	}
	fn get_buffer(&self, blk: &BlockId) -> Option<&Buffer> {
		self.buffers.get(blk)
	}
}
