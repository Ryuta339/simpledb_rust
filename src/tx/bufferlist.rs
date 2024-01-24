use anyhow::Result;
use std::{
	cell::RefCell,
	collections::HashMap,
	sync::{Arc, Mutex},
};


use crate::{
	buffer::{buffer::Buffer, manager::BufferMgr},
	file::block_id::BlockId,
};

pub struct BufferList {
	buffers: HashMap<BlockId, Arc<RefCell<Buffer>>>,
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
	fn get_buffer(&mut self, blk: &BlockId) -> Option<&mut Arc<RefCell<Buffer>>> {
		self.buffers.get_mut(blk)
	}
	fn pin(&mut self, blk: &BlockId) -> Result<()> {
		let buff = self.bm.lock().unwrap().pin(blk)?;
		self.buffers.insert(blk.clone(), buff);
		self.pins.push(blk.clone());

		Ok(())
	}
}
