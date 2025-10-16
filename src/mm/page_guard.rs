use crate::mm::BufferManager;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
type BlockId = u32;
/// PageGuard 在构造时 pin 一个页面，Drop 时自动 unpin
pub struct PageGuard<'a> {
    pub(crate) mgr: *mut BufferManager,
    block_id: BlockId,
    data_ptr: *mut u8,
    len: usize,
    _marker: PhantomData<&'a mut [u8]>,
}

impl<'a> PageGuard<'a> {
    /// 从 BufferManager 的 fetch 构造 PageGuard
    pub(crate) fn new(
        mgr: *mut BufferManager,
        block_id: BlockId,
        data_ptr: *mut u8,
        len: usize,
    ) -> Self {
        PageGuard {
            mgr,
            block_id,
            data_ptr,
            len,
            _marker: PhantomData,
        }
    }
}

unsafe impl<'a> Send for PageGuard<'a> {}
unsafe impl<'a> Sync for PageGuard<'a> {}

impl<'a> Deref for PageGuard<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.data_ptr, self.len) }
    }
}

impl<'a> DerefMut for PageGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr, self.len) }
    }
}

impl<'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        // 自动 unpin
        unsafe {
            if let Some(mgr) = self.mgr.as_mut() {
                mgr.unpin(self.block_id as u32);
            }
        }
    }
}
