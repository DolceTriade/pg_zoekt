use pgrx::prelude::*;

enum Mode {
    RO,
    RW,
}
pub struct BlockBuffer {
    mode: Mode,
    buffer: pg_sys::Buffer,
    page: pg_sys::Page,
    wal: Option<GenericWAL>,
}

const SPECIAL_SIZE: usize = pg_sys::BLCKSZ as usize - std::mem::size_of::<pg_sys::PageHeaderData>();

impl BlockBuffer {
    pub fn acquire(rel: pg_sys::Relation, num: u32) -> Self {
        let buffer = unsafe { pg_sys::ReadBuffer(rel, num) };
        unsafe {
            pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_SHARE as i32);
        }
        let page = unsafe { pg_sys::BufferGetPage(buffer) };
        Self {
            mode: Mode::RO,
            buffer,
            page,
            wal: None,
        }
    }

    pub fn aquire_mut(rel: pg_sys::Relation, num: u32) -> Self {
        let buffer = unsafe { pg_sys::ReadBuffer(rel, num) };
        unsafe {
            pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);
        }
        let wal = GenericWAL::new(rel);
        let page = wal.track(buffer, false);
        Self {
            mode: Mode::RW,
            buffer,
            page,
            wal: Some(wal),
        }
    }

    pub fn allocate(rel: pg_sys::Relation) -> Self {
        let buffer = unsafe { pg_sys::ReadBuffer(rel, pg_sys::InvalidBlockNumber) };
        unsafe {
            pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);
        }
        let wal = GenericWAL::new(rel);
        let page = unsafe {
            let page = wal.track(buffer, true);
            pg_sys::PageInit(page, pg_sys::BLCKSZ as usize, SPECIAL_SIZE);
            page
        };
        Self {
            mode: Mode::RW,
            buffer,
            page,
            wal: Some(wal),
        }
    }

    pub fn page_header(self) -> &'static pg_sys::PageHeaderData {
        unsafe { &*(self.page as *const pg_sys::PageHeaderData) }
    }

    pub fn as_struct<T>(self, offset: usize) -> anyhow::Result<&'static T> {
        if offset + std::mem::size_of::<T>() + std::mem::size_of::<pg_sys::PageHeaderData>()
            > pg_sys::BLCKSZ as usize
        {
            anyhow::bail!("Invalid offset. Out of bounds access");
        }

        let struct_ptr = unsafe { self.page.add(offset) };

        Ok(unsafe { &*(struct_ptr as *const T) })
    }
}

impl Drop for BlockBuffer {
    fn drop(&mut self) {
        unsafe {
            pg_sys::UnlockReleaseBuffer(self.buffer);
        }
    }
}

struct GenericWAL {
    state: Option<*mut pg_sys::GenericXLogState>,
}

impl GenericWAL {
    pub fn new(rel: pg_sys::Relation) -> Self {
        Self {
            state: Some(unsafe { pg_sys::GenericXLogStart(rel) }),
        }
    }

    pub fn track(&self, buffer: pg_sys::Buffer, new_page: bool) -> pg_sys::Page {
        let mut flags = 0_i32;
        new_page.then(|| flags |= pg_sys::GENERIC_XLOG_FULL_IMAGE as i32);
        unsafe {
            pg_sys::GenericXLogRegisterBuffer(
                self.state.expect("expected GenericXLog state"),
                buffer,
                flags,
            )
        }
    }
}

impl Drop for GenericWAL {
    fn drop(&mut self) {
        if let Some(state) = self.state
            && !state.is_null()
        {
            unsafe {
                _ = pg_sys::GenericXLogFinish(state);
            }
            _ = self.state.take();
        }
    }
}
