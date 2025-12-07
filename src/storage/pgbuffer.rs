use pgrx::prelude::*;

#[derive(Debug)]
pub struct BlockBuffer {
    buffer: pg_sys::Buffer,
    page: pg_sys::Page,
    wal: Option<GenericWAL>,
}

const SPECIAL_SIZE: usize = 4096;

impl BlockBuffer {
    pub fn acquire(rel: pg_sys::Relation, num: u32) -> Self {
        let buffer = unsafe { pg_sys::ReadBuffer(rel, num) };
        unsafe {
            pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_SHARE as i32);
        }
        let page = unsafe { pg_sys::BufferGetPage(buffer) };
        Self {
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

    pub unsafe fn as_ptr(&self) -> *mut i8 {
        unsafe { pg_sys::PageGetSpecialPointer(self.page) }
    }
}

impl Drop for BlockBuffer {
    fn drop(&mut self) {
        // Ensure generic WAL finishes before we release the buffer.
        _ = self.wal.take();
        unsafe {
            pg_sys::UnlockReleaseBuffer(self.buffer);
        }
    }
}

#[derive(Debug)]
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::ffi::CString;

    use super::*;
    use pgrx::prelude::*;

    #[pg_test]
    pub fn test_sanity() -> spi::Result<()> {
        let sql = "
            -- 1. Create table
            CREATE TABLE documents (id SERIAL PRIMARY KEY, text TEXT NOT NULL);
        ";
        Spi::run(sql)?;

        let table = "public.documents";
        let relation = unsafe { pgrx::PgRelation::open_with_name(&table).expect("table exists") };
        let mut blkno = 0;
        {
            let buff = BlockBuffer::allocate(relation.as_ptr());
            blkno = unsafe { pg_sys::BufferGetBlockNumber(buff.buffer) };
            let s = CString::new("hello").expect("string made");
            unsafe {
                std::ptr::copy(s.as_ptr(), buff.as_ptr(), s.count_bytes());
            }
        }

        {
            let buff = BlockBuffer::acquire(relation.as_ptr(), blkno);
            let h = unsafe { CString::from_raw(buff.as_ptr()) };
            info!("CString {h:?}");
            _ = h.into_raw();
        }

        Ok(())
    }
}
