use pgrx::prelude::*;
use zerocopy::{Immutable, IntoBytes, KnownLayout, PointerMetadata, TryFromBytes};

#[derive(Debug)]
pub struct BlockBuffer {
    buffer: pg_sys::Buffer,
    page: pg_sys::Page,
    wal: Option<GenericWAL>,
}

pub const SPECIAL_SIZE: usize = align_down(
    (pg_sys::BLCKSZ as usize)
        - std::mem::size_of::<pg_sys::PageHeaderData>()
        - std::mem::size_of::<usize>(),
    std::mem::size_of::<usize>(),
);

const fn align_down(val: usize, align: usize) -> usize {
    val & !(align - 1)
}

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

    pub fn block_number(&self) -> u32 {
        unsafe { pg_sys::BufferGetBlockNumber(self.buffer) }
    }

    pub fn as_struct<'a, T>(&'a self, offset: usize) -> anyhow::Result<&'a T>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let struct_size = std::mem::size_of::<T>();
        self.validate_bounds(offset, struct_size)?;

        // SAFETY: We validated that the requested range fits within the page's
        // special area. `try_ref_from_bytes` will check alignment and validity.
        let start = unsafe { pg_sys::PageGetSpecialPointer(self.page) as *const u8 };
        let bytes: &'a [u8] = unsafe { std::slice::from_raw_parts(start.add(offset), struct_size) };

        T::try_ref_from_bytes(bytes).map_err(|e| anyhow::Error::msg(e.to_string()))
    }

    pub fn as_struct_mut<'a, T>(&'a mut self, offset: usize) -> anyhow::Result<&'a mut T>
    where
        T: TryFromBytes + IntoBytes + KnownLayout,
    {
        let struct_size = std::mem::size_of::<T>();
        self.validate_bounds(offset, struct_size)?;

        // SAFETY: We validated that the requested range fits within the page's
        // special area. `try_mut_from_bytes` will check alignment and validity.
        let start = unsafe { pg_sys::PageGetSpecialPointer(self.page) as *mut u8 };
        let bytes: &'a mut [u8] =
            unsafe { std::slice::from_raw_parts_mut(start.add(offset), struct_size) };

        T::try_mut_from_bytes(bytes).map_err(|e| anyhow::Error::msg(e.to_string()))
    }

    pub fn as_struct_with_elems<'a, T>(
        &'a self,
        offset: usize,
        elems: usize,
    ) -> anyhow::Result<&'a T>
    where
        T: TryFromBytes + KnownLayout<PointerMetadata = usize> + Immutable + ?Sized,
    {
        let required = self.required_size::<T>(elems)?;
        self.validate_bounds(offset, required)?;
        let start = unsafe { pg_sys::PageGetSpecialPointer(self.page) as *const u8 };
        let bytes: &'a [u8] = unsafe { std::slice::from_raw_parts(start.add(offset), required) };
        T::try_ref_from_bytes_with_elems(bytes, elems)
            .map_err(|e| anyhow::Error::msg(e.to_string()))
    }

    pub fn as_struct_with_elems_mut<'a, T>(
        &'a mut self,
        offset: usize,
        elems: usize,
    ) -> anyhow::Result<&'a mut T>
    where
        T: TryFromBytes + IntoBytes + KnownLayout<PointerMetadata = usize> + ?Sized,
    {
        let required = self.required_size::<T>(elems)?;
        self.validate_bounds(offset, required)?;
        let start = unsafe { pg_sys::PageGetSpecialPointer(self.page) as *mut u8 };
        let bytes: &'a mut [u8] =
            unsafe { std::slice::from_raw_parts_mut(start.add(offset), required) };
        T::try_mut_from_bytes_with_elems(bytes, elems)
            .map_err(|e| anyhow::Error::msg(e.to_string()))
    }

    fn validate_bounds(&self, offset: usize, size: usize) -> anyhow::Result<()> {
        let end = offset
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("Offset overflow"))?;

        if end > SPECIAL_SIZE {
            anyhow::bail!("Invalid offset. Out of bounds access");
        }

        Ok(())
    }

    fn required_size<T>(&self, elems: usize) -> anyhow::Result<usize>
    where
        T: KnownLayout<PointerMetadata = usize> + ?Sized,
    {
        let meta = T::PointerMetadata::from_elem_count(elems);
        T::size_for_metadata(meta).ok_or_else(|| anyhow::anyhow!("Requested size would overflow"))
    }

    pub unsafe fn as_ptr_mut(&mut self) -> *mut i8 {
        unsafe { pg_sys::PageGetSpecialPointer(self.page) }
    }

    pub unsafe fn as_ptr(&self) -> *const i8 {
        unsafe { pg_sys::PageGetSpecialPointer(self.page) }
    }
}

impl Drop for BlockBuffer {
    fn drop(&mut self) {
        // Ensure generic WAL finishes before we release the buffer.
        if self.wal.is_some() {
            _ = self.wal.take();
            unsafe {
                pg_sys::MarkBufferDirty(self.buffer);
            }
        }
        unsafe {
            pg_sys::UnlockReleaseBuffer(self.buffer);
        }
    }
}

impl AsRef<[u8]> for BlockBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            let p = self.as_ptr();
            std::slice::from_raw_parts(p as *const u8, SPECIAL_SIZE)
        }
    }
}

impl AsMut<[u8]> for BlockBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe {
            let p = self.as_ptr_mut();
            std::slice::from_raw_parts_mut(p as *mut u8, SPECIAL_SIZE)
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
    use pgrx::{spi, Spi};

    #[pg_test]
    pub fn test_sanity() -> spi::Result<()> {
        let sql = "
            -- 1. Create table
            CREATE TABLE documents (id SERIAL PRIMARY KEY, text TEXT NOT NULL);
        ";
        Spi::run(sql)?;

        let table = "public.documents";
        let relation = unsafe { pgrx::PgRelation::open_with_name(&table).expect("table exists") };
        let blkno = {
            let mut buff = BlockBuffer::allocate(relation.as_ptr());
            let block = buff.block_number();
            let s = CString::new("hello").expect("string made");
            unsafe {
                std::ptr::copy(s.as_ptr(), buff.as_ptr_mut(), s.count_bytes());
            }
            block
        };

        {
            let mut buff = BlockBuffer::acquire(relation.as_ptr(), blkno);
            let h = unsafe { CString::from_raw(buff.as_ptr_mut()) };
            info!("CString {h:?}");
            _ = h.into_raw();
        }

        Ok(())
    }
}
