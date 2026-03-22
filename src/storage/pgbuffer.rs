use anyhow::{Result, anyhow};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys::PgTryBuilder;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use zerocopy::{Immutable, IntoBytes, KnownLayout, PointerMetadata, TryFromBytes};

static DEBUG_LEAK_LOCKED_BUFFERS: GucSetting<bool> = GucSetting::<bool>::new(false);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BufferState {
    Released,
    Pinned,
    Locked,
}

#[derive(Debug)]
pub struct BlockBuffer {
    buffer: pg_sys::Buffer,
    page: pg_sys::Page,
    wal: Option<GenericWAL>,
    state: BufferState,
}

pub const SPECIAL_SIZE: usize = align_down(
    (pg_sys::BLCKSZ as usize)
        - std::mem::size_of::<pg_sys::PageHeaderData>()
        - std::mem::size_of::<usize>(),
    std::mem::size_of::<usize>(),
);

struct RelationExtensionLockGuard {
    rel: pg_sys::Relation,
    lockmode: pg_sys::LOCKMODE,
}

impl RelationExtensionLockGuard {
    unsafe fn new(rel: pg_sys::Relation, lockmode: pg_sys::LOCKMODE) -> Self {
        unsafe { pg_sys::LockRelationForExtension(rel, lockmode) };
        Self { rel, lockmode }
    }
}

impl Drop for RelationExtensionLockGuard {
    fn drop(&mut self) {
        unsafe { pg_sys::UnlockRelationForExtension(self.rel, self.lockmode) };
    }
}

const fn align_down(val: usize, align: usize) -> usize {
    val & !(align - 1)
}

pub(crate) fn init() {
    GucRegistry::define_bool_guc(
        c"pg_zoekt.debug_leak_locked_buffers",
        c"Skip releasing locked BlockBuffer handles in Drop.",
        c"Testing escape hatch. When enabled, pg_zoekt intentionally leaks locked buffers instead of calling UnlockReleaseBuffer() from BlockBuffer::drop(). This avoids backend aborts from double unlocks but can leave pins/locks held until backend exit.",
        &DEBUG_LEAK_LOCKED_BUFFERS,
        GucContext::Userset,
        GucFlags::default(),
    );
}

fn ensure_block_in_range(rel: pg_sys::Relation, num: u32) -> Result<()> {
    if rel.is_null() {
        return Err(anyhow!("attempted to read buffer with null relation"));
    }
    if num == pg_sys::InvalidBlockNumber {
        return Err(anyhow!("attempted to read invalid block number"));
    }
    let nblocks =
        unsafe { pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) };
    if num >= nblocks {
        return Err(anyhow!(
            "block number out of range: {} (nblocks={})",
            num,
            nblocks
        ));
    }
    Ok(())
}

fn read_buffer(rel: pg_sys::Relation, num: u32) -> Result<pg_sys::Buffer> {
    PgTryBuilder::new(|| Ok(unsafe { pg_sys::ReadBuffer(rel, num) }))
        .catch_others(|error: CaughtError| {
            let relid = if rel.is_null() {
                0
            } else {
                unsafe { u32::from((*rel).rd_id) }
            };
            let nblocks = if rel.is_null() {
                0
            } else {
                unsafe {
                    pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM)
                }
            };
            match error {
                CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => Err(
                    anyhow!(
                        "ReadBuffer failed for rel {} block {} (nblocks={}): {} detail={:?} hint={:?} sqlstate={} source={}:{}",
                        relid,
                        num,
                        nblocks,
                        report.message(),
                        report.detail(),
                        report.hint(),
                        report.sql_error_code(),
                        report.file(),
                        report.line_number(),
                    ),
                ),
                CaughtError::RustPanic { ereport, .. } => Err(anyhow!(
                    "ReadBuffer failed for rel {} block {} (nblocks={}): {} detail={:?} hint={:?} sqlstate={} source={}:{}",
                    relid,
                    num,
                    nblocks,
                    ereport.message(),
                    ereport.detail(),
                    ereport.hint(),
                    ereport.sql_error_code(),
                    ereport.file(),
                    ereport.line_number(),
                )),
            }
        })
        .catch_rust_panic(|error: CaughtError| {
            let relid = if rel.is_null() {
                0
            } else {
                unsafe { u32::from((*rel).rd_id) }
            };
            let nblocks = if rel.is_null() {
                0
            } else {
                unsafe {
                    pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM)
                }
            };
            Err(anyhow!(
                "ReadBuffer panicked for rel {} block {} (nblocks={}): {:?}",
                relid,
                num,
                nblocks,
                error
            ))
        })
        .execute()
}

impl BlockBuffer {
    pub fn acquire(rel: pg_sys::Relation, num: u32) -> Result<Self> {
        ensure_block_in_range(rel, num)?;
        let buffer = read_buffer(rel, num)?;
        let page = unsafe { pg_sys::BufferGetPage(buffer) };
        Ok(Self {
            buffer,
            page,
            wal: None,
            state: BufferState::Pinned,
        })
    }

    pub fn aquire_mut(rel: pg_sys::Relation, num: u32) -> Result<Self> {
        ensure_block_in_range(rel, num)?;
        let buffer = read_buffer(rel, num)?;
        unsafe {
            pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);
        }
        let wal = GenericWAL::new(rel);
        let page = wal.track(buffer, false);
        Ok(Self {
            buffer,
            page,
            wal: Some(wal),
            state: BufferState::Locked,
        })
    }

    pub fn allocate(rel: pg_sys::Relation) -> Self {
        // TODO(pg18): evaluate whether ExtendBufferedRel can replace this path
        // without changing extension-locking or Generic WAL semantics.
        let lock = unsafe {
            RelationExtensionLockGuard::new(rel, pg_sys::ExclusiveLock as pg_sys::LOCKMODE)
        };
        let buffer = unsafe { pg_sys::ReadBuffer(rel, pg_sys::InvalidBlockNumber) };
        drop(lock);
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
            state: BufferState::Locked,
        }
    }

    pub fn block_number(&self) -> u32 {
        unsafe { pg_sys::BufferGetBlockNumber(self.buffer) }
    }

    #[allow(dead_code)]
    pub fn release(mut self) {
        self.release_internal();
    }

    #[allow(dead_code)]
    pub fn unlock_release(mut self) {
        self.release_internal();
    }

    pub fn close(mut self) {
        self.release_internal();
    }

    pub fn abandon(mut self) {
        self.disarm();
    }

    fn release_internal(&mut self) {
        if matches!(self.state, BufferState::Released) {
            return;
        }
        if self.finish_wal_if_needed().is_err() {
            return;
        }
        if matches!(self.state, BufferState::Locked) && DEBUG_LEAK_LOCKED_BUFFERS.get() {
            warning!(
                "pg_zoekt.debug_leak_locked_buffers is enabled; leaking locked buffer {} to avoid UnlockReleaseBuffer()",
                self.buffer
            );
            self.disarm();
            return;
        }
        unsafe {
            if pg_sys::BufferIsValid(self.buffer) {
                match self.state {
                    BufferState::Pinned => pg_sys::ReleaseBuffer(self.buffer),
                    BufferState::Locked => pg_sys::UnlockReleaseBuffer(self.buffer),
                    BufferState::Released => {}
                }
            }
        }
        self.disarm();
    }

    fn finish_wal_if_needed(&mut self) -> Result<()> {
        if self.wal.is_some() {
            _ = self.wal.take();
            unsafe {
                pg_sys::MarkBufferDirty(self.buffer);
            }
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.state = BufferState::Released;
        self.buffer = pg_sys::InvalidBuffer as _;
        self.page = std::ptr::null_mut();
        self.wal = None;
    }

    pub fn init_page(&mut self) {
        unsafe {
            pg_sys::PageInit(self.page, pg_sys::BLCKSZ as usize, SPECIAL_SIZE);
        }
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
    use std::ffi::{CStr, CString};

    use super::*;
    use pgrx::{Spi, spi};

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
                let bytes = s.as_bytes_with_nul();
                std::ptr::copy(bytes.as_ptr().cast(), buff.as_ptr_mut(), bytes.len());
            }
            buff.close();
            block
        };

        {
            let buff = BlockBuffer::acquire(relation.as_ptr(), blkno).expect("acquire buffer");
            let cstr = unsafe { CStr::from_ptr(buff.as_ptr()) };
            assert_eq!(
                cstr.to_str().expect("valid utf8"),
                "hello",
                "expected string contents"
            );
            buff.close();
        }

        Ok(())
    }

    #[pg_test]
    pub fn test_acquire_does_not_hold_buffer_lock() -> spi::Result<()> {
        let sql = "
            CREATE TABLE lock_probe (id SERIAL PRIMARY KEY, text TEXT NOT NULL);
        ";
        Spi::run(sql)?;

        let table = "public.lock_probe";
        let relation = unsafe { pgrx::PgRelation::open_with_name(&table).expect("table exists") };
        let blkno = {
            let mut buff = BlockBuffer::allocate(relation.as_ptr());
            let block = buff.block_number();
            let s = CString::new("probe").expect("string made");
            unsafe {
                std::ptr::copy(s.as_ptr(), buff.as_ptr_mut(), s.count_bytes());
            }
            buff.close();
            block
        };

        let buff = BlockBuffer::acquire(relation.as_ptr(), blkno).expect("acquire buffer");
        let buffer_id = buff.buffer;
        unsafe {
            // If acquire left a share lock, conditional exclusive lock would fail.
            assert!(pg_sys::ConditionalLockBuffer(buffer_id));
            pg_sys::LockBuffer(buffer_id, pg_sys::BUFFER_LOCK_UNLOCK as i32);
        }
        buff.close();

        Ok(())
    }
}
