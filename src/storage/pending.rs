use anyhow::{Context, Result, bail};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::HashSet;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use super::{
    ItemPointer, PENDING_BUCKET_MAGIC, PENDING_MAGIC, RootBlockList, pgbuffer::BlockBuffer,
};

const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PendingBucket>();
const ENTRY_SIZE: usize = std::mem::size_of::<PendingEntry>();
const INVALID_BLOCK: u32 = pg_sys::InvalidBlockNumber;

#[derive(Debug, Clone, Copy, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct PendingHeader {
    magic: u32,
    bytes_used: u32,
    head_block: u32,
    tail_block: u32,
    free_head: u32,
}

#[derive(Debug, Clone, Copy, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct PendingBucket {
    magic: u16,
    free: u16,
    next_block: u32,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
struct PendingEntry {
    block: u32,
    offset: u16,
    _pad: u16,
}

fn page_capacity() -> usize {
    super::pgbuffer::SPECIAL_SIZE - PAGE_HEADER_SIZE
}

fn read_struct<T: Copy>(bytes: &[u8]) -> T {
    debug_assert!(bytes.len() >= std::mem::size_of::<T>());
    unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
}

fn write_struct<T: Copy>(bytes: &mut [u8], value: T) {
    debug_assert!(bytes.len() >= std::mem::size_of::<T>());
    unsafe {
        std::ptr::write_unaligned(bytes.as_mut_ptr() as *mut T, value);
    }
}

pub fn init_pending(rel: pg_sys::Relation, header_block: u32) -> Result<()> {
    let mut header_buf =
        BlockBuffer::aquire_mut(rel, header_block).context("init_pending: acquire header block")?;
    let header = header_buf
        .as_struct_mut::<PendingHeader>(0)
        .context("pending header")?;
    header.magic = PENDING_MAGIC;
    header.bytes_used = 0;
    header.head_block = INVALID_BLOCK;
    header.tail_block = INVALID_BLOCK;
    header.free_head = INVALID_BLOCK;
    Ok(())
}

fn allocate_page(rel: pg_sys::Relation, free_head: &mut u32) -> Result<u32> {
    if *free_head != INVALID_BLOCK {
        let nblocks = unsafe {
            pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM)
        };
        if *free_head >= nblocks {
            warning!(
                "pending free_head out of range: block={} nblocks={}",
                free_head,
                nblocks
            );
            *free_head = INVALID_BLOCK;
        }
    }
    if *free_head != INVALID_BLOCK {
        let block = *free_head;
        let mut page =
            BlockBuffer::aquire_mut(rel, block).context("allocate_page: acquire free block")?;
        let next = {
            let header = page
                .as_struct::<PendingBucket>(0)
                .context("pending page header")?;
            header.next_block
        };
        let header = page
            .as_struct_mut::<PendingBucket>(0)
            .context("pending page header")?;
        header.magic = PENDING_BUCKET_MAGIC;
        header.free = page_capacity() as u16;
        header.next_block = INVALID_BLOCK;
        *free_head = next;
        return Ok(block);
    }

    let mut page = super::allocate_block(rel);
    let header = page
        .as_struct_mut::<PendingBucket>(0)
        .context("pending page header")?;
    header.magic = PENDING_BUCKET_MAGIC;
    header.free = page_capacity() as u16;
    header.next_block = INVALID_BLOCK;
    Ok(page.block_number())
}

fn ensure_pending_list(rel: pg_sys::Relation, root_block: u32) -> Result<u32> {
    let mut root =
        BlockBuffer::aquire_mut(rel, root_block).context("ensure_pending_list: acquire root")?;
    let rbl = root
        .as_struct_mut::<RootBlockList>(0)
        .context("root header")?;
    if rbl.version >= super::VERSION && rbl.pending_block != INVALID_BLOCK {
        return Ok(rbl.pending_block);
    }

    let header_block = super::allocate_block(rel).block_number();
    init_pending(rel, header_block).context("ensure_pending_list: init header")?;
    rbl.pending_block = header_block;
    if rbl.version < super::VERSION {
        rbl.version = super::VERSION;
    }
    Ok(header_block)
}

pub fn append_tid(rel: pg_sys::Relation, root_block: u32, tid: ItemPointer) -> Result<()> {
    let header_block =
        ensure_pending_list(rel, root_block).context("append_tid: ensure pending list")?;
    let mut header_buf =
        BlockBuffer::aquire_mut(rel, header_block).context("append_tid: acquire header block")?;
    let header = header_buf
        .as_struct_mut::<PendingHeader>(0)
        .context("pending header")?;
    if header.magic != PENDING_MAGIC {
        warning!(
            "pending header magic mismatch: expected={} got={} header_block={}",
            PENDING_MAGIC,
            header.magic,
            header_block
        );
        bail!("invalid pending header magic");
    }

    let entry = PendingEntry {
        block: tid.block_number,
        offset: tid.offset,
        _pad: 0,
    };

    let tail_blk = header.tail_block;
    if tail_blk == INVALID_BLOCK {
        let new_block =
            allocate_page(rel, &mut header.free_head).context("append_tid: allocate first page")?;
        header.head_block = new_block;
        header.tail_block = new_block;
    }

    let tail_block = header.tail_block;
    let mut page =
        BlockBuffer::aquire_mut(rel, tail_block).context("append_tid: acquire tail page")?;
    let free = {
        let header = page
            .as_struct::<PendingBucket>(0)
            .context("pending page header")?;
        if header.magic != PENDING_BUCKET_MAGIC {
            warning!(
                "pending page magic mismatch: expected={} got={} block={}",
                PENDING_BUCKET_MAGIC,
                header.magic,
                tail_block
            );
            bail!("corrupt pending page");
        }
        header.free as usize
    };

    if free < ENTRY_SIZE {
        drop(page);
        let new_block =
            allocate_page(rel, &mut header.free_head).context("append_tid: allocate new page")?;
        let mut old_tail = BlockBuffer::aquire_mut(rel, header.tail_block)
            .context("append_tid: acquire old tail")?;
        let old_header = old_tail
            .as_struct_mut::<PendingBucket>(0)
            .context("pending page header")?;
        old_header.next_block = new_block;
        drop(old_tail);

        header.tail_block = new_block;
        page = BlockBuffer::aquire_mut(rel, header.tail_block)
            .context("append_tid: acquire new tail")?;
    }

    let used = {
        let header = page
            .as_struct::<PendingBucket>(0)
            .context("pending page header")?;
        page_capacity().saturating_sub(header.free as usize)
    };
    let offset = PAGE_HEADER_SIZE + used;
    let bytes = page.as_mut();
    let end = offset + ENTRY_SIZE;
    if end > bytes.len() {
        warning!(
            "pending page write overflow: block={} offset={} entry_size={} page_size={}",
            header.tail_block,
            offset,
            ENTRY_SIZE,
            bytes.len()
        );
        bail!("pending page write overflow");
    }
    write_struct(&mut bytes[offset..end], entry);

    let page_header = page
        .as_struct_mut::<PendingBucket>(0)
        .context("append_tid: pending page header")?;
    let needed_u16: u16 = ENTRY_SIZE
        .try_into()
        .map_err(|e| {
            warning!(
                "pending entry size overflow: entry_size={} err={e}",
                ENTRY_SIZE
            );
            e
        })
        .context("pending entry size overflow")?;
    page_header.free = match page_header.free.checked_sub(needed_u16) {
        Some(val) => val,
        None => {
            warning!(
                "pending page free underflow: block={} free={} needed={}",
                header.tail_block,
                page_header.free,
                needed_u16
            );
            return Err(anyhow::anyhow!("pending page free underflow"));
        }
    };
    header.bytes_used = header.bytes_used.saturating_add(ENTRY_SIZE as u32);

    Ok(())
}

pub fn detach_pending(rel: pg_sys::Relation, root_block: u32) -> Result<Option<u32>> {
    let header_block = ensure_pending_list(rel, root_block)?;
    let mut header_buf = BlockBuffer::aquire_mut(rel, header_block)?;
    let header = header_buf
        .as_struct_mut::<PendingHeader>(0)
        .context("pending header")?;
    if header.magic != PENDING_MAGIC {
        bail!("invalid pending header magic");
    }
    let head = header.head_block;
    if head == INVALID_BLOCK {
        return Ok(None);
    }

    header.head_block = INVALID_BLOCK;
    header.tail_block = INVALID_BLOCK;
    header.bytes_used = 0;
    Ok(Some(head))
}

pub fn collect_blocks(rel: pg_sys::Relation, head: u32) -> Result<Vec<u32>> {
    let mut blocks = Vec::new();
    let mut current_block = Some(head);
    while let Some(block) = current_block {
        let page = BlockBuffer::acquire(rel, block)?;
        let next = {
            let header = page
                .as_struct::<PendingBucket>(0)
                .context("pending page header")?;
            if header.magic != PENDING_BUCKET_MAGIC {
                bail!("corrupt pending page during block collection");
            }
            header.next_block
        };
        drop(page);
        blocks.push(block);
        current_block = if next == INVALID_BLOCK {
            None
        } else {
            Some(next)
        };
    }
    Ok(blocks)
}

pub fn collect_all_blocks(rel: pg_sys::Relation, header_block: u32) -> Result<Vec<u32>> {
    let page = BlockBuffer::acquire(rel, header_block)?;
    let header = page
        .as_struct::<PendingHeader>(0)
        .context("pending header")?;
    if header.magic != PENDING_MAGIC {
        bail!("invalid pending header magic");
    }
    let mut blocks = Vec::new();
    blocks.push(header_block);
    if header.head_block != INVALID_BLOCK {
        blocks.extend(collect_blocks(rel, header.head_block)?);
    }
    Ok(blocks)
}

pub fn drain_block_entries<F>(rel: pg_sys::Relation, block: u32, mut on_tid: F) -> Result<usize>
where
    F: FnMut(ItemPointer),
{
    let page = BlockBuffer::acquire(rel, block)?;
    let used = {
        let header = page
            .as_struct::<PendingBucket>(0)
            .context("pending page header")?;
        if header.magic != PENDING_BUCKET_MAGIC {
            bail!("corrupt pending page during drain");
        }
        page_capacity().saturating_sub(header.free as usize)
    };

    let mut offset = PAGE_HEADER_SIZE;
    let end = PAGE_HEADER_SIZE + used;
    let bytes = page.as_ref();
    let mut count = 0usize;
    while offset + ENTRY_SIZE <= end {
        let entry = read_struct::<PendingEntry>(&bytes[offset..offset + ENTRY_SIZE]);
        offset += ENTRY_SIZE;
        on_tid(ItemPointer {
            block_number: entry.block,
            offset: entry.offset,
        });
        count += 1;
    }
    Ok(count)
}

pub fn drain_detached<F>(rel: pg_sys::Relation, head: u32, mut on_tid: F) -> Result<usize>
where
    F: FnMut(ItemPointer),
{
    let mut current_block = Some(head);
    let mut freed_pages = Vec::new();
    let mut count = 0usize;

    while let Some(block) = current_block {
        let page = BlockBuffer::acquire(rel, block)?;
        let (next, used) = {
            let header = page
                .as_struct::<PendingBucket>(0)
                .context("pending page header")?;
            if header.magic != PENDING_BUCKET_MAGIC {
                bail!("corrupt pending page during drain");
            }
            let used = page_capacity().saturating_sub(header.free as usize);
            (header.next_block, used)
        };

        let mut offset = PAGE_HEADER_SIZE;
        let end = PAGE_HEADER_SIZE + used;
        let bytes = page.as_ref();
        while offset + ENTRY_SIZE <= end {
            let entry = read_struct::<PendingEntry>(&bytes[offset..offset + ENTRY_SIZE]);
            offset += ENTRY_SIZE;
            on_tid(ItemPointer {
                block_number: entry.block,
                offset: entry.offset,
            });
            count += 1;
        }
        drop(page);
        freed_pages.push(block);
        current_block = if next == INVALID_BLOCK {
            None
        } else {
            Some(next)
        };
    }

    // Return pages to free list.
    if !freed_pages.is_empty() {
        let root = BlockBuffer::acquire(rel, 0)?;
        let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
        if rbl.pending_block != INVALID_BLOCK {
            let mut header_buf = BlockBuffer::aquire_mut(rel, rbl.pending_block)?;
            let header = header_buf
                .as_struct_mut::<PendingHeader>(0)
                .context("pending header")?;
            for block in freed_pages {
                let mut page = BlockBuffer::aquire_mut(rel, block)?;
                let page_header = page
                    .as_struct_mut::<PendingBucket>(0)
                    .context("pending page header")?;
                page_header.magic = PENDING_BUCKET_MAGIC;
                page_header.free = page_capacity() as u16;
                page_header.next_block = header.free_head;
                header.free_head = block;
            }
        }
    }

    Ok(count)
}

pub fn free_blocks(rel: pg_sys::Relation, blocks: &[u32]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    let root = BlockBuffer::acquire(rel, 0)?;
    let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
    if rbl.pending_block == INVALID_BLOCK {
        return Ok(());
    }
    let mut header_buf = BlockBuffer::aquire_mut(rel, rbl.pending_block)?;
    let header = header_buf
        .as_struct_mut::<PendingHeader>(0)
        .context("pending header")?;
    for block in blocks {
        let mut page = BlockBuffer::aquire_mut(rel, *block)?;
        let page_header = page
            .as_struct_mut::<PendingBucket>(0)
            .context("pending page header")?;
        page_header.magic = PENDING_BUCKET_MAGIC;
        page_header.free = page_capacity() as u16;
        page_header.next_block = header.free_head;
        header.free_head = *block;
    }
    Ok(())
}

pub fn cleanup_free_list(rel: pg_sys::Relation) -> Result<()> {
    let root = BlockBuffer::acquire(rel, 0).context("cleanup_free_list: root")?;
    let rbl = root
        .as_struct::<RootBlockList>(0)
        .context("cleanup_free_list: root header")?;
    if rbl.pending_block == INVALID_BLOCK {
        return Ok(());
    }

    let nblocks =
        unsafe { pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) };

    let mut header_buf = BlockBuffer::aquire_mut(rel, rbl.pending_block)
        .context("cleanup_free_list: pending header block")?;
    let header = header_buf
        .as_struct_mut::<PendingHeader>(0)
        .context("cleanup_free_list: pending header")?;
    if header.magic != PENDING_MAGIC {
        warning!("cleanup_free_list: invalid pending header magic");
        header.free_head = INVALID_BLOCK;
        return Ok(());
    }

    let mut seen = HashSet::new();
    let mut valid = Vec::new();
    let mut block = header.free_head;
    while block != INVALID_BLOCK {
        if block >= nblocks {
            warning!(
                "cleanup_free_list: free block out of range: block={} nblocks={}",
                block,
                nblocks
            );
            break;
        }
        if !seen.insert(block) {
            warning!("cleanup_free_list: free list cycle at block {}", block);
            break;
        }
        let page = BlockBuffer::acquire(rel, block).context("cleanup_free_list: free block")?;
        let page_header = page
            .as_struct::<PendingBucket>(0)
            .context("cleanup_free_list: pending page header")?;
        if page_header.magic != PENDING_BUCKET_MAGIC {
            warning!(
                "cleanup_free_list: bad pending page magic at block {}",
                block
            );
            break;
        }
        valid.push(block);
        block = page_header.next_block;
    }

    let mut head = INVALID_BLOCK;
    for block in valid.into_iter().rev() {
        let mut page =
            BlockBuffer::aquire_mut(rel, block).context("cleanup_free_list: rebuild free block")?;
        let page_header = page
            .as_struct_mut::<PendingBucket>(0)
            .context("cleanup_free_list: rebuild header")?;
        page_header.magic = PENDING_BUCKET_MAGIC;
        page_header.free = page_capacity() as u16;
        page_header.next_block = head;
        head = block;
    }
    header.free_head = head;
    Ok(())
}

#[cfg(any(test, feature = "pg_test"))]
pub fn test_get_free_head(rel: pg_sys::Relation) -> Result<u32> {
    let root = BlockBuffer::acquire(rel, 0).context("test_get_free_head: root")?;
    let rbl = root
        .as_struct::<RootBlockList>(0)
        .context("test_get_free_head: root header")?;
    if rbl.pending_block == INVALID_BLOCK {
        return Ok(INVALID_BLOCK);
    }
    let header_buf = BlockBuffer::acquire(rel, rbl.pending_block)
        .context("test_get_free_head: pending header block")?;
    let header = header_buf
        .as_struct::<PendingHeader>(0)
        .context("test_get_free_head: pending header")?;
    Ok(header.free_head)
}

#[cfg(any(test, feature = "pg_test"))]
pub fn test_set_free_head(rel: pg_sys::Relation, value: u32) -> Result<()> {
    let root = BlockBuffer::acquire(rel, 0).context("test_set_free_head: root")?;
    let rbl = root
        .as_struct::<RootBlockList>(0)
        .context("test_set_free_head: root header")?;
    if rbl.pending_block == INVALID_BLOCK {
        bail!("pending header not initialized");
    }
    let mut header_buf = BlockBuffer::aquire_mut(rel, rbl.pending_block)
        .context("test_set_free_head: pending header block")?;
    let header = header_buf
        .as_struct_mut::<PendingHeader>(0)
        .context("test_set_free_head: pending header")?;
    header.free_head = value;
    Ok(())
}
