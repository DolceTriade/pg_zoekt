use anyhow::{Context, Result};
use pgrx::prelude::*;
/// Storing stuff
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::io::Write;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

pub const TARGET_SEGMENTS: usize = 10;

pub mod decode;
pub mod encode;
mod parallel_merge;
pub mod pending;
pub mod pgbuffer;
pub mod tombstone;

pub const VERSION: u16 = 4;
pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");
pub const WAL_MAGIC: u32 = u32::from_ne_bytes(*b"wZKT");
pub const PENDING_MAGIC: u32 = u32::from_ne_bytes(*b"pPLD");
pub const PENDING_BUCKET_MAGIC: u16 = u16::from_ne_bytes(*b"PL");
pub const POSTING_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"oZKT");
pub const SEGMENT_LIST_MAGIC: u32 = u32::from_ne_bytes(*b"lZKT");
pub const TOMBSTONE_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"tZKT");
pub const FREE_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"fZKT");

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct RootBlockList {
    pub magic: u32,
    pub version: u16,
    pub wal_block: u32,
    pub num_segments: u32,
    pub segment_list_head: u32,
    pub segment_list_tail: u32,
    pub tombstone_block: u32,
    pub tombstone_bytes: u32,
    pub pending_block: u32,
    // Segments...
}

#[derive(Copy, Clone, Debug)]
pub enum MaintenanceLockMode {
    Try,
    Block,
}

pub struct MaintenanceLockGuard {
    locktag: pg_sys::LOCKTAG,
    lockmode: pg_sys::LOCKMODE,
    acquired: bool,
}

impl Drop for MaintenanceLockGuard {
    fn drop(&mut self) {
        if !self.acquired {
            return;
        }
        unsafe {
            pg_sys::LockRelease(&self.locktag, self.lockmode, false);
        }
    }
}

fn maintenance_locktag(rel: pg_sys::Relation) -> Option<pg_sys::LOCKTAG> {
    if rel.is_null() {
        return None;
    }
    let relid = u64::from(u32::from(unsafe { (*rel).rd_id }));
    let dbid = u32::from(unsafe { pg_sys::MyDatabaseId });
    let key = (0x5A4B54u64 << 32) | (relid & 0xffff_ffff);
    let key1 = (key >> 32) as u32;
    let key2 = (key & 0xffff_ffff) as u32;
    Some(pg_sys::LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: key1,
        locktag_field3: key2,
        locktag_field4: 1,
        locktag_type: pg_sys::LockTagType::LOCKTAG_ADVISORY as u8,
        locktag_lockmethodid: pg_sys::DEFAULT_LOCKMETHOD as u8,
    })
}

pub fn maintenance_lock(
    rel: pg_sys::Relation,
    mode: MaintenanceLockMode,
) -> Option<MaintenanceLockGuard> {
    let locktag = maintenance_locktag(rel)?;
    let lockmode = pg_sys::ExclusiveLock as pg_sys::LOCKMODE;
    let acquired = unsafe {
        let dont_wait = matches!(mode, MaintenanceLockMode::Try);
        match pg_sys::LockAcquire(&locktag, lockmode, false, dont_wait) {
            pg_sys::LockAcquireResult::LOCKACQUIRE_OK
            | pg_sys::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD => true,
            _ => false,
        }
    };
    if !acquired {
        return None;
    }
    Some(MaintenanceLockGuard {
        locktag,
        lockmode,
        acquired: true,
    })
}

pub fn maintenance_lock_blocking(rel: pg_sys::Relation) -> Option<MaintenanceLockGuard> {
    maintenance_lock(rel, MaintenanceLockMode::Block)
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct RootBlockListV1 {
    pub magic: u32,
    pub version: u16,
    pub wal_block: u32,
    pub num_segments: u32,
    // Segments...
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    PartialOrd,
    Ord,
    TryFromBytes,
    IntoBytes,
    KnownLayout,
    Unaligned,
    Immutable,
)]
#[repr(C, packed)]
pub struct ItemPointer {
    pub block_number: pgrx::pg_sys::BlockNumber,
    pub offset: pgrx::pg_sys::OffsetNumber,
}

impl TryFrom<pg_sys::ItemPointer> for ItemPointer {
    type Error = anyhow::Error;

    fn try_from(value: pg_sys::ItemPointer) -> anyhow::Result<Self> {
        if value.is_null() {
            anyhow::bail!("ItemPointer is null!");
        }
        let blk =
            unsafe { ((*value).ip_blkid.bi_hi as u32) << 16 | (*value).ip_blkid.bi_lo as u32 };
        let off = unsafe { (*value).ip_posid };
        Ok(Self {
            block_number: blk,
            offset: off,
        })
    }
}

impl From<pg_sys::ItemPointerData> for ItemPointer {
    fn from(value: pg_sys::ItemPointerData) -> Self {
        let blk = ((value.ip_blkid.bi_hi as u32) << 16) | (value.ip_blkid.bi_lo as u32);
        Self {
            block_number: blk,
            offset: value.ip_posid,
        }
    }
}

#[derive(
    Debug, FromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy, PartialEq, Eq,
)]
#[repr(C, packed)]
pub struct Segment {
    pub block: u32,
    pub size: u64,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct SegmentListPageHeader {
    pub magic: u32,
    pub next_block: u32,
    pub count: u16,
}

const fn segment_list_capacity() -> usize {
    let header = std::mem::size_of::<SegmentListPageHeader>();
    let seg = std::mem::size_of::<Segment>();
    (pgbuffer::SPECIAL_SIZE - header) / seg
}

fn segment_list_init_page(rel: pg_sys::Relation) -> Result<pgbuffer::BlockBuffer> {
    let mut page = allocate_block(rel);
    let hdr = page
        .as_struct_mut::<SegmentListPageHeader>(0)
        .context("segment list header")?;
    hdr.magic = SEGMENT_LIST_MAGIC;
    hdr.next_block = pg_sys::InvalidBlockNumber;
    hdr.count = 0;
    Ok(page)
}

pub fn segment_list_append(
    rel: pg_sys::Relation,
    root: &mut RootBlockList,
    segments: &[Segment],
) -> Result<()> {
    if segments.is_empty() {
        return Ok(());
    }
    let cap = segment_list_capacity();
    if cap == 0 {
        anyhow::bail!("segment list page capacity is 0");
    }

    // Ensure we have a head/tail page.
    if root.segment_list_head == pg_sys::InvalidBlockNumber {
        let page = segment_list_init_page(rel)?;
        let blk = page.block_number();
        root.segment_list_head = blk;
        root.segment_list_tail = blk;
        drop(page);
    }

    let mut remaining = segments;
    while !remaining.is_empty() {
        let mut tail = pgbuffer::BlockBuffer::aquire_mut(rel, root.segment_list_tail)?;
        let (used, next_block) = {
            let hdr = tail
                .as_struct_mut::<SegmentListPageHeader>(0)
                .context("segment list header")?;
            if hdr.magic != SEGMENT_LIST_MAGIC {
                anyhow::bail!("bad segment list magic");
            }
            (hdr.count as usize, hdr.next_block)
        };
        let avail = cap.saturating_sub(used);
        if avail == 0 {
            // Allocate next page and link it.
            let next_page = segment_list_init_page(rel)?;
            let next_blk = next_page.block_number();
            {
                let hdr = tail
                    .as_struct_mut::<SegmentListPageHeader>(0)
                    .context("segment list header")?;
                hdr.next_block = next_blk;
            }
            root.segment_list_tail = next_blk;
            drop(next_page);
            continue;
        }

        let take = avail.min(remaining.len());
        let header_size = std::mem::size_of::<SegmentListPageHeader>();
        let seg_size = std::mem::size_of::<Segment>();
        let start_off = header_size + used * seg_size;
        let bytes = unsafe {
            let p = tail.as_ptr_mut().add(start_off) as *mut u8;
            std::slice::from_raw_parts_mut(p, take * seg_size)
        };
        // SAFETY: `Segment` is plain old data and packed; we just copy bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                remaining.as_ptr() as *const u8,
                bytes.as_mut_ptr(),
                take * seg_size,
            );
        }
        {
            let hdr = tail
                .as_struct_mut::<SegmentListPageHeader>(0)
                .context("segment list header")?;
            hdr.count = (used + take) as u16;
            hdr.next_block = next_block;
        }
        root.num_segments = root
            .num_segments
            .checked_add(take as u32)
            .expect("segment count overflow");

        remaining = &remaining[take..];
    }
    Ok(())
}

pub fn segment_list_read(rel: pg_sys::Relation, root: &RootBlockList) -> Result<Vec<Segment>> {
    if root.num_segments == 0 || root.segment_list_head == pg_sys::InvalidBlockNumber {
        return Ok(Vec::new());
    }
    let cap = segment_list_capacity();
    let mut out = Vec::with_capacity(root.num_segments as usize);
    let mut blk = root.segment_list_head;
    while blk != pg_sys::InvalidBlockNumber && out.len() < root.num_segments as usize {
        let buf = pgbuffer::BlockBuffer::acquire(rel, blk)?;
        let hdr = buf
            .as_struct::<SegmentListPageHeader>(0)
            .context("segment list header")?;
        if hdr.magic != SEGMENT_LIST_MAGIC {
            anyhow::bail!("bad segment list magic");
        }
        let count = (hdr.count as usize).min(cap);
        let list = buf
            .as_struct_with_elems::<Segments>(std::mem::size_of::<SegmentListPageHeader>(), count)
            .context("segment list entries")?;
        out.extend_from_slice(&list.entries[..count]);
        blk = hdr.next_block;
    }
    out.truncate(root.num_segments as usize);
    Ok(out)
}

fn collect_segment_list_pages(rel: pg_sys::Relation, head: u32) -> Result<Vec<u32>> {
    let mut pages = Vec::new();
    let mut blk = head;
    while blk != pg_sys::InvalidBlockNumber {
        pages.push(blk);
        let buf = pgbuffer::BlockBuffer::acquire(rel, blk)?;
        let hdr = buf
            .as_struct::<SegmentListPageHeader>(0)
            .context("segment list header")?;
        if hdr.magic != SEGMENT_LIST_MAGIC {
            anyhow::bail!("bad segment list magic");
        }
        blk = hdr.next_block;
    }
    Ok(pages)
}

pub fn segment_list_rewrite(
    rel: pg_sys::Relation,
    root: &mut RootBlockList,
    segments: &[Segment],
) -> Result<()> {
    let old_head = root.segment_list_head;
    root.num_segments = 0;
    root.segment_list_head = pg_sys::InvalidBlockNumber;
    root.segment_list_tail = pg_sys::InvalidBlockNumber;
    segment_list_append(rel, root, segments)?;
    if old_head != pg_sys::InvalidBlockNumber {
        let old_pages = collect_segment_list_pages(rel, old_head)?;
        free_blocks(rel, &old_pages)?;
    }
    Ok(())
}

pub fn reloption_parallel_workers(index_relation: pg_sys::Relation) -> usize {
    if index_relation.is_null() {
        return 0;
    }
    let opts = unsafe { (*index_relation).rd_options as *const pg_sys::StdRdOptions };
    if opts.is_null() {
        return 0;
    }
    let workers = unsafe { (*opts).parallel_workers };
    if workers > 0 { workers as usize } else { 0 }
}

pub fn merge_with_workers(
    rel: pg_sys::Relation,
    segments: &[Segment],
    target_segments: usize,
    flush_threshold: usize,
    tombstones: &tombstone::Snapshot,
    workers: Option<usize>,
) -> Result<Vec<Segment>> {
    let workers = workers.unwrap_or_else(|| reloption_parallel_workers(rel));
    let target_segments = target_segments.max(1);
    if segments.len() <= target_segments {
        return Ok(segments.to_vec());
    }
    info!(
        "merge_with_workers: segments={} target_segments={} workers={}",
        segments.len(),
        target_segments,
        workers
    );

    // Partition segments into `target_segments` groups by total size.
    let mut sorted = segments.to_vec();
    sorted.sort_by_key(|seg| std::cmp::Reverse(seg.size));
    let mut groups: Vec<(u64, Vec<Segment>)> = (0..target_segments)
        .map(|_| (0u64, Vec::new()))
        .collect();
    for seg in sorted {
        if let Some((total, bucket)) = groups.iter_mut().min_by_key(|g| g.0) {
            *total = total.saturating_add(seg.size);
            bucket.push(seg);
        }
    }
    let mut flat = Vec::new();
    let mut offsets = Vec::with_capacity(groups.len() + 1);
    offsets.push(0u32);
    for (_, group) in groups.iter() {
        flat.extend_from_slice(group);
        offsets.push(flat.len() as u32);
    }
    info!(
        "merge_with_workers: group_count={} total_input_segments={}",
        offsets.len().saturating_sub(1),
        flat.len()
    );

    if workers > 1 {
        if let Some(merged) = unsafe {
            parallel_merge::merge_parallel(
                rel,
                &flat,
                &offsets,
                flush_threshold,
                tombstones.is_empty(),
                workers,
            )
        } {
            return Ok(merged);
        }
    }

    let mut merged = Vec::new();
    for window in offsets.windows(2) {
        let start = window[0] as usize;
        let end = window[1] as usize;
        if start >= end {
            continue;
        }
        let segs = &flat[start..end];
        merged.push(merge(rel, segs, flush_threshold, tombstones)?);
    }
    Ok(merged)
}

#[cfg(feature = "pg_test")]
pub fn test_parallel_merge_reset() {
    parallel_merge::test_parallel_merge_reset();
}

#[cfg(feature = "pg_test")]
pub fn test_parallel_merge_count() -> usize {
    parallel_merge::test_parallel_merge_count()
}

fn entry_fields(entry: &IndexEntry) -> (u32, u16, u32) {
    let block = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.block)) };
    let offset = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.offset)) };
    let data_length = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.data_length)) };
    (block, offset, data_length)
}

fn pop_free_block(rel: pg_sys::Relation) -> Result<Option<u32>> {
    let root = match pgbuffer::BlockBuffer::acquire(rel, 0) {
        Ok(root) => root,
        Err(_) => return Ok(None),
    };
    let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
    if rbl.magic != ROOT_MAGIC || rbl.wal_block == pg_sys::InvalidBlockNumber {
        return Ok(None);
    }

    let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)?;
    let wal = wal_buf
        .as_struct_mut::<WALHeader>(0)
        .context("wal header")?;
    if wal.free_head == pg_sys::InvalidBlockNumber {
        return Ok(None);
    }
    let head = wal.free_head;
    let free_buf = pgbuffer::BlockBuffer::acquire(rel, head)?;
    let free_hdr = free_buf
        .as_struct::<FreePageHeader>(0)
        .context("free page header")?;
    let magic = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(free_hdr.magic)) };
    if magic != FREE_PAGE_MAGIC {
        warning!(
            "free list corruption: block {} has magic {}, expected {}",
            head,
            magic,
            FREE_PAGE_MAGIC
        );
        wal.free_head = pg_sys::InvalidBlockNumber;
        return Ok(None);
    }
    wal.free_head = free_hdr.next_block;
    Ok(Some(head))
}

pub fn allocate_block(rel: pg_sys::Relation) -> pgbuffer::BlockBuffer {
    match pop_free_block(rel) {
        Ok(Some(block)) => {
            let mut page = match pgbuffer::BlockBuffer::aquire_mut(rel, block) {
                Ok(page) => page,
                Err(_) => return pgbuffer::BlockBuffer::allocate(rel),
            };
            page.init_page();
            page
        }
        _ => pgbuffer::BlockBuffer::allocate(rel),
    }
}

pub fn free_blocks(rel: pg_sys::Relation, blocks: &[u32]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    let root = pgbuffer::BlockBuffer::acquire(rel, 0)?;
    let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
    if rbl.magic != ROOT_MAGIC || rbl.wal_block == pg_sys::InvalidBlockNumber {
        return Ok(());
    }

    let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)?;
    let wal = wal_buf
        .as_struct_mut::<WALHeader>(0)
        .context("wal header")?;
    let mut head = wal.free_head;

    for block in blocks {
        if *block == 0
            || *block == rbl.wal_block
            || *block == rbl.pending_block
            || *block == rbl.tombstone_block
        {
            continue;
        }
        let mut page = pgbuffer::BlockBuffer::aquire_mut(rel, *block)?;
        let header = page
            .as_struct_mut::<FreePageHeader>(0)
            .context("free page header")?;
        header.magic = FREE_PAGE_MAGIC;
        header.next_block = head;
        head = *block;
    }
    wal.free_head = head;
    Ok(())
}

fn collect_segment_tree_blocks(
    rel: pg_sys::Relation,
    block: u32,
    out: &mut HashSet<u32>,
) -> Result<()> {
    if !out.insert(block) {
        return Ok(());
    }
    let buf = pgbuffer::BlockBuffer::acquire(rel, block)?;
    let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
    if header.magic != BLOCK_MAGIC {
        anyhow::bail!("invalid block magic while freeing segment");
    }
    if header.level == 0 {
        return Ok(());
    }
    let pointers = buf
        .as_struct_with_elems::<BlockPointerList>(
            std::mem::size_of::<BlockHeader>(),
            header.num_entries as usize,
        )
        .context("block pointers")?;
    let slice = &pointers.entries[..header.num_entries as usize];
    for p in slice {
        collect_segment_tree_blocks(rel, p.block, out)?;
    }
    Ok(())
}

fn collect_posting_blocks(
    rel: pg_sys::Relation,
    entry: &IndexEntry,
    out: &mut HashSet<u32>,
) -> Result<()> {
    let (mut block, _offset, data_length) = entry_fields(entry);
    if data_length == 0 || block == pg_sys::InvalidBlockNumber {
        return Ok(());
    }
    loop {
        if !out.insert(block) {
            break;
        }
        let buf = pgbuffer::BlockBuffer::acquire(rel, block)?;
        let header = buf
            .as_struct::<PostingPageHeader>(0)
            .context("posting page header")?;
        if header.magic != POSTING_PAGE_MAGIC {
            anyhow::bail!("invalid posting page magic while freeing segment");
        }
        if header.next_block == pg_sys::InvalidBlockNumber {
            break;
        }
        block = header.next_block;
    }
    Ok(())
}

pub fn free_segments(rel: pg_sys::Relation, segments: &[Segment]) -> Result<()> {
    if segments.is_empty() {
        return Ok(());
    }
    let mut blocks: HashSet<u32> = HashSet::new();
    for seg in segments {
        collect_segment_tree_blocks(rel, seg.block, &mut blocks)?;
        let leaf_blocks = collect_leaf_blocks(rel, seg.block)?;
        for leaf in leaf_blocks {
            let buf = pgbuffer::BlockBuffer::acquire(rel, leaf)?;
            let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
            if header.magic != BLOCK_MAGIC {
                anyhow::bail!("invalid block magic while freeing segment");
            }
            let entries = buf
                .as_struct_with_elems::<IndexList>(
                    std::mem::size_of::<BlockHeader>(),
                    header.num_entries as usize,
                )
                .context("index entries")?;
            let slice = &entries.entries[..header.num_entries as usize];
            for entry in slice {
                collect_posting_blocks(rel, entry, &mut blocks)?;
            }
        }
    }
    let mut list: Vec<u32> = blocks.into_iter().collect();
    list.sort_unstable();
    free_blocks(rel, &list)
}

fn collect_free_list_blocks(rel: pg_sys::Relation, wal_block: u32) -> Result<Vec<u32>> {
    if wal_block == pg_sys::InvalidBlockNumber {
        return Ok(Vec::new());
    }
    let wal_buf = pgbuffer::BlockBuffer::acquire(rel, wal_block)?;
    let wal = wal_buf.as_struct::<WALHeader>(0).context("wal header")?;
    let mut out = Vec::new();
    let mut seen: HashSet<u32> = HashSet::new();
    let mut blk = wal.free_head;
    while blk != pg_sys::InvalidBlockNumber {
        if !seen.insert(blk) {
            warning!("free list cycle detected at block {}", blk);
            break;
        }
        out.push(blk);
        let buf = pgbuffer::BlockBuffer::acquire(rel, blk)?;
        let hdr = buf
            .as_struct::<FreePageHeader>(0)
            .context("free page header")?;
        let magic = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(hdr.magic)) };
        if magic != FREE_PAGE_MAGIC {
            warning!(
                "free list corruption: block {} has magic {}, expected {}",
                blk,
                magic,
                FREE_PAGE_MAGIC
            );
            break;
        }
        blk = hdr.next_block;
    }
    Ok(out)
}

pub fn maybe_truncate_relation(
    rel: pg_sys::Relation,
    rbl: &RootBlockList,
    segments: &[Segment],
) -> Result<()> {
    let mut used: HashSet<u32> = HashSet::new();
    used.insert(0);
    if rbl.wal_block != pg_sys::InvalidBlockNumber {
        used.insert(rbl.wal_block);
    }
    if rbl.pending_block != pg_sys::InvalidBlockNumber {
        used.insert(rbl.pending_block);
    }
    if rbl.tombstone_block != pg_sys::InvalidBlockNumber {
        used.insert(rbl.tombstone_block);
    }

    if rbl.segment_list_head != pg_sys::InvalidBlockNumber {
        let pages = collect_segment_list_pages(rel, rbl.segment_list_head)?;
        used.extend(pages);
    }

    for seg in segments {
        collect_segment_tree_blocks(rel, seg.block, &mut used)?;
        let leaf_blocks = collect_leaf_blocks(rel, seg.block)?;
        for leaf in leaf_blocks {
            let buf = pgbuffer::BlockBuffer::acquire(rel, leaf)?;
            let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
            if header.magic != BLOCK_MAGIC {
                anyhow::bail!("invalid block magic while truncating");
            }
            let entries = buf
                .as_struct_with_elems::<IndexList>(
                    std::mem::size_of::<BlockHeader>(),
                    header.num_entries as usize,
                )
                .context("index entries")?;
            let slice = &entries.entries[..header.num_entries as usize];
            for entry in slice {
                collect_posting_blocks(rel, entry, &mut used)?;
            }
        }
    }

    let max_used = *used.iter().max().unwrap_or(&0);
    let new_nblocks = max_used.saturating_add(1);
    let nblocks =
        unsafe { pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) };
    if new_nblocks >= nblocks {
        return Ok(());
    }

    let keep = collect_free_list_blocks(rel, rbl.wal_block)?
        .into_iter()
        .filter(|b| *b < new_nblocks)
        .collect::<Vec<u32>>();

    if rbl.wal_block != pg_sys::InvalidBlockNumber {
        let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)?;
        let wal = wal_buf
            .as_struct_mut::<WALHeader>(0)
            .context("wal header")?;
        let mut head = pg_sys::InvalidBlockNumber;
        for block in keep {
            let mut page = pgbuffer::BlockBuffer::aquire_mut(rel, block)?;
            let header = page
                .as_struct_mut::<FreePageHeader>(0)
                .context("free page header")?;
            header.magic = FREE_PAGE_MAGIC;
            header.next_block = head;
            head = block;
        }
        wal.free_head = head;
    }

    unsafe {
        pg_sys::RelationTruncate(rel, new_nblocks);
    }
    Ok(())
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct Segments {
    pub entries: [Segment],
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct BlockPointer {
    pub min_trigram: u32,
    pub block: u32,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct BlockPointerList {
    pub entries: [BlockPointer],
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct BlockHeader {
    pub magic: u32,
    pub level: u8,
    pub num_entries: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct IndexEntry {
    pub trigram: u32,

    pub block: u32,  // The physical block where data starts
    pub offset: u16, // Where inside that block (0..8192)

    pub data_length: u32,

    pub frequency: u32,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct IndexList {
    pub entries: [IndexEntry],
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct WALHeader {
    pub magic: u32,
    pub bytes_used: u32,
    pub head_block: u32,
    pub tail_block: u32,
    pub free_head: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct PostingPageHeader {
    pub magic: u32,
    pub next_block: u32,
    pub next_offset: u16,
    pub free: u16,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct FreePageHeader {
    pub magic: u32,
    pub next_block: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct CompressedBlockHeader {
    // Max of 128 docs per batch
    pub num_docs: u8,
    // hopefully good enough, we'll see.
    pub docs_blk_len: u16,
    pub docs_off_len: u16,

    pub counts_len: u16,

    pub pos_len: u16,

    pub flags_len: u16,
}

struct InternalFrame {
    block: u32,
    next_idx: usize,
    count: usize,
}

struct SegmentCursor {
    rel: pg_sys::Relation,
    stack: Vec<InternalFrame>,
    leaf: Option<pgbuffer::BlockBuffer>,
    leaf_entry_idx: usize,
    leaf_entry_count: usize,
    current: Option<IndexEntry>,
}

impl SegmentCursor {
    fn read_block_header(buf: &pgbuffer::BlockBuffer) -> Result<BlockHeader> {
        let bytes = buf.as_ref();
        let size = std::mem::size_of::<BlockHeader>();
        if size > pgbuffer::SPECIAL_SIZE {
            anyhow::bail!("block header size exceeds page");
        }
        let header =
            unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const BlockHeader) };
        Ok(header)
    }

    fn read_block_pointer(
        buf: &pgbuffer::BlockBuffer,
        idx: usize,
        count: usize,
    ) -> Result<BlockPointer> {
        if idx >= count {
            anyhow::bail!("block pointer index out of range");
        }
        let size = std::mem::size_of::<BlockPointer>();
        let base = std::mem::size_of::<BlockHeader>();
        let offset = base
            .checked_add(idx.saturating_mul(size))
            .context("block pointer offset overflow")?;
        if offset + size > pgbuffer::SPECIAL_SIZE {
            anyhow::bail!("block pointer offset out of bounds");
        }
        let bytes = buf.as_ref();
        let ptr = unsafe { bytes.as_ptr().add(offset) as *const BlockPointer };
        Ok(unsafe { std::ptr::read_unaligned(ptr) })
    }

    fn read_index_entry(
        buf: &pgbuffer::BlockBuffer,
        idx: usize,
        count: usize,
    ) -> Result<IndexEntry> {
        if idx >= count {
            anyhow::bail!("index entry out of range");
        }
        let size = std::mem::size_of::<IndexEntry>();
        let base = std::mem::size_of::<BlockHeader>();
        let offset = base
            .checked_add(idx.saturating_mul(size))
            .context("index entry offset overflow")?;
        if offset + size > pgbuffer::SPECIAL_SIZE {
            anyhow::bail!("index entry offset out of bounds");
        }
        let bytes = buf.as_ref();
        let ptr = unsafe { bytes.as_ptr().add(offset) as *const IndexEntry };
        Ok(unsafe { std::ptr::read_unaligned(ptr) })
    }

    fn new(rel: pg_sys::Relation, segment: &Segment) -> Result<Self> {
        let mut cursor = Self {
            rel,
            stack: Vec::new(),
            leaf: None,
            leaf_entry_idx: 0,
            leaf_entry_count: 0,
            current: None,
        };
        cursor.descend_leftmost(segment.block)?;
        cursor.advance()?;
        Ok(cursor)
    }

    fn current_entry(&self) -> Option<&IndexEntry> {
        self.current.as_ref()
    }

    fn read_child_block(&self, block: u32, idx: usize) -> Result<u32> {
        let buf = pgbuffer::BlockBuffer::acquire(self.rel, block)?;
        let header = Self::read_block_header(&buf)?;
        if header.magic != BLOCK_MAGIC {
            anyhow::bail!("invalid block magic while merging");
        }
        let entry = Self::read_block_pointer(&buf, idx, header.num_entries as usize)?;
        Ok(entry.block)
    }

    fn descend_leftmost(&mut self, mut block: u32) -> Result<()> {
        loop {
            let buf = pgbuffer::BlockBuffer::acquire(self.rel, block)?;
            let header = Self::read_block_header(&buf)?;
            if header.magic != BLOCK_MAGIC {
                anyhow::bail!("invalid block magic while merging");
            }
            if header.level == 0 {
                self.leaf_entry_idx = 0;
                self.leaf_entry_count = header.num_entries as usize;
                self.leaf = Some(buf);
                return Ok(());
            }
            let count = header.num_entries as usize;
            if count == 0 {
                self.leaf = None;
                self.leaf_entry_count = 0;
                return Ok(());
            }
            let child = Self::read_block_pointer(&buf, 0, count)?.block;
            self.stack.push(InternalFrame {
                block,
                next_idx: 1,
                count,
            });
            block = child;
        }
    }

    fn advance_leaf(&mut self) -> Result<bool> {
        self.leaf = None;
        self.leaf_entry_idx = 0;
        self.leaf_entry_count = 0;
        while let Some(mut frame) = self.stack.pop() {
            if frame.next_idx < frame.count {
                let child = self.read_child_block(frame.block, frame.next_idx)?;
                frame.next_idx += 1;
                self.stack.push(frame);
                self.descend_leftmost(child)?;
                if self.leaf_entry_count > 0 {
                    return Ok(true);
                }
                continue;
            }
        }
        Ok(false)
    }

    fn load_current_entry(&mut self) -> Result<bool> {
        let Some(leaf) = self.leaf.as_ref() else {
            return Ok(false);
        };
        if self.leaf_entry_idx >= self.leaf_entry_count {
            return Ok(false);
        }
        let entry = Self::read_index_entry(leaf, self.leaf_entry_idx, self.leaf_entry_count)?;
        self.leaf_entry_idx += 1;
        self.current = Some(entry);
        Ok(true)
    }

    fn advance(&mut self) -> Result<bool> {
        if self.load_current_entry()? {
            return Ok(true);
        }
        while self.advance_leaf()? {
            if self.load_current_entry()? {
                return Ok(true);
            }
        }
        self.current = None;
        Ok(false)
    }
}

pub fn read_segment_entries(rel: pg_sys::Relation, segment: &Segment) -> Result<Vec<IndexEntry>> {
    let leaf_blocks = collect_leaf_blocks(rel, segment.block)?;
    let mut all_entries = Vec::new();
    for leaf_block in leaf_blocks {
        let buf = pgbuffer::BlockBuffer::acquire(rel, leaf_block)?;
        let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
        if header.magic != BLOCK_MAGIC {
            anyhow::bail!("invalid block magic while merging");
        }
        if header.level != 0 {
            anyhow::bail!("expected leaf page while merging");
        }
        let entries = buf
            .as_struct_with_elems::<IndexList>(
                std::mem::size_of::<BlockHeader>(),
                header.num_entries as usize,
            )
            .context("index entries")?;
        all_entries.extend_from_slice(&entries.entries[..header.num_entries as usize]);
    }
    Ok(all_entries)
}

pub fn resolve_leaf_for_trigram(
    rel: pg_sys::Relation,
    root_block: u32,
    trigram: u32,
) -> Result<Option<u32>> {
    let mut block = root_block;
    loop {
        let buf = pgbuffer::BlockBuffer::acquire(rel, block)?;
        let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
        if header.magic != BLOCK_MAGIC {
            anyhow::bail!("invalid block magic");
        }
        if header.level == 0 {
            return Ok(Some(block));
        }
        let pointers = buf
            .as_struct_with_elems::<BlockPointerList>(
                std::mem::size_of::<BlockHeader>(),
                header.num_entries as usize,
            )
            .context("block pointers")?;
        let slice = &pointers.entries[..header.num_entries as usize];
        if slice.is_empty() {
            return Ok(None);
        }
        let idx = match slice.binary_search_by(|p| {
            let mt = p.min_trigram;
            mt.cmp(&trigram)
        }) {
            Ok(i) => i,
            Err(0) => return Ok(None),
            Err(i) => i - 1,
        };
        block = slice[idx].block;
    }
}

pub fn collect_leaf_blocks(rel: pg_sys::Relation, root_block: u32) -> Result<Vec<u32>> {
    fn collect(rel: pg_sys::Relation, block: u32, out: &mut Vec<u32>) -> Result<()> {
        let buf = pgbuffer::BlockBuffer::acquire(rel, block)?;
        let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
        if header.magic != BLOCK_MAGIC {
            anyhow::bail!("invalid block magic");
        }
        if header.level == 0 {
            out.push(block);
            return Ok(());
        }
        let pointers = buf
            .as_struct_with_elems::<BlockPointerList>(
                std::mem::size_of::<BlockHeader>(),
                header.num_entries as usize,
            )
            .context("block pointers")?;
        let slice = &pointers.entries[..header.num_entries as usize];
        for p in slice {
            collect(rel, p.block, out)?;
        }
        Ok(())
    }

    let mut out = Vec::new();
    collect(rel, root_block, &mut out)?;
    Ok(out)
}

fn merge_entry_postings_stream(
    rel: pg_sys::Relation,
    entries: &[IndexEntry],
    tombstones: &tombstone::Snapshot,
    mut on_doc: impl FnMut(ItemPointer, &[crate::trgm::Occurance]) -> Result<()>,
) -> Result<()> {
    let mut cursors = Vec::new();
    for entry in entries {
        let mut cursor = unsafe { crate::storage::decode::PostingCursor::new(rel, entry)? };
        if cursor.advance()? {
            cursors.push(cursor);
        }
    }

    let mut heap: BinaryHeap<Reverse<(ItemPointer, usize)>> = BinaryHeap::new();
    for (idx, cursor) in cursors.iter().enumerate() {
        if let Some(tid) = cursor.current_tid() {
            heap.push(Reverse((tid, idx)));
        }
    }

    let mut occs: Vec<crate::trgm::Occurance> = Vec::new();
    let mut cursor_indices: Vec<usize> = Vec::new();
    while let Some(Reverse((target, idx))) = heap.pop() {
        cursor_indices.clear();
        cursor_indices.push(idx);
        while let Some(Reverse((next_tid, next_idx))) = heap.peek().cloned() {
            if next_tid != target {
                break;
            }
            heap.pop();
            cursor_indices.push(next_idx);
        }

        if tombstones.contains(target) {
            for idx in cursor_indices.iter().copied() {
                let cursor = &mut cursors[idx];
                if cursor.advance()? {
                    if let Some(next_tid) = cursor.current_tid() {
                        heap.push(Reverse((next_tid, idx)));
                    }
                }
            }
            continue;
        }
        let needs_sort = cursor_indices.len() > 1;

        occs.clear();
        for idx in cursor_indices.iter().copied() {
            let cursor = &mut cursors[idx];
            if let Some(doc) = cursor.current() {
                occs.reserve(doc.positions.len());
                for (position, flags) in doc.positions.iter() {
                    let mut occ = crate::trgm::Occurance(*position);
                    occ.set_flags(*flags);
                    occs.push(occ);
                }
            }
            if cursor.advance()? {
                if let Some(next_tid) = cursor.current_tid() {
                    heap.push(Reverse((next_tid, idx)));
                }
            }
        }

        if !occs.is_empty() {
            if needs_sort {
                occs.sort_unstable_by_key(|occ| occ.position());
            }
            on_doc(target, &occs)?;
        }
    }

    Ok(())
}

pub fn merge(
    rel: pg_sys::Relation,
    segments: &[Segment],
    _flush_threshold: usize,
    tombstones: &tombstone::Snapshot,
) -> Result<Segment> {
    let total_bytes = segments
        .iter()
        .map(|segment| segment.size as usize)
        .sum::<usize>();
    info!("merge: segments={} total_bytes={}", segments.len(), total_bytes);

    let mut cursors = Vec::new();
    for segment in segments {
        let cursor = SegmentCursor::new(rel, segment)?;
        if cursor.current_entry().is_some() {
            cursors.push(cursor);
        }
    }

    if cursors.is_empty() {
        return Ok(Segment {
            block: pg_sys::InvalidBlockNumber,
            size: 0,
        });
    }

    // Heap of (trigram, cursor_idx) for k-way merge across segments.
    let mut heap: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::new();
    for (idx, cursor) in cursors.iter().enumerate() {
        if let Some(entry) = cursor.current_entry() {
            heap.push(Reverse((entry.trigram, idx)));
        }
    }

    // Stream postings directly into pages while building leaf index entries.
    let mut writer = crate::storage::encode::PageWriter::new(rel, pgbuffer::SPECIAL_SIZE);
    let mut leaf: Option<pgbuffer::BlockBuffer> = None;
    let mut leaf_block = pg_sys::InvalidBlockNumber;
    let mut leaf_min_trigram: Option<u32> = None;
    let mut leaf_entries_written: usize = 0;
    let mut leaf_pointers: Vec<BlockPointer> = Vec::new();
    const BH_SIZE: usize = std::mem::size_of::<BlockHeader>();
    const ENTRY_SIZE: usize = std::mem::size_of::<IndexEntry>();
    let leaf_entry_cap = (pgbuffer::SPECIAL_SIZE - BH_SIZE) / ENTRY_SIZE;

    let mut byte_count: u64 = 0;
    let mut doc_count: u64 = 0;
    let mut occ_count: u64 = 0;
    let mut occ_count_known = true;
    let max_chunk_size =
        pgbuffer::SPECIAL_SIZE - std::mem::size_of::<PostingPageHeader>();

    fn start_leaf(
        rel: pg_sys::Relation,
        leaf: &mut Option<pgbuffer::BlockBuffer>,
        leaf_block: &mut u32,
        leaf_entries_written: &mut usize,
        leaf_min_trigram: &mut Option<u32>,
    ) -> Result<()> {
        let mut page = allocate_block(rel);
        *leaf_block = page.block_number();
        let header = page
            .as_struct_mut::<BlockHeader>(0)
            .context("block header")?;
        header.magic = BLOCK_MAGIC;
        header.level = 0;
        header.num_entries = 0;
        *leaf_entries_written = 0;
        *leaf_min_trigram = None;
        *leaf = Some(page);
        Ok(())
    }

    fn finalize_leaf(
        leaf: &mut Option<pgbuffer::BlockBuffer>,
        leaf_entries_written: usize,
        leaf_min_trigram: &Option<u32>,
        leaf_block: u32,
        leaf_pointers: &mut Vec<BlockPointer>,
    ) -> Result<()> {
        if leaf_entries_written == 0 {
            *leaf = None;
            return Ok(());
        }
        let min_trigram = leaf_min_trigram.context("leaf missing min trigram")?;
        leaf_pointers.push(BlockPointer {
            min_trigram,
            block: leaf_block,
        });
        *leaf = None;
        Ok(())
    }

    let mut group_entries: Vec<IndexEntry> = Vec::new();
    let mut cursor_indices: Vec<usize> = Vec::new();
    while let Some(Reverse((trigram, idx))) = heap.pop() {
        cursor_indices.clear();
        cursor_indices.push(idx);
        while let Some(Reverse((next_trigram, next_idx))) = heap.peek().cloned() {
            if next_trigram != trigram {
                break;
            }
            heap.pop();
            cursor_indices.push(next_idx);
        }

        // Collect all segment entries for this trigram and advance those cursors.
        group_entries.clear();
        for idx in cursor_indices.iter().copied() {
            let cursor = &mut cursors[idx];
            let Some(entry) = cursor.current_entry() else {
                continue;
            };
            if entry.trigram != trigram {
                continue;
            }
            group_entries.push(*entry);
            cursor.advance()?;
            if let Some(next) = cursor.current_entry() {
                heap.push(Reverse((next.trigram, idx)));
            }
        }

        if leaf.is_none() {
            start_leaf(
                rel,
                &mut leaf,
                &mut leaf_block,
                &mut leaf_entries_written,
                &mut leaf_min_trigram,
            )?;
        }
        if leaf_entries_written >= leaf_entry_cap {
            finalize_leaf(
                &mut leaf,
                leaf_entries_written,
                &leaf_min_trigram,
                leaf_block,
                &mut leaf_pointers,
            )?;
            start_leaf(
                rel,
                &mut leaf,
                &mut leaf_block,
                &mut leaf_entries_written,
                &mut leaf_min_trigram,
            )?;
        }

        // Encode postings for this trigram into posting pages.
        let mut idx_entry = IndexEntry {
            trigram,
            block: 0,
            offset: 0,
            data_length: 0,
            frequency: 0,
        };
        let mut trgm_docs: u32 = 0;
        let mut builder = encode::CompressedBatchBuilder::new();
        let mut compressed = Vec::new();
        let mut first_chunk = false;

        let mut flush_chunk = |builder: &mut encode::CompressedBatchBuilder,
                               idx: &mut IndexEntry,
                               first_chunk: &mut bool|
         -> Result<()> {
            if builder.num_docs() == 0 {
                return Ok(());
            }
            builder.compress_into(&mut compressed);
            if compressed.len() > max_chunk_size {
                anyhow::bail!(
                    "chunk size {} exceeds page capacity {}",
                    compressed.len(),
                    max_chunk_size
                );
            }
            let loc = writer.start_chunk(compressed.len());
            writer
                .write_all(&compressed)
                .expect("posting write succeeds");
            if !*first_chunk {
                idx.block = loc.block_number;
                idx.offset = loc.offset as u16;
                *first_chunk = true;
            }
            idx.data_length = idx
                .data_length
                .checked_add(compressed.len() as u32)
                .expect("overflow on data length");
            byte_count = byte_count.saturating_add(compressed.len() as u64);
            builder.reset();
            Ok(())
        };

        if group_entries.len() == 1 && tombstones.is_empty() {
            let entry = &group_entries[0];
            let (_, _, data_length) = entry_fields(entry);
            let frequency =
                unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.frequency)) };
            idx_entry.data_length = data_length;
            idx_entry.frequency = frequency;
            if data_length > 0 {
                let first_len = (data_length as usize).min(max_chunk_size);
                let loc = writer.start_chunk(first_len);
                idx_entry.block = loc.block_number;
                idx_entry.offset = loc.offset as u16;
                unsafe {
                    crate::storage::decode::copy_posting_bytes(
                        rel,
                        entry,
                        data_length as usize,
                        &mut writer,
                    )?;
                }
                byte_count = byte_count.saturating_add(data_length as u64);
            }
            doc_count = doc_count.saturating_add(frequency as u64);
            occ_count_known = false;
        } else {
            merge_entry_postings_stream(rel, &group_entries, tombstones, |doc, occs| {
                trgm_docs = trgm_docs.saturating_add(1);
                doc_count = doc_count.saturating_add(1);
                occ_count = occ_count.saturating_add(occs.len() as u64);
                if occs.is_empty() {
                    return Ok(());
                }
                let mut start = 0usize;
                while start < occs.len() {
                    if builder.num_docs() >= u8::MAX as usize {
                        flush_chunk(&mut builder, &mut idx_entry, &mut first_chunk)?;
                        continue;
                    }

                    let remaining = occs.len() - start;
                    let can_take = builder.max_positions_fit(max_chunk_size).min(remaining);
                    if can_take == 0 {
                        if builder.num_docs() == 0 {
                            anyhow::bail!(
                                "single doc chunk size {} exceeds page capacity {}",
                                occs.len(),
                                max_chunk_size
                            );
                        }
                        flush_chunk(&mut builder, &mut idx_entry, &mut first_chunk)?;
                        continue;
                    }
                    builder.add(doc, &occs[start..start + can_take]);
                    start += can_take;
                }
                Ok(())
            })?;
            flush_chunk(&mut builder, &mut idx_entry, &mut first_chunk)?;
            idx_entry.frequency = trgm_docs;
        }

        let leaf_ref = leaf.as_mut().context("leaf buffer")?;
        let header = leaf_ref
            .as_struct_mut::<BlockHeader>(0)
            .context("block header")?;
        header.num_entries = (leaf_entries_written + 1) as u32;
        let entries = leaf_ref
            .as_struct_with_elems_mut::<IndexList>(BH_SIZE, leaf_entry_cap)
            .context("index entries")?;
        entries.entries[leaf_entries_written] = idx_entry;
        if leaf_min_trigram.is_none() {
            leaf_min_trigram = Some(trigram);
        }
        leaf_entries_written += 1;
    }

    finalize_leaf(
        &mut leaf,
        leaf_entries_written,
        &leaf_min_trigram,
        leaf_block,
        &mut leaf_pointers,
    )?;
    if occ_count_known {
        info!(
            "Encoded {doc_count} docs, {occ_count} occs and {byte_count} bytes"
        );
    } else {
        info!("Encoded {doc_count} docs and {byte_count} bytes (occs elided)");
    }
    let segment = Segment {
        block: crate::storage::encode::build_segment_root(rel, &leaf_pointers)?,
        size: byte_count,
    };
    Ok(segment)
}
