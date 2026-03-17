use anyhow::{Context, Result};
use pgrx::prelude::*;
/// Storing stuff
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::io::Write;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

pub const TARGET_SEGMENTS: usize = 10;
pub const DEFAULT_LARGE_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_LARGE_SEGMENT_DEAD_RATIO_BPS: u32 = 2_500;
pub const DEFAULT_LARGE_SEGMENT_MIN_RECLAIM_BYTES: u64 = 64 * 1024 * 1024;
pub const SEGMENT_DEADNESS_SAMPLE_ENTRIES: usize = 32;
pub const SEGMENT_DEADNESS_SAMPLE_DOCS_PER_ENTRY: usize = 128;

static LARGE_SEGMENT_BYTES: AtomicU64 = AtomicU64::new(DEFAULT_LARGE_SEGMENT_BYTES);
static LARGE_SEGMENT_DEAD_RATIO_BPS: AtomicU32 =
    AtomicU32::new(DEFAULT_LARGE_SEGMENT_DEAD_RATIO_BPS);
static LARGE_SEGMENT_MIN_RECLAIM_BYTES: AtomicU64 =
    AtomicU64::new(DEFAULT_LARGE_SEGMENT_MIN_RECLAIM_BYTES);

pub mod decode;
pub mod encode;
mod parallel_merge;
pub mod pending;
pub mod pgbuffer;
pub mod tombstone;

pub const VERSION: u16 = 6;
pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");
pub const WAL_MAGIC: u32 = u32::from_ne_bytes(*b"wZKT");
pub const PENDING_MAGIC: u32 = u32::from_ne_bytes(*b"pPLD");
pub const PENDING_BUCKET_MAGIC: u16 = u16::from_ne_bytes(*b"PL");
pub const POSTING_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"oZKT");
pub const SEGMENT_LIST_MAGIC: u32 = u32::from_ne_bytes(*b"lZKT");
pub const SEGMENT_EXTENT_MAGIC: u32 = u32::from_ne_bytes(*b"eZKT");
pub const TOMBSTONE_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"tZKT");
pub const FREE_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"fZKT");

#[derive(Debug, Clone, Copy, Default)]
pub struct DeadnessEstimate {
    pub sampled_docs: u64,
    pub sampled_dead_docs: u64,
    pub estimated_dead_ratio: f64,
    pub estimated_reclaim_bytes: u64,
}

pub fn large_segment_bytes() -> u64 {
    LARGE_SEGMENT_BYTES.load(Ordering::Relaxed)
}

pub fn large_segment_dead_ratio() -> f64 {
    LARGE_SEGMENT_DEAD_RATIO_BPS.load(Ordering::Relaxed) as f64 / 10_000.0
}

pub fn large_segment_min_reclaim_bytes() -> u64 {
    LARGE_SEGMENT_MIN_RECLAIM_BYTES.load(Ordering::Relaxed)
}

pub fn estimate_segment_deadness(
    rel: pg_sys::Relation,
    segment: &Segment,
    tombstones: &tombstone::Snapshot,
) -> Result<DeadnessEstimate> {
    let segment_block = segment.block;
    let segment_size = segment.size;
    let entries = read_segment_entries(rel, segment)?;
    if entries.is_empty() {
        return Ok(DeadnessEstimate::default());
    }

    let sample_count = entries.len().min(SEGMENT_DEADNESS_SAMPLE_ENTRIES);
    let mut sampled_docs = 0u64;
    let mut sampled_dead_docs = 0u64;
    let mut last_idx: Option<usize> = None;

    for sample_idx in 0..sample_count {
        let idx = if sample_count == 1 {
            0
        } else {
            sample_idx.saturating_mul(entries.len().saturating_sub(1)) / (sample_count - 1)
        };
        if last_idx == Some(idx) {
            continue;
        }
        last_idx = Some(idx);

        let entry = &entries[idx];
        let mut cursor = match unsafe { decode::PostingCursor::new(rel, entry) } {
            Ok(cursor) => cursor,
            Err(e) => {
                warning!(
                    "estimate_segment_deadness: failed to open posting cursor for segment block {}: {e:#}",
                    segment_block
                );
                continue;
            }
        };

        let mut per_entry_docs = 0usize;
        while per_entry_docs < SEGMENT_DEADNESS_SAMPLE_DOCS_PER_ENTRY {
            match cursor.advance() {
                Ok(true) => {
                    let Some(doc) = cursor.current() else {
                        continue;
                    };
                    sampled_docs = sampled_docs.saturating_add(1);
                    if tombstones.contains(doc.tid) {
                        sampled_dead_docs = sampled_dead_docs.saturating_add(1);
                    }
                    per_entry_docs += 1;
                }
                Ok(false) => break,
                Err(e) => {
                    warning!(
                        "estimate_segment_deadness: failed to advance posting cursor for segment block {}: {e:#}",
                        segment_block
                    );
                    break;
                }
            }
        }
    }

    if sampled_docs == 0 {
        return Ok(DeadnessEstimate::default());
    }

    let estimated_dead_ratio = sampled_dead_docs as f64 / sampled_docs as f64;
    let estimated_reclaim_bytes = ((segment_size as f64) * estimated_dead_ratio) as u64;
    Ok(DeadnessEstimate {
        sampled_docs,
        sampled_dead_docs,
        estimated_dead_ratio,
        estimated_reclaim_bytes,
    })
}

pub fn segment_merge_eligible(
    rel: pg_sys::Relation,
    segment: &Segment,
    tombstones: &tombstone::Snapshot,
) -> Result<bool> {
    let segment_block = segment.block;
    let segment_size = segment.size;
    if segment_size < large_segment_bytes() {
        return Ok(true);
    }

    let estimate = estimate_segment_deadness(rel, segment, tombstones)?;
    let ratio_threshold = large_segment_dead_ratio();
    let reclaim_threshold = large_segment_min_reclaim_bytes();
    let eligible = estimate.estimated_dead_ratio >= ratio_threshold
        && estimate.estimated_reclaim_bytes >= reclaim_threshold;
    info!(
        "segment_merge_eligible: block={} size={} sampled_docs={} sampled_dead_docs={} estimated_dead_ratio={:.4} ratio_threshold={:.4} estimated_reclaim_bytes={} reclaim_threshold={} eligible={}",
        segment_block,
        segment_size,
        estimate.sampled_docs,
        estimate.sampled_dead_docs,
        estimate.estimated_dead_ratio,
        ratio_threshold,
        estimate.estimated_reclaim_bytes,
        reclaim_threshold,
        eligible
    );
    Ok(eligible)
}

#[cfg(any(test, feature = "pg_test"))]
pub fn test_set_large_segment_policy(
    large_bytes: u64,
    dead_ratio_bps: u32,
    min_reclaim_bytes: u64,
) {
    LARGE_SEGMENT_BYTES.store(large_bytes, Ordering::Relaxed);
    LARGE_SEGMENT_DEAD_RATIO_BPS.store(dead_ratio_bps, Ordering::Relaxed);
    LARGE_SEGMENT_MIN_RECLAIM_BYTES.store(min_reclaim_bytes, Ordering::Relaxed);
}

#[cfg(any(test, feature = "pg_test"))]
pub fn test_reset_large_segment_policy() {
    test_set_large_segment_policy(
        DEFAULT_LARGE_SEGMENT_BYTES,
        DEFAULT_LARGE_SEGMENT_DEAD_RATIO_BPS,
        DEFAULT_LARGE_SEGMENT_MIN_RECLAIM_BYTES,
    );
}

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

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexBootstrapBlocks {
    pub(crate) root_block: u32,
    pub(crate) wal_block: u32,
    pub(crate) pending_block: u32,
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
    pub extent_head: u32,
    pub extent_count: u32,
}

#[derive(
    Debug, FromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy, PartialEq, Eq,
)]
#[repr(C, packed)]
pub struct SegmentV1 {
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

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct SegmentExtent {
    pub start_block: u32,
    pub len: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct SegmentExtentListPageHeader {
    pub magic: u32,
    pub next_block: u32,
    pub count: u16,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct SegmentExtents {
    pub entries: [SegmentExtent],
}

#[derive(Default, Debug)]
pub(crate) struct BlockExtentTracker {
    extents: Vec<SegmentExtent>,
}

impl BlockExtentTracker {
    pub(crate) fn record(&mut self, block: u32) {
        if let Some(last) = self.extents.last_mut() {
            let expected_next = last.start_block.saturating_add(last.len);
            if block == expected_next {
                last.len = last.len.saturating_add(1);
                return;
            }
        }
        self.extents.push(SegmentExtent {
            start_block: block,
            len: 1,
        });
    }

    pub(crate) fn take(self) -> Vec<SegmentExtent> {
        self.extents
    }
}

pub(crate) fn record_block(tracker: Option<*mut BlockExtentTracker>, block: u32) {
    if let Some(ptr) = tracker {
        unsafe {
            (*ptr).record(block);
        }
    }
}

pub(crate) fn log_block_event(
    rel: pg_sys::Relation,
    event: &'static str,
    block: u32,
    kind: &'static str,
) {
    let relid = if rel.is_null() {
        0
    } else {
        unsafe { u32::from((*rel).rd_id) }
    };
    info!(
        "block_event: rel={} event={} block={} kind={}",
        relid, event, block, kind
    );
}

fn sample_blocks(blocks: &[u32]) -> String {
    const LIMIT: usize = 8;
    let mut parts = blocks
        .iter()
        .take(LIMIT)
        .map(u32::to_string)
        .collect::<Vec<_>>();
    if blocks.len() > LIMIT {
        parts.push(format!("...(+{})", blocks.len() - LIMIT));
    }
    format!("[{}]", parts.join(","))
}

const fn segment_list_capacity(version: u16) -> usize {
    let header = std::mem::size_of::<SegmentListPageHeader>();
    let seg = if version >= 6 {
        std::mem::size_of::<Segment>()
    } else {
        std::mem::size_of::<SegmentV1>()
    };
    (pgbuffer::SPECIAL_SIZE - header) / seg
}

fn segment_list_init_page(
    rel: pg_sys::Relation,
    root: &RootBlockList,
) -> Result<pgbuffer::BlockBuffer> {
    let mut page = allocate_block_with_root(rel, root);
    log_block_event(rel, "init", page.block_number(), "segment_list_page");
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
    let cap = segment_list_capacity(root.version);
    if cap == 0 {
        anyhow::bail!("segment list page capacity is 0");
    }

    // Ensure we have a head/tail page.
    if root.segment_list_head == pg_sys::InvalidBlockNumber {
        let page = segment_list_init_page(rel, root)?;
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
            let next_page = segment_list_init_page(rel, root)?;
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
        if root.version >= 6 {
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
        } else {
            let seg_size = std::mem::size_of::<SegmentV1>();
            let start_off = header_size + used * seg_size;
            let bytes = unsafe {
                let p = tail.as_ptr_mut().add(start_off) as *mut u8;
                std::slice::from_raw_parts_mut(p, take * seg_size)
            };
            let mut compat = Vec::with_capacity(take);
            for seg in &remaining[..take] {
                compat.push(SegmentV1 {
                    block: seg.block,
                    size: seg.size,
                });
            }
            // SAFETY: `SegmentV1` is plain old data and packed; we just copy bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    compat.as_ptr() as *const u8,
                    bytes.as_mut_ptr(),
                    take * seg_size,
                );
            }
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
    let cap = segment_list_capacity(root.version);
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
        if root.version >= 6 {
            let list = buf
                .as_struct_with_elems::<Segments>(
                    std::mem::size_of::<SegmentListPageHeader>(),
                    count,
                )
                .context("segment list entries")?;
            out.extend_from_slice(&list.entries[..count]);
        } else {
            let list = buf
                .as_struct_with_elems::<SegmentsV1>(
                    std::mem::size_of::<SegmentListPageHeader>(),
                    count,
                )
                .context("segment list entries")?;
            out.extend(list.entries[..count].iter().map(|seg| Segment {
                block: seg.block,
                size: seg.size,
                extent_head: pg_sys::InvalidBlockNumber,
                extent_count: 0,
            }));
        }
        blk = hdr.next_block;
    }
    out.truncate(root.num_segments as usize);
    Ok(out)
}

const fn segment_extent_capacity() -> usize {
    let header = std::mem::size_of::<SegmentExtentListPageHeader>();
    let extent = std::mem::size_of::<SegmentExtent>();
    (pgbuffer::SPECIAL_SIZE - header) / extent
}

fn segment_extent_list_write(rel: pg_sys::Relation, extents: &[SegmentExtent]) -> Result<u32> {
    if extents.is_empty() {
        return Ok(pg_sys::InvalidBlockNumber);
    }
    let cap = segment_extent_capacity();
    if cap == 0 {
        anyhow::bail!("segment extent list page capacity is 0");
    }
    let mut remaining = extents;
    let mut head = pg_sys::InvalidBlockNumber;
    let mut tail = pg_sys::InvalidBlockNumber;
    while !remaining.is_empty() {
        let mut page = allocate_block(rel);
        let blk = page.block_number();
        log_block_event(rel, "init", blk, "segment_extent_page");
        let hdr = page
            .as_struct_mut::<SegmentExtentListPageHeader>(0)
            .context("segment extent list header")?;
        hdr.magic = SEGMENT_EXTENT_MAGIC;
        hdr.next_block = pg_sys::InvalidBlockNumber;
        hdr.count = 0;
        if head == pg_sys::InvalidBlockNumber {
            head = blk;
        } else {
            let mut prev =
                pgbuffer::BlockBuffer::aquire_mut(rel, tail).context("extent list page")?;
            let prev_hdr = prev
                .as_struct_mut::<SegmentExtentListPageHeader>(0)
                .context("segment extent list header")?;
            prev_hdr.next_block = blk;
        }
        let take = remaining.len().min(cap);
        let header_size = std::mem::size_of::<SegmentExtentListPageHeader>();
        let extent_size = std::mem::size_of::<SegmentExtent>();
        let bytes = unsafe {
            let p = page.as_ptr_mut().add(header_size) as *mut u8;
            std::slice::from_raw_parts_mut(p, take * extent_size)
        };
        // SAFETY: `SegmentExtent` is plain old data and packed; we just copy bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                remaining.as_ptr() as *const u8,
                bytes.as_mut_ptr(),
                take * extent_size,
            );
        }
        let hdr = page
            .as_struct_mut::<SegmentExtentListPageHeader>(0)
            .context("segment extent list header")?;
        hdr.count = take as u16;
        tail = blk;
        remaining = &remaining[take..];
    }
    Ok(head)
}

pub(crate) fn segment_extent_list_read(
    rel: pg_sys::Relation,
    head: u32,
    count: u32,
) -> Result<(Vec<SegmentExtent>, Vec<u32>)> {
    if head == pg_sys::InvalidBlockNumber || count == 0 {
        return Ok((Vec::new(), Vec::new()));
    }
    let cap = segment_extent_capacity();
    let mut extents = Vec::with_capacity(count as usize);
    let mut pages = Vec::new();
    let mut blk = head;
    while blk != pg_sys::InvalidBlockNumber && extents.len() < count as usize {
        pages.push(blk);
        let buf = pgbuffer::BlockBuffer::acquire(rel, blk)?;
        let hdr = buf
            .as_struct::<SegmentExtentListPageHeader>(0)
            .context("segment extent list header")?;
        if hdr.magic != SEGMENT_EXTENT_MAGIC {
            anyhow::bail!("bad segment extent list magic");
        }
        let take = (hdr.count as usize).min(cap);
        let list = buf
            .as_struct_with_elems::<SegmentExtents>(
                std::mem::size_of::<SegmentExtentListPageHeader>(),
                take,
            )
            .context("segment extent list entries")?;
        extents.extend_from_slice(&list.entries[..take]);
        blk = hdr.next_block;
    }
    extents.truncate(count as usize);
    Ok((extents, pages))
}

fn segment_attach_extents(
    rel: pg_sys::Relation,
    segment: &mut Segment,
    extents: &[SegmentExtent],
) -> Result<()> {
    if extents.is_empty() {
        segment.extent_head = pg_sys::InvalidBlockNumber;
        segment.extent_count = 0;
        return Ok(());
    }
    let head = segment_extent_list_write(rel, extents)?;
    segment.extent_head = head;
    segment.extent_count = extents.len() as u32;
    Ok(())
}

pub(crate) fn collect_segment_list_pages(rel: pg_sys::Relation, head: u32) -> Result<Vec<u32>> {
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
        free_blocks_with_meta(rel, root_meta(root), &old_pages)?;
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
    let requested_workers = workers.unwrap_or_else(|| reloption_parallel_workers(rel));
    let mut workers = requested_workers;
    if unsafe { pg_sys::IsInParallelMode() } {
        info!("merge_with_workers: parallel mode active, disabling internal parallel merge");
        workers = 0;
    }
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
    let mut groups: Vec<(u64, Vec<Segment>)> =
        (0..target_segments).map(|_| (0u64, Vec::new())).collect();
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
    let group_count = offsets.len().saturating_sub(1);
    if workers > 0 {
        let max_workers = unsafe { pg_sys::max_parallel_workers.max(0) as usize };
        workers = workers.min(max_workers).min(group_count);
    }
    info!(
        "merge_with_workers: group_count={} total_input_segments={} requested_workers={} effective_workers={}",
        group_count,
        flat.len(),
        requested_workers,
        workers
    );

    if workers > 0 {
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
        if tombstones.is_empty() && segs.len() == 1 {
            merged.push(segs[0]);
            continue;
        }
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

#[derive(Clone, Copy)]
struct RootMeta {
    magic: u32,
    version: u16,
    wal_block: u32,
    pending_block: u32,
    tombstone_block: u32,
}

fn root_meta(root: &RootBlockList) -> RootMeta {
    RootMeta {
        magic: root.magic,
        version: root.version,
        wal_block: root.wal_block,
        pending_block: root.pending_block,
        tombstone_block: root.tombstone_block,
    }
}

fn read_root_meta(rel: pg_sys::Relation) -> Result<RootMeta> {
    let root = pgbuffer::BlockBuffer::acquire(rel, 0)?;
    let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
    Ok(root_meta(rbl))
}

fn pop_free_block_with_meta(rel: pg_sys::Relation, meta: RootMeta) -> Result<Option<u32>> {
    if meta.magic != ROOT_MAGIC || meta.wal_block == pg_sys::InvalidBlockNumber {
        return Ok(None);
    }

    let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, meta.wal_block)?;
    let wal = wal_buf
        .as_struct_mut::<WALHeader>(0)
        .context("wal header")?;
    let nblocks =
        unsafe { pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) };
    while wal.free_head != pg_sys::InvalidBlockNumber {
        let head = wal.free_head;
        if head >= nblocks {
            warning!(
                "free list corruption: block {} is outside relation size {}",
                head,
                nblocks
            );
            wal.free_head = pg_sys::InvalidBlockNumber;
            wal.free_max_block = pg_sys::InvalidBlockNumber;
            break;
        }
        let free_buf = match pgbuffer::BlockBuffer::acquire(rel, head) {
            Ok(buf) => buf,
            Err(e) => {
                warning!("free list corruption: cannot read block {}: {e:#}", head);
                wal.free_head = pg_sys::InvalidBlockNumber;
                wal.free_max_block = pg_sys::InvalidBlockNumber;
                break;
            }
        };
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
            wal.free_max_block = pg_sys::InvalidBlockNumber;
            break;
        }
        if meta.version >= 5 && wal.free_max_block == head {
            wal.free_max_block = pg_sys::InvalidBlockNumber;
        }
        wal.free_head = free_hdr.next_block;
        log_block_event(rel, "reuse", head, "generic_block");
        return Ok(Some(head));
    }
    Ok(None)
}

fn allocate_block_with_meta(rel: pg_sys::Relation, meta: RootMeta) -> pgbuffer::BlockBuffer {
    match pop_free_block_with_meta(rel, meta) {
        Ok(Some(block)) => {
            let mut page = match pgbuffer::BlockBuffer::aquire_mut(rel, block) {
                Ok(page) => page,
                Err(_) => return pgbuffer::BlockBuffer::allocate(rel),
            };
            page.init_page();
            log_block_event(rel, "reinit", block, "generic_block");
            page
        }
        _ => {
            let page = pgbuffer::BlockBuffer::allocate(rel);
            let block = page.block_number();
            log_block_event(rel, "extend", block, "generic_block");
            if let Err(err) = update_high_water_block_with_meta(rel, meta, block) {
                warning!("failed to update high-water mark: {err:#?}");
            }
            page
        }
    }
}

pub(crate) fn allocate_block_with_root(
    rel: pg_sys::Relation,
    root: &RootBlockList,
) -> pgbuffer::BlockBuffer {
    allocate_block_with_meta(rel, root_meta(root))
}

pub fn allocate_block(rel: pg_sys::Relation) -> pgbuffer::BlockBuffer {
    match read_root_meta(rel) {
        Ok(meta) => allocate_block_with_meta(rel, meta),
        Err(_) => pgbuffer::BlockBuffer::allocate(rel),
    }
}

pub(crate) fn allocate_block_tracked(
    rel: pg_sys::Relation,
    tracker: Option<*mut BlockExtentTracker>,
) -> pgbuffer::BlockBuffer {
    let page = allocate_block(rel);
    record_block(tracker, page.block_number());
    page
}

fn free_blocks_with_meta(rel: pg_sys::Relation, meta: RootMeta, blocks: &[u32]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    if meta.magic != ROOT_MAGIC || meta.wal_block == pg_sys::InvalidBlockNumber {
        return Ok(());
    }

    let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, meta.wal_block)?;
    let wal = wal_buf
        .as_struct_mut::<WALHeader>(0)
        .context("wal header")?;
    let mut head = wal.free_head;
    let mut free_max = if meta.version >= 5 {
        wal.free_max_block
    } else {
        pg_sys::InvalidBlockNumber
    };

    for block in blocks {
        if *block == 0
            || *block == meta.wal_block
            || *block == meta.pending_block
            || *block == meta.tombstone_block
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
        log_block_event(rel, "free", *block, "generic_block");
        if meta.version >= 5 && (free_max == pg_sys::InvalidBlockNumber || *block > free_max) {
            free_max = *block;
        }
    }
    wal.free_head = head;
    if meta.version >= 5 {
        wal.free_max_block = free_max;
    }
    Ok(())
}

pub fn free_blocks(rel: pg_sys::Relation, blocks: &[u32]) -> Result<()> {
    let meta = read_root_meta(rel)?;
    free_blocks_with_meta(rel, meta, blocks)
}

pub(crate) fn initialize_index_storage(
    rel: pg_sys::Relation,
    expect_root_block_zero: bool,
) -> Result<IndexBootstrapBlocks> {
    let root_block = {
        let mut root_buffer = pgbuffer::BlockBuffer::allocate(rel);
        let root_block = root_buffer.block_number();
        log_block_event(rel, "extend", root_block, "root_page");
        if expect_root_block_zero && root_block != 0 {
            anyhow::bail!("expected root block 0 for empty index, got {root_block}");
        }
        let rbl = root_buffer
            .as_struct_mut::<RootBlockList>(0)
            .context("initialize_index_storage: root header")?;
        rbl.magic = ROOT_MAGIC;
        rbl.version = VERSION;
        rbl.wal_block = pg_sys::InvalidBlockNumber;
        rbl.num_segments = 0;
        rbl.segment_list_head = pg_sys::InvalidBlockNumber;
        rbl.segment_list_tail = pg_sys::InvalidBlockNumber;
        rbl.tombstone_block = pg_sys::InvalidBlockNumber;
        rbl.tombstone_bytes = 0;
        rbl.pending_block = pg_sys::InvalidBlockNumber;
        root_block
    };

    let pending_block = {
        let pending_buffer = pgbuffer::BlockBuffer::allocate(rel);
        let pending_block = pending_buffer.block_number();
        log_block_event(rel, "extend", pending_block, "pending_header");
        pending_block
    };
    pending::init_pending(rel, pending_block).context("initialize_index_storage: init pending")?;

    let wal_block = {
        let mut wal_buffer = pgbuffer::BlockBuffer::allocate(rel);
        let wal_block = wal_buffer.block_number();
        log_block_event(rel, "extend", wal_block, "wal_page");
        let wal = wal_buffer
            .as_struct_mut::<WALHeader>(0)
            .context("initialize_index_storage: wal header")?;
        wal.magic = WAL_MAGIC;
        wal.bytes_used = 0;
        wal.head_block = pg_sys::InvalidBlockNumber;
        wal.tail_block = pg_sys::InvalidBlockNumber;
        wal.free_head = pg_sys::InvalidBlockNumber;
        wal.free_max_block = pg_sys::InvalidBlockNumber;
        wal.high_water_block = root_block.max(wal_block).max(pending_block);
        wal_block
    };

    {
        let mut root_buffer = pgbuffer::BlockBuffer::aquire_mut(rel, root_block)
            .context("initialize_index_storage: reacquire root")?;
        let rbl = root_buffer
            .as_struct_mut::<RootBlockList>(0)
            .context("initialize_index_storage: update root header")?;
        rbl.wal_block = wal_block;
        rbl.pending_block = pending_block;
    }

    Ok(IndexBootstrapBlocks {
        root_block,
        wal_block,
        pending_block,
    })
}

fn update_high_water_block_with_meta(
    rel: pg_sys::Relation,
    meta: RootMeta,
    block: u32,
) -> Result<()> {
    if meta.magic != ROOT_MAGIC || meta.version < 5 || meta.wal_block == pg_sys::InvalidBlockNumber
    {
        return Ok(());
    }
    let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, meta.wal_block)?;
    let wal = wal_buf
        .as_struct_mut::<WALHeader>(0)
        .context("wal header")?;
    if wal.high_water_block == pg_sys::InvalidBlockNumber || block > wal.high_water_block {
        wal.high_water_block = block;
    }
    Ok(())
}

pub(crate) fn collect_segment_tree_blocks(
    rel: pg_sys::Relation,
    block: u32,
    out: &mut HashSet<u32>,
) -> Result<()> {
    if !out.insert(block) {
        return Ok(());
    }
    let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, block)?;
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

pub(crate) fn collect_posting_blocks(
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
        let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, block)?;
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
    let start = std::time::Instant::now();
    info!("free_segments start: segments={}", segments.len());
    let mut blocks: HashSet<u32> = HashSet::new();
    for seg in segments {
        if seg.extent_head != pg_sys::InvalidBlockNumber && seg.extent_count > 0 {
            let (extents, extent_pages) =
                segment_extent_list_read(rel, seg.extent_head, seg.extent_count)?;
            for blk in extent_pages {
                blocks.insert(blk);
            }
            for extent in extents {
                let end = extent.start_block.saturating_add(extent.len);
                for blk in extent.start_block..end {
                    blocks.insert(blk);
                }
            }
            continue;
        }
        collect_segment_tree_blocks(rel, seg.block, &mut blocks)?;
        let leaf_blocks = collect_leaf_blocks(rel, seg.block)?;
        for leaf in leaf_blocks {
            let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, leaf)?;
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
    let res = free_blocks(rel, &list);
    info!(
        "free_segments done: segments={} blocks={} elapsed_ms={}",
        segments.len(),
        list.len(),
        start.elapsed().as_millis()
    );
    res
}

pub(crate) fn collect_free_list_blocks(rel: pg_sys::Relation, wal_block: u32) -> Result<Vec<u32>> {
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

fn collect_reachable_blocks(
    rel: pg_sys::Relation,
    rbl: &RootBlockList,
    segments: &[Segment],
) -> Result<HashSet<u32>> {
    let mut used: HashSet<u32> = HashSet::new();
    used.insert(0);
    if rbl.wal_block != pg_sys::InvalidBlockNumber {
        used.insert(rbl.wal_block);
    }
    if rbl.pending_block != pg_sys::InvalidBlockNumber {
        used.insert(rbl.pending_block);
        match crate::storage::pending::collect_all_blocks(rel, rbl.pending_block) {
            Ok(blocks) => used.extend(blocks),
            Err(e) => warning!("failed to collect pending blocks: {e:#}"),
        }
    }
    if rbl.tombstone_block != pg_sys::InvalidBlockNumber {
        used.insert(rbl.tombstone_block);
    }

    if rbl.segment_list_head != pg_sys::InvalidBlockNumber {
        let pages = collect_segment_list_pages(rel, rbl.segment_list_head)?;
        used.extend(pages);
    }

    for seg in segments {
        if seg.extent_head != pg_sys::InvalidBlockNumber && seg.extent_count > 0 {
            let (extents, extent_pages) =
                segment_extent_list_read(rel, seg.extent_head, seg.extent_count)?;
            used.extend(extent_pages);
            for extent in extents {
                let end = extent.start_block.saturating_add(extent.len);
                for blk in extent.start_block..end {
                    used.insert(blk);
                }
            }
            continue;
        }
        collect_segment_tree_blocks(rel, seg.block, &mut used)?;
        let leaf_blocks = collect_leaf_blocks(rel, seg.block)?;
        for leaf in leaf_blocks {
            let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, leaf)?;
            let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
            if header.magic != BLOCK_MAGIC {
                anyhow::bail!("invalid block magic while collecting reachable blocks");
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
    Ok(used)
}

pub fn reclaim_orphan_blocks(
    rel: pg_sys::Relation,
    rbl: &RootBlockList,
    segments: &[Segment],
) -> Result<usize> {
    let start = std::time::Instant::now();
    let nblocks =
        unsafe { pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) };
    if nblocks <= 1 {
        return Ok(0);
    }
    let used = collect_reachable_blocks(rel, rbl, segments)?;
    let free_list_blocks = if rbl.wal_block != pg_sys::InvalidBlockNumber {
        collect_free_list_blocks(rel, rbl.wal_block)?
    } else {
        Vec::new()
    };
    let mut used_blocks = used.iter().copied().collect::<Vec<_>>();
    used_blocks.sort_unstable();
    info!(
        "reclaim_orphan_blocks scan: rel={} relation_blocks={} reachable_blocks={} reachable_sample={} free_list_blocks={} free_list_sample={}",
        unsafe { u32::from((*rel).rd_id) },
        nblocks,
        used.len(),
        sample_blocks(&used_blocks),
        free_list_blocks.len(),
        sample_blocks(&free_list_blocks),
    );
    let free: HashSet<u32> = free_list_blocks.into_iter().collect();
    let mut orphans = Vec::new();
    for blk in 1..nblocks {
        if !used.contains(&blk) && !free.contains(&blk) {
            orphans.push(blk);
        }
    }
    if !orphans.is_empty() {
        info!(
            "reclaim_orphan_blocks victims: rel={} orphan_blocks={} orphan_sample={}",
            unsafe { u32::from((*rel).rd_id) },
            orphans.len(),
            sample_blocks(&orphans),
        );
    }
    free_blocks(rel, &orphans)?;
    info!(
        "reclaim_orphan_blocks done: relation_blocks={} reachable_blocks={} reclaimed_blocks={} elapsed_ms={}",
        nblocks,
        used.len(),
        orphans.len(),
        start.elapsed().as_millis()
    );
    Ok(orphans.len())
}

fn block_is_free_page(rel: pg_sys::Relation, block: u32) -> Result<bool> {
    let buf = pgbuffer::BlockBuffer::acquire(rel, block)?;
    let hdr = buf
        .as_struct::<FreePageHeader>(0)
        .context("free page header")?;
    let magic = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(hdr.magic)) };
    Ok(magic == FREE_PAGE_MAGIC)
}

pub fn maybe_truncate_relation(
    rel: pg_sys::Relation,
    rbl: &RootBlockList,
    _segments: &[Segment],
) -> Result<()> {
    let start = std::time::Instant::now();
    let wal_block = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(rbl.wal_block)) };
    info!(
        "maybe_truncate_relation start: rel={} wal_block={}",
        unsafe { u32::from((*rel).rd_id) },
        wal_block
    );
    let nblocks =
        unsafe { pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) };
    if nblocks <= 1 {
        return Ok(());
    }
    if rbl.version >= 5 && rbl.wal_block != pg_sys::InvalidBlockNumber {
        let wal_buf = pgbuffer::BlockBuffer::acquire(rel, rbl.wal_block)?;
        let wal = wal_buf.as_struct::<WALHeader>(0).context("wal header")?;
        if wal.free_max_block == pg_sys::InvalidBlockNumber {
            info!(
                "maybe_truncate_relation done: truncated=false elapsed_ms={}",
                start.elapsed().as_millis()
            );
            return Ok(());
        }
    }
    let mut new_nblocks = nblocks;
    while new_nblocks > 1 {
        let tail = new_nblocks.saturating_sub(1);
        if !block_is_free_page(rel, tail)? {
            break;
        }
        new_nblocks = tail;
    }
    if new_nblocks >= nblocks {
        info!(
            "maybe_truncate_relation done: truncated=false elapsed_ms={}",
            start.elapsed().as_millis()
        );
        return Ok(());
    }

    let freelist_start = std::time::Instant::now();
    let keep = collect_free_list_blocks(rel, rbl.wal_block)?
        .into_iter()
        .filter(|b| *b < new_nblocks)
        .collect::<Vec<u32>>();
    let freelist_collect_elapsed_ms = freelist_start.elapsed().as_millis();

    let freelist_rewrite_start = std::time::Instant::now();
    if rbl.wal_block != pg_sys::InvalidBlockNumber {
        let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)?;
        let wal = wal_buf
            .as_struct_mut::<WALHeader>(0)
            .context("wal header")?;
        let mut head = pg_sys::InvalidBlockNumber;
        for block in &keep {
            let mut page = pgbuffer::BlockBuffer::aquire_mut(rel, *block)?;
            let header = page
                .as_struct_mut::<FreePageHeader>(0)
                .context("free page header")?;
            header.magic = FREE_PAGE_MAGIC;
            header.next_block = head;
            head = *block;
        }
        wal.free_head = head;
        if rbl.version >= 5 {
            wal.free_max_block = keep
                .iter()
                .copied()
                .max()
                .unwrap_or(pg_sys::InvalidBlockNumber);
            wal.high_water_block = new_nblocks.saturating_sub(1);
        }
    }
    let freelist_rewrite_elapsed_ms = freelist_rewrite_start.elapsed().as_millis();
    info!(
        "maybe_truncate_relation phase=free_list rel={} keep_blocks={} keep_sample={} collect_elapsed_ms={} rewrite_elapsed_ms={}",
        unsafe { u32::from((*rel).rd_id) },
        keep.len(),
        sample_blocks(&keep),
        freelist_collect_elapsed_ms,
        freelist_rewrite_elapsed_ms
    );

    let truncate_start = std::time::Instant::now();
    let truncated_tail = (new_nblocks..nblocks.min(new_nblocks.saturating_add(8))).collect::<Vec<_>>();
    info!(
        "maybe_truncate_relation phase=truncate_decision rel={} old_nblocks={} new_nblocks={} truncated_tail_sample={}",
        unsafe { u32::from((*rel).rd_id) },
        nblocks,
        new_nblocks,
        sample_blocks(&truncated_tail),
    );
    unsafe {
        pg_sys::RelationTruncate(rel, new_nblocks);
    }
    let truncate_elapsed_ms = truncate_start.elapsed().as_millis();
    info!(
        "maybe_truncate_relation phase=truncate old_nblocks={} new_nblocks={} elapsed_ms={}",
        nblocks, new_nblocks, truncate_elapsed_ms
    );
    info!(
        "maybe_truncate_relation done: truncated=true new_nblocks={} elapsed_ms={}",
        new_nblocks,
        start.elapsed().as_millis()
    );
    Ok(())
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct Segments {
    pub entries: [Segment],
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct SegmentsV1 {
    pub entries: [SegmentV1],
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
    pub free_max_block: u32,
    pub high_water_block: u32,
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
        let header = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const BlockHeader) };
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
        let buf = pgbuffer::BlockBuffer::acquire_pinned(self.rel, block)?;
        let header = Self::read_block_header(&buf)?;
        if header.magic != BLOCK_MAGIC {
            anyhow::bail!("invalid block magic while merging");
        }
        let entry = Self::read_block_pointer(&buf, idx, header.num_entries as usize)?;
        Ok(entry.block)
    }

    fn descend_leftmost(&mut self, mut block: u32) -> Result<()> {
        loop {
            let buf = pgbuffer::BlockBuffer::acquire_pinned(self.rel, block)?;
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
    let segment_block = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(segment.block)) };
    let segment_size = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(segment.size)) };
    let leaf_blocks = collect_leaf_blocks(rel, segment_block)?;
    info!(
        "read_segment_entries: rel={} segment_root={} segment_size={} leaf_blocks={} leaf_sample={}",
        unsafe { u32::from((*rel).rd_id) },
        segment_block,
        segment_size,
        leaf_blocks.len(),
        sample_blocks(&leaf_blocks),
    );
    let mut all_entries = Vec::new();
    for leaf_block in leaf_blocks {
        let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, leaf_block)?;
        let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
        if header.magic != BLOCK_MAGIC {
            anyhow::bail!("invalid block magic while merging");
        }
        if header.level != 0 {
            anyhow::bail!("expected leaf page while merging");
        }
        let num_entries = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(header.num_entries)) };
        let entries = buf
            .as_struct_with_elems::<IndexList>(
                std::mem::size_of::<BlockHeader>(),
                num_entries as usize,
            )
            .context("index entries")?;
        let leaf_entries = &entries.entries[..num_entries as usize];
        let posting_sample = leaf_entries
            .iter()
            .take(4)
            .map(|entry| {
                let block = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.block)) };
                let offset = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.offset)) };
                let len =
                    unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.data_length)) };
                format!("{block}:{offset}:{len}")
            })
            .collect::<Vec<_>>()
            .join(",");
        info!(
            "read_segment_entries leaf: rel={} segment_root={} leaf_block={} entries={} posting_sample=[{}]",
            unsafe { u32::from((*rel).rd_id) },
            segment_block,
            leaf_block,
            num_entries,
            posting_sample,
        );
        all_entries.extend_from_slice(leaf_entries);
    }
    Ok(all_entries)
}

pub fn validate_segment_root(rel: pg_sys::Relation, segment: &Segment) -> Result<()> {
    let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, segment.block)?;
    let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
    if header.magic != BLOCK_MAGIC {
        anyhow::bail!("invalid block magic while validating segment root");
    }
    Ok(())
}

pub fn resolve_leaf_for_trigram(
    rel: pg_sys::Relation,
    root_block: u32,
    trigram: u32,
) -> Result<Option<u32>> {
    let mut block = root_block;
    loop {
        let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, block)?;
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
        let buf = pgbuffer::BlockBuffer::acquire_pinned(rel, block)?;
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
    mut on_doc: impl FnMut(ItemPointer, &[(u32, u8)]) -> Result<()>,
) -> Result<()> {
    fn advance_posting_cursor(
        cursors: &mut [crate::storage::decode::PostingCursor],
        heap: &mut BinaryHeap<Reverse<(ItemPointer, usize)>>,
        idx: usize,
        deleted: bool,
        source_count: &mut usize,
        occs: &mut Vec<(u32, u8)>,
    ) -> Result<()> {
        let cursor = &mut cursors[idx];
        if !deleted {
            if let Some(doc) = cursor.current() {
                *source_count = source_count.saturating_add(1);
                occs.reserve(doc.positions.len());
                occs.extend(doc.positions.iter().copied());
            }
        }
        if cursor.advance()? {
            if let Some(next_tid) = cursor.current_tid() {
                heap.push(Reverse((next_tid, idx)));
            }
        }
        Ok(())
    }

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

    let mut occs: Vec<(u32, u8)> = Vec::new();
    while let Some(Reverse((target, idx))) = heap.pop() {
        let mut source_count = 0usize;
        occs.clear();
        let deleted = tombstones.contains(target);

        advance_posting_cursor(
            &mut cursors,
            &mut heap,
            idx,
            deleted,
            &mut source_count,
            &mut occs,
        )?;
        loop {
            let Some(&Reverse((next_tid, _))) = heap.peek() else {
                break;
            };
            if next_tid != target {
                break;
            }
            let Some(Reverse((_, next_idx))) = heap.pop() else {
                break;
            };
            advance_posting_cursor(
                &mut cursors,
                &mut heap,
                next_idx,
                deleted,
                &mut source_count,
                &mut occs,
            )?;
        }

        if deleted {
            continue;
        }

        if !occs.is_empty() {
            if source_count > 1 {
                occs.sort_unstable_by_key(|(position, _)| *position);
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
    fn advance_segment_cursor(
        cursors: &mut [SegmentCursor],
        heap: &mut BinaryHeap<Reverse<(u32, usize)>>,
        group_entries: &mut Vec<IndexEntry>,
        idx: usize,
        trigram: u32,
    ) -> Result<()> {
        let cursor = &mut cursors[idx];
        let Some(entry) = cursor.current_entry() else {
            return Ok(());
        };
        if entry.trigram != trigram {
            return Ok(());
        }
        group_entries.push(*entry);
        cursor.advance()?;
        if let Some(next) = cursor.current_entry() {
            heap.push(Reverse((next.trigram, idx)));
        }
        Ok(())
    }

    let root = pgbuffer::BlockBuffer::acquire(rel, 0)?;
    let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
    let track_extents = rbl.magic == ROOT_MAGIC && rbl.version >= 6;
    let mut tracker = BlockExtentTracker::default();
    let tracker_ptr = if track_extents {
        Some(&mut tracker as *mut _)
    } else {
        None
    };
    let total_bytes = segments
        .iter()
        .map(|segment| segment.size as usize)
        .sum::<usize>();
    info!(
        "merge: segments={} total_bytes={}",
        segments.len(),
        total_bytes
    );

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
            extent_head: pg_sys::InvalidBlockNumber,
            extent_count: 0,
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
    let mut writer =
        crate::storage::encode::PageWriter::new(rel, pgbuffer::SPECIAL_SIZE, tracker_ptr);
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
    let max_chunk_size = pgbuffer::SPECIAL_SIZE - std::mem::size_of::<PostingPageHeader>();

    fn start_leaf(
        rel: pg_sys::Relation,
        leaf: &mut Option<pgbuffer::BlockBuffer>,
        leaf_block: &mut u32,
        leaf_entries_written: &mut usize,
        leaf_min_trigram: &mut Option<u32>,
        tracker: Option<*mut BlockExtentTracker>,
    ) -> Result<()> {
        let mut page = allocate_block_tracked(rel, tracker);
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
    while let Some(Reverse((trigram, idx))) = heap.pop() {
        // Collect all segment entries for this trigram and advance those cursors.
        group_entries.clear();
        advance_segment_cursor(&mut cursors, &mut heap, &mut group_entries, idx, trigram)?;
        loop {
            let Some(&Reverse((next_trigram, _))) = heap.peek() else {
                break;
            };
            if next_trigram != trigram {
                break;
            }
            let Some(Reverse((_, next_idx))) = heap.pop() else {
                break;
            };
            advance_segment_cursor(
                &mut cursors,
                &mut heap,
                &mut group_entries,
                next_idx,
                trigram,
            )?;
        }

        if leaf.is_none() {
            start_leaf(
                rel,
                &mut leaf,
                &mut leaf_block,
                &mut leaf_entries_written,
                &mut leaf_min_trigram,
                tracker_ptr,
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
                tracker_ptr,
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
                unsafe {
                    let result =
                        crate::storage::decode::copy_posting_chunks(rel, entry, &mut writer);
                    if let Err(e) = result {
                        let (block, offset, _len) = entry_fields(entry);
                        warning!(
                            "posting copy failed: trigram={} block={} offset={} length={} err={e:#}",
                            trigram,
                            block,
                            offset,
                            data_length
                        );
                        return Err(e);
                    }
                    if let Ok(Some(loc)) = result {
                        idx_entry.block = loc.block_number;
                        idx_entry.offset = loc.offset as u16;
                    }
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
                    builder.add_raw(doc, &occs[start..start + can_take]);
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
        info!("Encoded {doc_count} docs, {occ_count} occs and {byte_count} bytes");
    } else {
        info!("Encoded {doc_count} docs and {byte_count} bytes (occs elided)");
    }
    let mut segment = Segment {
        block: crate::storage::encode::build_segment_root(rel, &leaf_pointers, tracker_ptr)?,
        size: byte_count,
        extent_head: pg_sys::InvalidBlockNumber,
        extent_count: 0,
    };
    if track_extents {
        let extents = tracker.take();
        segment_attach_extents(rel, &mut segment, &extents)?;
    }
    Ok(segment)
}
