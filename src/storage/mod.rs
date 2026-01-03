use anyhow::{Context, Result};
use pgrx::prelude::*;
/// Storing stuff
use std::collections::{BTreeMap, HashSet};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

pub const TARGET_SEGMENTS: usize = 10;

pub mod decode;
pub mod encode;
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

pub fn maintenance_lock_try(rel: pg_sys::Relation) -> Option<MaintenanceLockGuard> {
    maintenance_lock(rel, MaintenanceLockMode::Try)
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

#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy, PartialEq, Eq)]
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

fn segment_list_capacity() -> usize {
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
    let wal = wal_buf.as_struct_mut::<WALHeader>(0).context("wal header")?;
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
            head, magic, FREE_PAGE_MAGIC
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
    let wal = wal_buf.as_struct_mut::<WALHeader>(0).context("wal header")?;
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
                blk, magic, FREE_PAGE_MAGIC
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
    let nblocks = unsafe {
        pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM)
    };
    if new_nblocks >= nblocks {
        return Ok(());
    }

    let keep = collect_free_list_blocks(rel, rbl.wal_block)?
        .into_iter()
        .filter(|b| *b < new_nblocks)
        .collect::<Vec<u32>>();

    if rbl.wal_block != pg_sys::InvalidBlockNumber {
        let mut wal_buf = pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)?;
        let wal = wal_buf.as_struct_mut::<WALHeader>(0).context("wal header")?;
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

struct SegmentCursor {
    entries: Vec<IndexEntry>,
    idx: usize,
}

impl SegmentCursor {
    fn current_entry(&self) -> Option<&IndexEntry> {
        self.entries.get(self.idx)
    }

    fn advance(&mut self) {
        self.idx += 1;
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

fn peek_next_trigram(cursors: &[SegmentCursor]) -> Option<u32> {
    cursors
        .iter()
        .filter_map(|c| c.current_entry().map(|entry| entry.trigram))
        .min()
}

fn merge_entry_postings(
    rel: pg_sys::Relation,
    entries: &[IndexEntry],
    tombstones: &tombstone::Snapshot,
) -> Result<BTreeMap<ItemPointer, Vec<crate::trgm::Occurance>>> {
    let mut docs: BTreeMap<ItemPointer, Vec<crate::trgm::Occurance>> = BTreeMap::new();
    for entry in entries {
        for posting in unsafe { crate::storage::decode::decode_postings(rel, entry)? } {
            if tombstones.contains(posting.tid) {
                continue;
            }
            let list = docs.entry(posting.tid).or_insert_with(Vec::new);
            for (position, flags) in posting.positions {
                let mut occ = crate::trgm::Occurance(position);
                occ.set_flags(flags);
                list.push(occ);
            }
        }
    }
    Ok(docs)
}

fn flush_collector(
    rel: pg_sys::Relation,
    collector: &mut crate::trgm::Collector,
    target: &mut Vec<Segment>,
) -> Result<()> {
    let trgms = collector.take_trgms();
    if trgms.is_empty() {
        return Ok(());
    }
    let mut segments = encode::Encoder::encode_trgms(rel, &trgms)?;
    target.append(&mut segments);
    Ok(())
}

pub fn merge(
    rel: pg_sys::Relation,
    segments: &[Segment],
    target_segments: usize,
    flush_threshold: usize,
    tombstones: &tombstone::Snapshot,
) -> Result<Vec<Segment>> {
    let target_segments = target_segments.max(1);
    if segments.len() <= target_segments {
        return Ok(segments.to_vec());
    }

    let total_bytes = segments
        .iter()
        .map(|segment| segment.size)
        .sum::<u64>()
        .min(usize::MAX as u64) as usize;
    let per_segment_target = std::cmp::max(1usize, total_bytes / target_segments);
    info!(
        "merge: segments={} target_segments={} total_bytes={} per_segment_target={} flush_threshold={}",
        segments.len(),
        target_segments,
        total_bytes,
        per_segment_target,
        flush_threshold
    );

    let mut cursors = Vec::new();
    for segment in segments {
        let entries = read_segment_entries(rel, segment)?;
        if !entries.is_empty() {
            cursors.push(SegmentCursor { entries, idx: 0 });
        }
    }

    if cursors.is_empty() {
        return Ok(Vec::new());
    }

    let mut collector = crate::trgm::Collector::new();
    let mut bytes_since_flush = 0usize;
    let mut processed_bytes: u64 = 0;
    let mut result = Vec::new();
    let mut interrupt_counter: u32 = 0;
    let mut flush_count: u64 = 0;

    while let Some(trigram) = peek_next_trigram(&cursors) {
        interrupt_counter = interrupt_counter.wrapping_add(1);
        if (interrupt_counter & 0x3ff) == 0 {
            pg_sys::check_for_interrupts!();
        }
        let mut group_entries = Vec::new();
        for cursor in cursors.iter_mut() {
            while let Some(entry) = cursor.current_entry() {
                if entry.trigram != trigram {
                    break;
                }
                group_entries.push(*entry);
                cursor.advance();
            }
        }
        let mut postings = merge_entry_postings(rel, &group_entries, tombstones)?;
        for (doc, occs) in postings.iter_mut() {
            occs.sort_unstable_by_key(|occ| occ.position());
            collector.add_occurrences(trigram, *doc, occs);
        }
        drop(postings);
        let group_bytes = group_entries
            .iter()
            .map(|entry| entry.data_length as usize)
            .sum::<usize>();
        bytes_since_flush += group_bytes;
        processed_bytes = processed_bytes.saturating_add(group_bytes as u64);
        if collector.memory_usage() >= flush_threshold || bytes_since_flush >= per_segment_target {
            let collector_bytes = collector.memory_usage();
            flush_collector(rel, &mut collector, &mut result)?;
            flush_count = flush_count.saturating_add(1);
            if (flush_count & 0x3f) == 0 {
                let pct = if total_bytes == 0 {
                    0.0
                } else {
                    (processed_bytes as f64 / total_bytes as f64) * 100.0
                };
                info!(
                    "merge flush: count={} collector_bytes={} bytes_since_flush={} result_segments={} progress_pct={:.1}",
                    flush_count,
                    collector_bytes,
                    bytes_since_flush,
                    result.len(),
                    pct
                );
            }
            bytes_since_flush = 0;
        }
    }

    if collector.memory_usage() > 0 {
        flush_collector(rel, &mut collector, &mut result)?;
    }

    Ok(result)
}
