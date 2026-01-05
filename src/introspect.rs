use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use std::collections::BTreeMap;

use crate::storage::{IndexEntry, Segment};

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct PendingHeader {
    magic: u32,
    bytes_used: u32,
    head_block: u32,
    tail_block: u32,
    free_head: u32,
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct PendingBucket {
    magic: u16,
    free: u16,
    next_block: u32,
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct TombstonePageHeader {
    magic: u32,
    used: u16,
    next_block: u32,
}

#[derive(Default, Clone)]
struct OverheadStats {
    pages: u64,
    bytes_pg_header: u64,
    bytes_header: u64,
    bytes_payload: u64,
    bytes_free: u64,
    bytes_unknown: u64,
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    if offset + 4 > bytes.len() {
        return 0;
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_ne_bytes(buf)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    if offset + 2 > bytes.len() {
        return 0;
    }
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_ne_bytes(buf)
}

fn record_page(
    stats: &mut OverheadStats,
    header_bytes: usize,
    payload_bytes: usize,
    free_bytes: usize,
    special_size: usize,
    pg_header_bytes: usize,
) {
    stats.pages += 1;
    stats.bytes_pg_header = stats.bytes_pg_header.saturating_add(pg_header_bytes as u64);
    stats.bytes_header = stats.bytes_header.saturating_add(header_bytes as u64);
    stats.bytes_payload = stats.bytes_payload.saturating_add(payload_bytes as u64);
    stats.bytes_free = stats.bytes_free.saturating_add(free_bytes as u64);
    let used = header_bytes
        .saturating_add(payload_bytes)
        .saturating_add(free_bytes);
    let unknown = special_size.saturating_sub(used);
    stats.bytes_unknown = stats.bytes_unknown.saturating_add(unknown as u64);
}

fn segment_by_index(segments: &[Segment], segment_idx: i32) -> Segment {
    if segment_idx < 1 {
        error!("segment_idx must be >= 1");
    }
    let idx = (segment_idx - 1) as usize;
    let seg = segments
        .get(idx)
        .unwrap_or_else(|| error!("segment_idx out of range"));
    *seg
}

fn entry_fields(entry: &IndexEntry) -> (u32, u32, u16, u32, u32) {
    let trigram = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.trigram)) };
    let block = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.block)) };
    let offset = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.offset)) };
    let data_length = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.data_length)) };
    let frequency = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.frequency)) };
    (trigram, block, offset, data_length, frequency)
}

#[pg_extern]
pub fn pg_zoekt_index_segments(
    index: pg_sys::Oid,
) -> TableIterator<'static, (name!(segment_idx, i32), name!(block, i64), name!(size, i64))> {
    let mut rows = Vec::new();
    unsafe {
        let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
        let segments = crate::query::read_segments(rel).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read segments: {e:#?}");
        });
        for (idx, seg) in segments.iter().enumerate() {
            rows.push(((idx + 1) as i32, seg.block as i64, seg.size as i64));
        }
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    }
    TableIterator::new(rows.into_iter())
}

#[pg_extern]
pub fn pg_zoekt_segment_entries(
    index: pg_sys::Oid,
    segment_idx: i32,
) -> TableIterator<
    'static,
    (
        name!(trigram, i64),
        name!(block, i64),
        name!(offset, i32),
        name!(data_length, i64),
        name!(frequency, i64),
    ),
> {
    let mut rows = Vec::new();
    unsafe {
        let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
        let segments = crate::query::read_segments(rel).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read segments: {e:#?}");
        });
        let segment = segment_by_index(&segments, segment_idx);
        let entries = crate::storage::read_segment_entries(rel, &segment).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read segment entries: {e:#?}");
        });
        for entry in entries.iter() {
            let (trigram, block, offset, data_length, frequency) = entry_fields(entry);
            rows.push((
                trigram as i64,
                block as i64,
                offset as i32,
                data_length as i64,
                frequency as i64,
            ));
        }
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    }
    TableIterator::new(rows.into_iter())
}

#[pg_extern]
pub fn pg_zoekt_postings_preview(
    index: pg_sys::Oid,
    segment_idx: i32,
    trigram: i64,
    max_docs: i32,
) -> TableIterator<
    'static,
    (
        name!(block, i64),
        name!(offset, i32),
        name!(positions_count, i32),
        name!(first_position, i32),
        name!(last_position, i32),
    ),
> {
    let limit = max_docs.max(0) as usize;
    if limit == 0 {
        return TableIterator::new(std::iter::empty());
    }
    let mut rows = Vec::new();
    let trigram_u32 = trigram as u32;
    unsafe {
        let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
        let segments = crate::query::read_segments(rel).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read segments: {e:#?}");
        });
        let segment = segment_by_index(&segments, segment_idx);
        let entries = crate::storage::read_segment_entries(rel, &segment).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read segment entries: {e:#?}");
        });
        for entry in entries.iter() {
            let (entry_trigram, _, _, _, _) = entry_fields(entry);
            if entry_trigram != trigram_u32 {
                continue;
            }
            let mut cursor =
                crate::storage::decode::PostingCursor::new(rel, entry).unwrap_or_else(|e| {
                    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                    error!("failed to open posting cursor: {e:#?}");
                });
            let mut seen = 0usize;
            while seen < limit {
                if !cursor.advance().unwrap_or_else(|e| {
                    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                    error!("failed to advance posting cursor: {e:#?}");
                }) {
                    break;
                }
                let doc = cursor.current().expect("cursor should have doc");
                let positions_count = doc.positions.len() as i32;
                let (first, last) = if doc.positions.is_empty() {
                    (0i32, 0i32)
                } else {
                    let first_pos = doc.positions.first().map(|p| p.0).unwrap_or(0) as i32;
                    let last_pos = doc.positions.last().map(|p| p.0).unwrap_or(0) as i32;
                    (first_pos, last_pos)
                };
                rows.push((
                    doc.tid.block_number as i64,
                    doc.tid.offset as i32,
                    positions_count,
                    first,
                    last,
                ));
                seen += 1;
            }
        }
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    }
    TableIterator::new(rows.into_iter())
}

#[pg_extern]
pub fn pg_zoekt_index_overhead(
    index: pg_sys::Oid,
) -> TableIterator<
    'static,
    (
        name!(page_type, String),
        name!(pages, i64),
        name!(bytes_total, i64),
        name!(bytes_pg_header, i64),
        name!(bytes_header, i64),
        name!(bytes_payload, i64),
        name!(bytes_free, i64),
        name!(bytes_unknown, i64),
    ),
> {
    let mut stats = BTreeMap::<String, OverheadStats>::new();
    unsafe {
        let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
        let nblocks =
            pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM) as usize;
        let special_size = crate::storage::pgbuffer::SPECIAL_SIZE;
        let pg_header_bytes = (pg_sys::BLCKSZ as usize).saturating_sub(special_size);
        for block in 0..nblocks {
            let buf = crate::storage::pgbuffer::BlockBuffer::acquire(rel, block as u32)
                .unwrap_or_else(|e| error!("failed to read block {block}: {e:#?}"));
            let bytes = buf.as_ref();
            let magic = read_u32(bytes, 0);
            let mut page_type = "unknown".to_string();
            let (header_bytes, payload_bytes, free_bytes) = if magic == crate::storage::ROOT_MAGIC {
                let rbl = buf
                    .as_struct::<crate::storage::RootBlockList>(0)
                    .unwrap_or_else(|e| error!("root header: {e:#?}"));
                if rbl.version >= 2 {
                    let header_size = std::mem::size_of::<crate::storage::RootBlockList>();
                    page_type = "root".to_string();
                    (header_size, 0, special_size.saturating_sub(header_size))
                } else {
                    let rbl1 = buf
                        .as_struct::<crate::storage::RootBlockListV1>(0)
                        .unwrap_or_else(|e| error!("root header v1: {e:#?}"));
                    let header_size = std::mem::size_of::<crate::storage::RootBlockListV1>();
                    let payload =
                        (rbl1.num_segments as usize).saturating_mul(std::mem::size_of::<Segment>());
                    page_type = "root_v1".to_string();
                    (
                        header_size,
                        payload,
                        special_size.saturating_sub(header_size + payload),
                    )
                }
            } else if magic == crate::storage::BLOCK_MAGIC {
                let header = buf
                    .as_struct::<crate::storage::BlockHeader>(0)
                    .unwrap_or_else(|e| error!("block header: {e:#?}"));
                let header_size = std::mem::size_of::<crate::storage::BlockHeader>();
                let entry_size = if header.level == 0 {
                    std::mem::size_of::<crate::storage::IndexEntry>()
                } else {
                    std::mem::size_of::<crate::storage::BlockPointer>()
                };
                let payload = (header.num_entries as usize).saturating_mul(entry_size);
                page_type = if header.level == 0 {
                    "block_leaf".to_string()
                } else {
                    "block_internal".to_string()
                };
                (
                    header_size,
                    payload,
                    special_size.saturating_sub(header_size + payload),
                )
            } else if magic == crate::storage::POSTING_PAGE_MAGIC {
                let header = buf
                    .as_struct::<crate::storage::PostingPageHeader>(0)
                    .unwrap_or_else(|e| error!("posting header: {e:#?}"));
                let header_size = std::mem::size_of::<crate::storage::PostingPageHeader>();
                let header_bytes = header.next_offset as usize;
                let free_end = header.free as usize;
                let payload = free_end.saturating_sub(header_bytes);
                let free = special_size.saturating_sub(free_end);
                page_type = "posting".to_string();
                (header_bytes.max(header_size), payload, free)
            } else if magic == crate::storage::SEGMENT_LIST_MAGIC {
                let header = buf
                    .as_struct::<crate::storage::SegmentListPageHeader>(0)
                    .unwrap_or_else(|e| error!("segment list header: {e:#?}"));
                let header_size = std::mem::size_of::<crate::storage::SegmentListPageHeader>();
                let payload =
                    (header.count as usize).saturating_mul(std::mem::size_of::<Segment>());
                page_type = "segment_list".to_string();
                (
                    header_size,
                    payload,
                    special_size.saturating_sub(header_size + payload),
                )
            } else if magic == crate::storage::TOMBSTONE_PAGE_MAGIC {
                let header: TombstonePageHeader =
                    std::ptr::read_unaligned(bytes.as_ptr() as *const TombstonePageHeader);
                let header_size = std::mem::size_of::<TombstonePageHeader>();
                let payload = header.used as usize;
                let capacity = crate::storage::pgbuffer::SPECIAL_SIZE - header_size;
                page_type = "tombstone".to_string();
                (header_size, payload, capacity.saturating_sub(payload))
            } else if magic == crate::storage::WAL_MAGIC {
                let header_size = std::mem::size_of::<crate::storage::WALHeader>();
                page_type = "wal".to_string();
                (header_size, 0, special_size.saturating_sub(header_size))
            } else if magic == crate::storage::FREE_PAGE_MAGIC {
                let header_size = std::mem::size_of::<crate::storage::FreePageHeader>();
                page_type = "free".to_string();
                (header_size, 0, special_size.saturating_sub(header_size))
            } else if magic == crate::storage::PENDING_MAGIC {
                let header_size = std::mem::size_of::<PendingHeader>();
                page_type = "pending_header".to_string();
                (header_size, 0, special_size.saturating_sub(header_size))
            } else {
                let bucket_magic = read_u16(bytes, 0);
                if bucket_magic == crate::storage::PENDING_BUCKET_MAGIC {
                    let header: PendingBucket =
                        std::ptr::read_unaligned(bytes.as_ptr() as *const PendingBucket);
                    let header_size = std::mem::size_of::<PendingBucket>();
                    let capacity = crate::storage::pgbuffer::SPECIAL_SIZE - header_size;
                    let payload = capacity.saturating_sub(header.free as usize);
                    page_type = "pending_bucket".to_string();
                    (header_size, payload, header.free as usize)
                } else {
                    (0, 0, special_size)
                }
            };
            let entry = stats.entry(page_type).or_default();
            record_page(
                entry,
                header_bytes,
                payload_bytes,
                free_bytes,
                special_size,
                pg_header_bytes,
            );
        }
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    }

    let mut rows = Vec::new();
    for (page_type, s) in stats {
        let bytes_total = (s.pages as i64) * (pg_sys::BLCKSZ as i64);
        rows.push((
            page_type,
            s.pages as i64,
            bytes_total,
            s.bytes_pg_header as i64,
            s.bytes_header as i64,
            s.bytes_payload as i64,
            s.bytes_free as i64,
            s.bytes_unknown as i64,
        ));
    }
    TableIterator::new(rows.into_iter())
}
