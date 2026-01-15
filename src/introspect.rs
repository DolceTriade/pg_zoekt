use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use std::collections::BTreeMap;

use crate::storage::{IndexEntry, Segment, SegmentExtent, WALHeader};
use crate::trgm::CompactTrgm;

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
pub fn pg_zoekt_segment_extents(
    index: pg_sys::Oid,
    segment_idx: i32,
) -> TableIterator<'static, (name!(start_block, i64), name!(len, i64))> {
    let mut rows = Vec::new();
    unsafe {
        let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
        let segments = crate::query::read_segments(rel).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read segments: {e:#?}");
        });
        let segment = segment_by_index(&segments, segment_idx);
        if segment.extent_head != pg_sys::InvalidBlockNumber && segment.extent_count > 0 {
            let (extents, _pages) = crate::storage::segment_extent_list_read(
                rel,
                segment.extent_head,
                segment.extent_count,
            )
            .unwrap_or_else(|e| {
                pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                error!("failed to read segment extents: {e:#?}");
            });
            for SegmentExtent { start_block, len } in extents {
                rows.push((start_block as i64, len as i64));
            }
        }
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    }
    TableIterator::new(rows.into_iter())
}

#[pg_extern]
pub fn pg_zoekt_trigram_to_text(trigram: i64) -> String {
    if trigram < 0 || trigram > u32::MAX as i64 {
        error!("trigram out of range: {}", trigram);
    }
    let value = trigram as u32;
    let b0 = (value & 0xff) as u8;
    let b1 = ((value >> 8) & 0xff) as u8;
    let b2 = ((value >> 16) & 0xff) as u8;
    if b0 == 0 || b1 == 0 || b2 == 0 {
        error!("trigram has zero byte: {}", trigram);
    }
    CompactTrgm(value).txt()
}

#[pg_extern]
pub fn pg_zoekt_text_to_trigram(trigram: &str) -> i64 {
    let ct = CompactTrgm::try_from(trigram).unwrap_or_else(|e| {
        error!("invalid trigram: {e:#?}");
    });
    ct.trgm() as i64
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
        name!(ctid, pg_sys::ItemPointerData),
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
                let ctid = pg_sys::ItemPointerData {
                    ip_blkid: pg_sys::BlockIdData {
                        bi_hi: (doc.tid.block_number >> 16) as u16,
                        bi_lo: (doc.tid.block_number & 0xffff) as u16,
                    },
                    ip_posid: doc.tid.offset,
                };
                rows.push((
                    ctid,
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
pub fn pg_zoekt_posting_positions(
    index: pg_sys::Oid,
    segment_idx: i32,
    trigram: i64,
    ctid: pg_sys::ItemPointerData,
) -> TableIterator<'static, (name!(position, i64), name!(flags, i32))> {
    let mut rows = Vec::new();
    let trigram_u32 = trigram as u32;
    let target: crate::storage::ItemPointer = ctid.into();
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
            loop {
                if !cursor.advance().unwrap_or_else(|e| {
                    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                    error!("failed to advance posting cursor: {e:#?}");
                }) {
                    break;
                }
                let doc = cursor.current().expect("cursor should have doc");
                if doc.tid != target {
                    continue;
                }
                for (pos, flags) in doc.positions.iter() {
                    rows.push((*pos as i64, *flags as i32));
                }
                break;
            }
            break;
        }
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    }
    TableIterator::new(rows.into_iter())
}

#[pg_extern]
pub fn pg_zoekt_wal_stats(
    index: pg_sys::Oid,
) -> TableIterator<
    'static,
    (
        name!(wal_block, i64),
        name!(free_head, i64),
        name!(free_max_block, i64),
        name!(high_water_block, i64),
    ),
> {
    let mut rows = Vec::new();
    unsafe {
        let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
        let root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0).unwrap_or_else(|e| {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("failed to read root: {e:#?}");
        });
        let rbl = root
            .as_struct::<crate::storage::RootBlockList>(0)
            .unwrap_or_else(|e| {
                pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                error!("failed to read root header: {e:#?}");
            });
        if rbl.magic != crate::storage::ROOT_MAGIC {
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            error!("invalid root magic");
        }
        if rbl.wal_block != pg_sys::InvalidBlockNumber {
            let wal_buf = crate::storage::pgbuffer::BlockBuffer::acquire(rel, rbl.wal_block)
                .unwrap_or_else(|e| {
                    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                    error!("failed to read wal block: {e:#?}");
                });
            let wal = wal_buf.as_struct::<WALHeader>(0).unwrap_or_else(|e| {
                pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                error!("failed to read wal header: {e:#?}");
            });
            rows.push((
                rbl.wal_block as i64,
                wal.free_head as i64,
                wal.free_max_block as i64,
                wal.high_water_block as i64,
            ));
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
        let mut root_version: u16 = 0;
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
                root_version = rbl.version;
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
                let seg_size = if root_version >= 6 {
                    std::mem::size_of::<Segment>()
                } else {
                    std::mem::size_of::<crate::storage::SegmentV1>()
                };
                let payload = (header.count as usize).saturating_mul(seg_size);
                page_type = "segment_list".to_string();
                (
                    header_size,
                    payload,
                    special_size.saturating_sub(header_size + payload),
                )
            } else if magic == crate::storage::SEGMENT_EXTENT_MAGIC {
                let header = buf
                    .as_struct::<crate::storage::SegmentExtentListPageHeader>(0)
                    .unwrap_or_else(|e| error!("segment extent header: {e:#?}"));
                let header_size =
                    std::mem::size_of::<crate::storage::SegmentExtentListPageHeader>();
                let payload = (header.count as usize)
                    .saturating_mul(std::mem::size_of::<crate::storage::SegmentExtent>());
                page_type = "segment_extent".to_string();
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
