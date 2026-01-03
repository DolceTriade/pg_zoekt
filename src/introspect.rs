use pgrx::iter::TableIterator;
use pgrx::prelude::*;

use crate::storage::{IndexEntry, Segment};

fn segment_by_index(segments: &[Segment], segment_idx: i32) -> Segment {
    if segment_idx < 1 {
        error!("segment_idx must be >= 1");
    }
    let idx = (segment_idx - 1) as usize;
    let seg = segments.get(idx).unwrap_or_else(|| error!("segment_idx out of range"));
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
        let entries =
            crate::storage::read_segment_entries(rel, &segment).unwrap_or_else(|e| {
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
        let entries =
            crate::storage::read_segment_entries(rel, &segment).unwrap_or_else(|e| {
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
