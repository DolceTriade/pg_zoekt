use std::cmp::Ordering;
use std::collections::HashSet;
use std::ffi::CStr;
use std::collections::HashMap;

use anyhow::Context;
use pgrx::datum::{DatumWithOid, FromDatum};
use pgrx::prelude::*;
use zerocopy::TryFromBytes;

#[derive(Debug, Default)]
struct ScanState {
    matches: Vec<pg_sys::ItemPointerData>,
    cursor: usize,
}

impl ScanState {
    fn reset(&mut self) {
        self.matches.clear();
        self.cursor = 0;
    }

    fn push_match(&mut self, item: crate::storage::ItemPointer) {
        let mut tid = pg_sys::ItemPointerData::default();
        let blk = item.block_number;
        let blk_hi = (blk >> 16) as u16;
        let blk_lo = (blk & 0xffff) as u16;
        tid.ip_blkid.bi_hi = blk_hi;
        tid.ip_blkid.bi_lo = blk_lo;
        tid.ip_posid = item.offset;
        self.matches.push(tid);
    }

    fn sort_dedup(&mut self) {
        self.matches.sort_by(|a, b| {
            let a_blk = (a.ip_blkid.bi_hi as u32) << 16 | a.ip_blkid.bi_lo as u32;
            let b_blk = (b.ip_blkid.bi_hi as u32) << 16 | b.ip_blkid.bi_lo as u32;
            match a_blk.cmp(&b_blk) {
                Ordering::Equal => a.ip_posid.cmp(&b.ip_posid),
                other => other,
            }
        });
        self.matches.dedup_by(|a, b| {
            a.ip_posid == b.ip_posid
                && a.ip_blkid.bi_hi == b.ip_blkid.bi_hi
                && a.ip_blkid.bi_lo == b.ip_blkid.bi_lo
        });
    }

    fn next(&mut self) -> Option<pg_sys::ItemPointerData> {
        if self.cursor >= self.matches.len() {
            None
        } else {
            let tid = self.matches[self.cursor];
            self.cursor += 1;
            Some(tid)
        }
    }
}

fn scan_keys_to_pattern(keys: pg_sys::ScanKey, nkeys: i32) -> Option<String> {
    if keys.is_null() || nkeys <= 0 {
        return None;
    }
    unsafe {
        let key = *keys;
        if (key.sk_flags & pg_sys::SK_ISNULL as i32) != 0 {
            return None;
        }
        String::from_datum(key.sk_argument, false)
    }
}

fn extract_trigrams(pattern: &str) -> HashSet<u32> {
    let mut trgms = HashSet::new();
    for (trgm, _) in crate::trgm::Extractor::extract(pattern) {
        if let Ok(ct) = crate::trgm::CompactTrgm::try_from(trgm) {
            trgms.insert(ct.trgm());
        }
    }
    trgms
}

unsafe fn read_segments(rel: pg_sys::Relation) -> anyhow::Result<Vec<crate::storage::Segment>> {
    let mut root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0);
    let rbl = root
        .as_struct::<crate::storage::RootBlockList>(0)
        .context("root header")?;
    if rbl.magic != crate::storage::ROOT_MAGIC {
        anyhow::bail!("invalid root magic");
    }
    let segments = root
        .as_struct_with_elems::<crate::storage::Segments>(
            std::mem::size_of::<crate::storage::RootBlockList>(),
            rbl.num_segments as usize,
        )
        .context("segments")?;
    Ok(segments.entries.to_vec())
}

unsafe fn find_entry_for_trigram(
    rel: pg_sys::Relation,
    block: u32,
    trigram: u32,
) -> anyhow::Result<Option<crate::storage::IndexEntry>> {
    let mut buf = crate::storage::pgbuffer::BlockBuffer::acquire(rel, block);
    let bh = buf
        .as_struct::<crate::storage::BlockHeader>(0)
        .context("block header")?;
    if bh.magic != crate::storage::BLOCK_MAGIC {
        anyhow::bail!("bad block magic");
    }
    let entries = buf
        .as_struct_with_elems::<crate::storage::IndexList>(
            std::mem::size_of::<crate::storage::BlockHeader>(),
            bh.num_entries as usize,
        )
        .context("entry list")?;
    let slice = &entries.entries;
    let pos = slice
        .binary_search_by(|e| {
            let trig = e.trigram;
            trig.cmp(&trigram)
        })
        .ok();
    Ok(pos.map(|idx| slice[idx]))
}

#[derive(Debug, Clone)]
struct PatternTrgm {
    trigram: u32,
    pos: u32,
    flags: u8,
}

#[derive(Debug, Clone)]
struct DocPosting {
    tid: crate::storage::ItemPointer,
    positions: Vec<(u32, u8)>, // (position, flags)
}

fn delta_decode(values: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(values.len());
    let mut acc = 0_u32;
    for v in values {
        acc = acc.wrapping_add(*v);
        out.push(acc);
    }
    out
}

fn take_slice<'a>(
    page: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> anyhow::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .context("overflow while slicing postings")?;
    if end > page.len() {
        anyhow::bail!("postings overrun");
    }
    let slice = &page[*cursor..end];
    *cursor = end;
    Ok(slice)
}

unsafe fn decode_postings(
    rel: pg_sys::Relation,
    entry: &crate::storage::IndexEntry,
) -> anyhow::Result<Vec<DocPosting>> {
    let mut buf = crate::storage::pgbuffer::BlockBuffer::acquire(rel, entry.block);
    let page = buf.as_ref();
    let start = entry.offset as usize;
    if start >= page.len() {
        anyhow::bail!("offset out of bounds");
    }
    let header_bytes = page
        .get(start..start + std::mem::size_of::<crate::storage::CompressedBlockHeader>())
        .context("header slice")?;
    let hdr = *crate::storage::CompressedBlockHeader::try_ref_from_bytes(header_bytes)
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let mut cursor = start + std::mem::size_of::<crate::storage::CompressedBlockHeader>();

    let num_docs = hdr.num_docs as usize;
    let blk_bytes = take_slice(page, &mut cursor, hdr.docs_blk_len as usize)?;
    let mut blk_nums = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::scalar::Scalar>(blk_bytes, num_docs, &mut blk_nums);
    let blk_nums = delta_decode(&blk_nums);

    let off_bytes = take_slice(page, &mut cursor, hdr.docs_off_len as usize)?;
    let mut offs = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::scalar::Scalar>(off_bytes, num_docs, &mut offs);

    let count_bytes = take_slice(page, &mut cursor, hdr.counts_len as usize)?;
    let mut counts = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::scalar::Scalar>(count_bytes, num_docs, &mut counts);

    let total_positions: usize = counts.iter().copied().map(|c| c as usize).sum();
    let pos_bytes = take_slice(page, &mut cursor, hdr.pos_len as usize)?;
    let mut positions = vec![0u32; total_positions];
    if total_positions > 0 {
        stream_vbyte::decode::decode::<stream_vbyte::scalar::Scalar>(
            pos_bytes,
            total_positions,
            &mut positions,
        );
    }
    let positions = delta_decode(&positions);

    let flag_bytes = take_slice(page, &mut cursor, hdr.flags_len as usize)?;
    let flags = bitfield_rle::decode(flag_bytes.to_vec()).unwrap_or_default();

    let mut pos_cursor = 0usize;
    let mut postings = Vec::with_capacity(num_docs);
    for i in 0..num_docs {
        let num_pos = counts[i] as usize;
        let doc_positions = positions
            .get(pos_cursor..pos_cursor + num_pos)
            .unwrap_or(&[]);
        let doc_flags = flags
            .get(pos_cursor..pos_cursor + num_pos)
            .unwrap_or(&[]);
        pos_cursor += num_pos;

        let mut pairs = Vec::with_capacity(doc_positions.len());
        for (p, f) in doc_positions.iter().zip(doc_flags.iter().copied().chain(std::iter::repeat(0)))
        {
            pairs.push((*p, f));
        }

        postings.push(DocPosting {
            tid: crate::storage::ItemPointer {
                block_number: blk_nums[i],
                offset: offs[i] as u16,
            },
            positions: pairs,
        });
    }

    Ok(postings)
}

fn relation_qualified_name(relid: pg_sys::Oid) -> Option<String> {
    unsafe {
        let relname_ptr = pg_sys::get_rel_name(relid);
        if relname_ptr.is_null() {
            return None;
        }
        let relname = CStr::from_ptr(relname_ptr).to_string_lossy();
        let nsp = pg_sys::get_rel_namespace(relid);
        let nspname_ptr = pg_sys::get_namespace_name(nsp);
        let nspname = if nspname_ptr.is_null() {
            None
        } else {
            Some(CStr::from_ptr(nspname_ptr).to_string_lossy().into_owned())
        };
        Some(match nspname {
            Some(n) => format!("\"{}\".\"{}\"", n.replace('"', "\"\""), relname.replace('"', "\"\"")),
            None => relname.into_owned(),
        })
    }
}

fn is_case_sensitive(keys: pg_sys::ScanKey) -> bool {
    if keys.is_null() {
        return true;
    }
    unsafe {
        let strategy = (*keys).sk_strategy as u16;
        matches!(
            strategy,
            crate::operators::STRATEGY_LIKE | crate::operators::STRATEGY_REGEX
        )
    }
}

fn extract_pattern_trigrams(pattern: &str) -> Vec<PatternTrgm> {
    let mut res = Vec::new();
    for (trgm, pos) in crate::trgm::Extractor::extract(pattern) {
        if let Ok(ct) = crate::trgm::CompactTrgm::try_from(trgm) {
            res.push(PatternTrgm {
                trigram: ct.trgm(),
                pos: pos as u32,
                flags: ct.flags(),
            });
        }
    }
    res
}

fn flags_match(doc_flag: u8, pattern_flag: u8, case_sensitive: bool) -> bool {
    if !case_sensitive {
        return true;
    }
    (doc_flag & 0b111) == (pattern_flag & 0b111)
}

unsafe fn build_scan_state(
    index_relation: pg_sys::Relation,
    keys: pg_sys::ScanKey,
    nkeys: std::os::raw::c_int,
) -> ScanState {
    let pattern = scan_keys_to_pattern(keys, nkeys);
    let pattern_str = if let Some(p) = pattern {
        p
    } else {
        return ScanState::default();
    };

    let case_sensitive = is_case_sensitive(keys);
    let pattern_trgms = extract_pattern_trigrams(&pattern_str);
    if pattern_trgms.is_empty() {
        return ScanState::default();
    }

    // Decode postings per trigram.
    let mut trigram_hits: Vec<HashMap<crate::storage::ItemPointer, Vec<(u32, u8)>>> = Vec::new();
    if let Ok(segments) = read_segments(index_relation) {
        for pt in &pattern_trgms {
            let mut map: HashMap<crate::storage::ItemPointer, Vec<(u32, u8)>> = HashMap::new();
            for seg in &segments {
                if let Ok(Some(entry)) =
                    find_entry_for_trigram(index_relation, seg.block, pt.trigram)
                {
                    match decode_postings(index_relation, &entry) {
                        Ok(postings) => {
                            for doc in postings {
                                map.entry(doc.tid)
                                    .or_default()
                                    .extend(doc.positions.into_iter());
                            }
                        }
                        Err(e) => warning!("failed to decode postings: {e:#}"),
                    }
                }
            }
            trigram_hits.push(map);
        }
    }

    // Intersect postings with positional alignment.
    let mut state = ScanState::default();
    if trigram_hits.iter().any(|m| m.is_empty()) {
        return state;
    }
    let anchor_hits = &trigram_hits[0];
    let anchor_pt = &pattern_trgms[0];
    let anchor_pattern_pos = anchor_pt.pos as i64;

    for (tid, positions) in anchor_hits {
        for (anchor_pos, anchor_flag) in positions {
            let mut ok = true;
            for (idx, pt) in pattern_trgms.iter().enumerate().skip(1) {
                let delta = pt.pos as i64 - anchor_pattern_pos;
                if delta.is_negative() && (*anchor_pos as i64) < -delta {
                    ok = false;
                    break;
                }
                let target = (*anchor_pos as i64 + delta) as u32;
                let Some(doc_positions) = trigram_hits[idx].get(tid) else {
                    ok = false;
                    break;
                };
                let mut found = false;
                for (p, f) in doc_positions {
                    if *p == target && flags_match(*f, pt.flags, case_sensitive) {
                        found = true;
                        break;
                    }
                }
                if !found {
                    ok = false;
                    break;
                }
            }
            if ok && flags_match(*anchor_flag, anchor_pt.flags, case_sensitive) {
                state.push_match(*tid);
                break;
            }
        }
    }

    state.sort_dedup();
    state
}

pub unsafe extern "C-unwind" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: std::os::raw::c_int,
    norderbys: std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let scan = pg_sys::RelationGetIndexScan(index_relation, nkeys, norderbys);
    if scan.is_null() {
        return std::ptr::null_mut();
    }

    // Keys arrive via amrescan; start with an empty state.
    let state = ScanState::default();
    (*scan).opaque = Box::into_raw(Box::new(state)) as *mut std::ffi::c_void;
    scan
}

pub unsafe extern "C-unwind" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: std::os::raw::c_int,
) {
    if scan.is_null() {
        return;
    }
    let state_ptr = (*scan).opaque as *mut ScanState;
    let state_ref = if let Some(state) = state_ptr.as_mut() {
        state
    } else {
        let boxed = Box::new(ScanState::default());
        (*scan).opaque = Box::into_raw(boxed) as *mut _;
        (*scan).opaque as *mut ScanState
    };
    let new_state = build_scan_state((*scan).indexRelation, keys, nkeys);
    *state_ref = new_state;
}

pub unsafe extern "C-unwind" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    if scan.is_null() {
        return false;
    }
    let state_ptr = (*scan).opaque as *mut ScanState;
    let Some(state) = state_ptr.as_mut() else {
        return false;
    };
    if let Some(tid) = state.next() {
        (*scan).xs_heaptid = tid;
        (*scan).xs_recheck = true; // Caller should recheck the LIKE/regex predicate.
        true
    } else {
        false
    }
}

pub unsafe extern "C-unwind" fn amendscan(scan: pg_sys::IndexScanDesc) {
    if scan.is_null() {
        return;
    }
    let opaque = (*scan).opaque as *mut ScanState;
    if !opaque.is_null() {
        _ = Box::from_raw(opaque);
        (*scan).opaque = std::ptr::null_mut();
    }
}

#[allow(clippy::too_many_arguments)]
pub unsafe extern "C-unwind" fn amcostestimate(
    root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    // Basic cost model: heavily bias toward seq scan until index execution is mature.
    // Still provide sane estimates to keep the planner stable.
    let indexinfo = path.as_ref().and_then(|p| unsafe { p.indexinfo.as_ref() });

    let pages = indexinfo.map(|i| i.pages as f64).unwrap_or(1000.0).max(1.0);
    let tuples = indexinfo.map(|i| i.tuples as f64).unwrap_or(10000.0).max(1.0);

    if !index_pages.is_null() {
        unsafe {
            *index_pages = pages;
        }
    }

    let selectivity = (1.0 / tuples.sqrt()).clamp(0.0001, 0.25);
    if !index_selectivity.is_null() {
        unsafe {
            *index_selectivity = selectivity;
        }
    }

    let startup = 100.0 + loop_count;
    let total = startup + pages * selectivity * 50.0;

    if !index_startup_cost.is_null() {
        unsafe { *index_startup_cost = startup };
    }
    if !index_total_cost.is_null() {
        unsafe { *index_total_cost = total };
    }
    if !index_correlation.is_null() {
        unsafe { *index_correlation = 0.0 };
    }
}
