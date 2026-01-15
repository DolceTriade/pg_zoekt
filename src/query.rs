use anyhow::Context;
use pgrx::datum::FromDatum;
use pgrx::list::PgList;
use pgrx::prelude::*;
use std::cmp::Ordering;

type PostingCursor = crate::storage::decode::PostingCursor;

#[derive(Debug)]
struct ScanState {
    matches: Vec<pg_sys::ItemPointerData>,
    cursor: usize,
    lossy: bool,
    tombstones: crate::storage::tombstone::Snapshot,
}

impl Default for ScanState {
    fn default() -> Self {
        Self {
            matches: Vec::new(),
            cursor: 0,
            lossy: false,
            tombstones: crate::storage::tombstone::Snapshot::default(),
        }
    }
}

impl ScanState {
    fn push_match(&mut self, item: crate::storage::ItemPointer) {
        if self.tombstones.contains(item) {
            return;
        }
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

#[derive(Debug, Clone)]
struct PatternTrgm {
    trigram: u32,
    pos: u32,
    flags: u8,
}

#[derive(Debug, Clone)]
struct SegmentPattern {
    trigrams: Vec<PatternTrgm>,
}

const LOSSY_FLAG: u8 = 0x80;

pub unsafe fn read_segments(rel: pg_sys::Relation) -> anyhow::Result<Vec<crate::storage::Segment>> {
    let root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0)?;
    let rbl = root
        .as_struct::<crate::storage::RootBlockList>(0)
        .context("root header")?;
    if rbl.magic != crate::storage::ROOT_MAGIC {
        anyhow::bail!("invalid root magic");
    }

    if rbl.version >= 2 {
        return crate::storage::segment_list_read(rel, rbl);
    }

    // Legacy v1: segments stored inline right after the root header.
    let rbl1 = root
        .as_struct::<crate::storage::RootBlockListV1>(0)
        .context("root header v1")?;
    let segments = root
        .as_struct_with_elems::<crate::storage::SegmentsV1>(
            std::mem::size_of::<crate::storage::RootBlockListV1>(),
            rbl1.num_segments as usize,
        )
        .context("segments")?;
    Ok(segments
        .entries
        .iter()
        .map(|seg| crate::storage::Segment {
            block: seg.block,
            size: seg.size,
            extent_head: pg_sys::InvalidBlockNumber,
            extent_count: 0,
        })
        .collect())
}

unsafe fn find_entry_for_trigram(
    rel: pg_sys::Relation,
    block: u32,
    trigram: u32,
) -> anyhow::Result<Option<crate::storage::IndexEntry>> {
    let Some(leaf_block) = crate::storage::resolve_leaf_for_trigram(rel, block, trigram)? else {
        return Ok(None);
    };
    let buf = crate::storage::pgbuffer::BlockBuffer::acquire(rel, leaf_block)?;
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

fn is_case_sensitive(keys: pg_sys::ScanKey) -> bool {
    if keys.is_null() {
        return true;
    }
    unsafe {
        match (*keys).sk_strategy as u16 {
            crate::operators::STRATEGY_ILIKE | crate::operators::STRATEGY_IREGEX => false,
            crate::operators::STRATEGY_LIKE | crate::operators::STRATEGY_REGEX => true,
            _ => true,
        }
    }
}

fn is_regex_strategy(keys: pg_sys::ScanKey) -> bool {
    if keys.is_null() {
        return false;
    }
    unsafe {
        matches!(
            (*keys).sk_strategy as u16,
            crate::operators::STRATEGY_REGEX | crate::operators::STRATEGY_IREGEX
        )
    }
}

fn extract_literal_segments(pattern: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '%' => {
                if !current.is_empty() {
                    segments.push(std::mem::take(&mut current));
                }
            }
            '_' => {
                // single-char wildcard: terminate current literal run
                if !current.is_empty() {
                    segments.push(std::mem::take(&mut current));
                }
            }
            '\\' => {
                // escape: treat next char as literal if present
                if let Some(n) = chars.next() {
                    current.push(n);
                }
            }
            other => current.push(other),
        }
    }
    if !current.is_empty() {
        segments.push(current);
    }
    segments
}

fn has_short_literal_segment(pattern: &str) -> bool {
    extract_literal_segments(pattern)
        .into_iter()
        .any(|seg| seg.chars().count() < 3)
}

fn quantifier_is_optional(spec: &str) -> bool {
    let mut iter = spec.splitn(2, ',');
    let min_str = iter.next().unwrap_or("");
    let min = min_str.trim().parse::<u32>().ok();
    match min {
        Some(0) | None => true,
        Some(_) => false,
    }
}

fn regex_to_wildcard_pattern(pattern: &str) -> String {
    let mut out = String::new();
    let mut current = String::new();
    let mut chars = pattern.chars().peekable();

    let flush_current = |out: &mut String, current: &mut String| {
        if !current.is_empty() {
            out.push_str(current);
            current.clear();
        }
    };

    let push_wildcard = |out: &mut String| {
        if !out.ends_with('%') {
            out.push('%');
        }
    };

    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let Some(next) = chars.next() else {
                    current.push('\\');
                    break;
                };
                match next {
                    '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
                    | '\\' => {
                        current.push(next);
                    }
                    'x' => {
                        flush_current(&mut out, &mut current);
                        push_wildcard(&mut out);
                        for _ in 0..2 {
                            if let Some(c) = chars.peek() {
                                if c.is_ascii_hexdigit() {
                                    _ = chars.next();
                                }
                            }
                        }
                    }
                    'u' => {
                        flush_current(&mut out, &mut current);
                        push_wildcard(&mut out);
                        if let Some('{') = chars.peek().copied() {
                            _ = chars.next();
                            while let Some(c) = chars.next() {
                                if c == '}' {
                                    break;
                                }
                            }
                        } else {
                            for _ in 0..4 {
                                if let Some(c) = chars.peek() {
                                    if c.is_ascii_hexdigit() {
                                        _ = chars.next();
                                    }
                                }
                            }
                        }
                    }
                    'd' | 'D' | 's' | 'S' | 'w' | 'W' | 'b' | 'B' | 'A' | 'z' | 'Z' | 'p' | 'P'
                    | 'Q' | 'E' | 'n' | 'r' | 't' | 'f' | 'v' => {
                        flush_current(&mut out, &mut current);
                        push_wildcard(&mut out);
                    }
                    c if c.is_ascii_digit() => {
                        flush_current(&mut out, &mut current);
                        push_wildcard(&mut out);
                    }
                    other => {
                        current.push(other);
                    }
                }
            }
            '.' | '^' | '$' | '(' | ')' | '|' => {
                flush_current(&mut out, &mut current);
                push_wildcard(&mut out);
            }
            '[' => {
                flush_current(&mut out, &mut current);
                push_wildcard(&mut out);
                let mut escaped = false;
                while let Some(c) = chars.next() {
                    if escaped {
                        escaped = false;
                        continue;
                    }
                    if c == '\\' {
                        escaped = true;
                        continue;
                    }
                    if c == ']' {
                        break;
                    }
                }
            }
            '*' | '?' => {
                if !current.is_empty() {
                    current.pop();
                }
                flush_current(&mut out, &mut current);
                push_wildcard(&mut out);
            }
            '+' => {
                flush_current(&mut out, &mut current);
                push_wildcard(&mut out);
            }
            '{' => {
                let mut spec = String::new();
                let mut terminated = false;
                while let Some(c) = chars.next() {
                    if c == '}' {
                        terminated = true;
                        break;
                    }
                    spec.push(c);
                }
                if quantifier_is_optional(&spec) && !current.is_empty() {
                    current.pop();
                }
                flush_current(&mut out, &mut current);
                push_wildcard(&mut out);
                if !terminated {
                    break;
                }
            }
            other => current.push(other),
        }
    }
    if !current.is_empty() {
        out.push_str(&current);
    }
    out
}

fn pattern_wildcard_info(pattern: &str) -> (bool, bool) {
    let mut leading = false;
    let mut has_single = false;
    let mut iter = pattern.chars().peekable();
    let mut at_start = true;
    while let Some(ch) = iter.next() {
        match ch {
            '\\' => {
                _ = iter.next();
                at_start = false;
            }
            '%' => {
                if at_start {
                    leading = true;
                }
                at_start = false;
            }
            '_' => {
                has_single = true;
                if at_start {
                    leading = true;
                }
                at_start = false;
            }
            _ => {
                at_start = false;
            }
        }
    }
    (leading, has_single)
}

fn extract_pattern_segments(pattern: &str) -> Vec<SegmentPattern> {
    let mut segments = Vec::new();
    for seg in extract_literal_segments(pattern) {
        let mut trigms = Vec::new();
        for (trgm, pos) in crate::trgm::Extractor::extract(&seg) {
            if let Ok(ct) = crate::trgm::CompactTrgm::try_from(trgm) {
                trigms.push(PatternTrgm {
                    trigram: ct.trgm(),
                    pos: pos as u32,
                    flags: ct.flags(),
                });
            }
        }
        if !trigms.is_empty() {
            segments.push(SegmentPattern { trigrams: trigms });
        }
    }
    segments
}

fn pattern_has_trigram(pattern: &str) -> bool {
    extract_pattern_segments(pattern)
        .into_iter()
        .any(|segment| !segment.trigrams.is_empty())
}

fn regex_has_trigram(pattern: &str) -> bool {
    let wildcard = regex_to_wildcard_pattern(pattern);
    extract_pattern_segments(&wildcard)
        .into_iter()
        .any(|segment| !segment.trigrams.is_empty())
}

fn pattern_has_lossy_trigram(segments: &[SegmentPattern]) -> bool {
    segments
        .iter()
        .any(|segment| segment.trigrams.iter().any(|pt| pt.flags & LOSSY_FLAG != 0))
}

fn flags_match(doc_flag: u8, pattern_flag: u8, case_sensitive: bool) -> bool {
    if !case_sensitive {
        return true;
    }
    (doc_flag & 0b111) == (pattern_flag & 0b111)
}

fn entry_for_trigram(
    rel: pg_sys::Relation,
    segments: &[crate::storage::Segment],
    trigram: u32,
) -> Vec<crate::storage::IndexEntry> {
    let mut entries = Vec::new();
    for seg in segments {
        if let Ok(Some(entry)) = unsafe { find_entry_for_trigram(rel, seg.block, trigram) } {
            entries.push(entry);
        }
    }
    entries
}

fn stream_segment_occurrences(
    rel: pg_sys::Relation,
    index_segments: &[crate::storage::Segment],
    seg_pattern: &SegmentPattern,
    case_sensitive: bool,
) -> anyhow::Result<Vec<(crate::storage::ItemPointer, Vec<u32>)>> {
    if seg_pattern.trigrams.is_empty() {
        return Ok(Vec::new());
    }

    struct TrgmWork {
        pat_idx: usize,
        entries: Vec<crate::storage::IndexEntry>,
        freq: u32,
    }

    struct TrigramCursor {
        cursors: Vec<PostingCursor>,
        heap: std::collections::BinaryHeap<std::cmp::Reverse<(crate::storage::ItemPointer, usize)>>,
    }

    impl TrigramCursor {
        fn current_tid(&self) -> Option<crate::storage::ItemPointer> {
            self.heap.peek().map(|entry| entry.0.0)
        }

        fn current(&self) -> Option<&crate::storage::decode::DocPosting> {
            let (_, idx) = self.heap.peek()?.0;
            self.cursors.get(idx)?.current()
        }

        fn advance(&mut self) -> anyhow::Result<bool> {
            let Some(std::cmp::Reverse((_tid, idx))) = self.heap.pop() else {
                return Ok(false);
            };
            if self.cursors[idx].advance()? {
                if let Some(next_tid) = self.cursors[idx].current_tid() {
                    self.heap.push(std::cmp::Reverse((next_tid, idx)));
                }
            }
            Ok(!self.heap.is_empty())
        }
    }

    let mut trgm_entries: Vec<TrgmWork> = Vec::new();
    for (idx, pt) in seg_pattern.trigrams.iter().enumerate() {
        let entries = entry_for_trigram(rel, index_segments, pt.trigram);
        if entries.is_empty() {
            return Ok(Vec::new());
        };
        let freq = entries
            .iter()
            .map(|e| e.frequency)
            .min()
            .unwrap_or(u32::MAX);
        trgm_entries.push(TrgmWork {
            pat_idx: idx,
            entries,
            freq,
        });
    }

    trgm_entries.sort_by_key(|w| w.freq);

    let mut cursors = Vec::with_capacity(trgm_entries.len());
    for work in &trgm_entries {
        let mut per_seg = Vec::new();
        for entry in &work.entries {
            let mut cur = unsafe { PostingCursor::new(rel, entry)? };
            // Start positioned at the first doc for this segment.
            if !cur.advance()? {
                continue;
            }
            per_seg.push(cur);
        }
        if per_seg.is_empty() {
            return Ok(Vec::new());
        }
        let mut heap: std::collections::BinaryHeap<
            std::cmp::Reverse<(crate::storage::ItemPointer, usize)>,
        > = std::collections::BinaryHeap::new();
        for (idx, cur) in per_seg.iter().enumerate() {
            if let Some(tid) = cur.current_tid() {
                heap.push(std::cmp::Reverse((tid, idx)));
            }
        }
        if heap.is_empty() {
            return Ok(Vec::new());
        }
        cursors.push(TrigramCursor {
            cursors: per_seg,
            heap,
        });
    }

    let mut occurrences: Vec<(crate::storage::ItemPointer, Vec<u32>)> = Vec::new();

    'driver: loop {
        let driver_tid = match cursors[0].current_tid() {
            Some(t) => t,
            None => break,
        };

        let mut mismatch = false;
        for idx in 1..cursors.len() {
            loop {
                let tid = match cursors[idx].current_tid() {
                    Some(t) => t,
                    None => break 'driver,
                };
                match tid.cmp(&driver_tid) {
                    Ordering::Less => {
                        if !cursors[idx].advance()? {
                            break 'driver;
                        }
                        continue;
                    }
                    Ordering::Equal => break,
                    Ordering::Greater => {
                        mismatch = true;
                        break;
                    }
                }
            }
            if mismatch {
                break;
            }
        }

        if mismatch {
            if !cursors[0].advance()? {
                break;
            }
            continue 'driver;
        }

        let anchor_pattern_idx = trgm_entries[0].pat_idx;
        let anchor_pt = &seg_pattern.trigrams[anchor_pattern_idx];
        let anchor_doc = cursors[0].current().expect("cursor populated");

        let mut starts: Vec<u32> = Vec::new();
        for (pos, flag) in &anchor_doc.positions {
            if *pos < anchor_pt.pos || !flags_match(*flag, anchor_pt.flags, case_sensitive) {
                continue;
            }
            let mut ok = true;
            for cur_idx in 1..cursors.len() {
                let pat_idx = trgm_entries[cur_idx].pat_idx;
                let pt = &seg_pattern.trigrams[pat_idx];
                let delta = pt.pos as i64 - anchor_pt.pos as i64;
                let target = if delta.is_negative() {
                    let delta = (-delta) as u32;
                    if *pos < delta {
                        ok = false;
                        break;
                    }
                    *pos - delta
                } else {
                    *pos + delta as u32
                };
                let doc = cursors[cur_idx].current().expect("aligned cursor");
                if !doc
                    .positions
                    .iter()
                    .any(|(p, f)| *p == target && flags_match(*f, pt.flags, case_sensitive))
                {
                    ok = false;
                    break;
                }
            }
            if ok {
                let start = *pos - anchor_pt.pos;
                starts.push(start);
            }
        }

        if !starts.is_empty() {
            occurrences.push((driver_tid, starts));
        }

        if !cursors[0].advance()? {
            break;
        }
    }

    Ok(occurrences)
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
    let is_regex = is_regex_strategy(keys);
    let wildcard_pattern = if is_regex {
        regex_to_wildcard_pattern(&pattern_str)
    } else {
        pattern_str.clone()
    };
    let segments = extract_pattern_segments(&wildcard_pattern);
    if segments.is_empty() {
        if is_regex {
            return build_full_regex_scan_state(index_relation);
        }
        return ScanState::default();
    }
    let (leading_wildcard, has_single_char_wildcard) = if is_regex {
        (true, true)
    } else {
        pattern_wildcard_info(&pattern_str)
    };

    if let Ok(index_segments) = unsafe { read_segments(index_relation) } {
        let mut state = ScanState::default();
        state.lossy = if is_regex {
            true
        } else {
            has_single_char_wildcard
                || pattern_has_lossy_trigram(&segments)
                || has_short_literal_segment(&pattern_str)
        };
        state.tombstones =
            crate::storage::tombstone::load_snapshot(index_relation).unwrap_or_else(|e| {
                warning!("failed to load tombstones: {e:#}");
                crate::storage::tombstone::Snapshot::default()
            });

        if is_regex {
            for seg_pattern in &segments {
                match stream_segment_occurrences(
                    index_relation,
                    &index_segments,
                    seg_pattern,
                    case_sensitive,
                ) {
                    Ok(occs) => {
                        for (tid, _) in occs {
                            state.push_match(tid);
                        }
                    }
                    Err(e) => {
                        warning!("failed to stream segment postings: {e:#}");
                        return state;
                    }
                }
            }
            state.sort_dedup();
            return state;
        }

        // Fast path: single literal segment with one trigram and anchored start (e.g. `xyz%`).
        if !leading_wildcard
            && !has_single_char_wildcard
            && segments.len() == 1
            && segments[0].trigrams.len() == 1
        {
            let pt = &segments[0].trigrams[0];
            let entries = entry_for_trigram(index_relation, &index_segments, pt.trigram);
            for entry in &entries {
                match unsafe { PostingCursor::new(index_relation, entry) } {
                    Ok(mut cur) => loop {
                        match cur.advance_check_position(pt.pos, pt.flags, case_sensitive) {
                            Ok(Some((tid, ok))) => {
                                if ok {
                                    state.push_match(tid);
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                warning!("failed to stream postings: {e:#}");
                                break;
                            }
                        }
                    },
                    Err(e) => warning!("failed to stream postings: {e:#}"),
                }
            }
            state.sort_dedup();
            return state;
        }
        let mut segment_occurrences: Vec<Vec<(crate::storage::ItemPointer, Vec<u32>)>> = Vec::new();

        for seg_pattern in &segments {
            match stream_segment_occurrences(
                index_relation,
                &index_segments,
                seg_pattern,
                case_sensitive,
            ) {
                Ok(occs) if !occs.is_empty() => segment_occurrences.push(occs),
                Ok(_) => return state,
                Err(e) => {
                    warning!("failed to stream segment postings: {e:#}");
                    return state;
                }
            }
        }

        if segment_occurrences.is_empty() {
            return state;
        }

        let first = &segment_occurrences[0];
        'tid_loop: for (tid, starts) in first {
            let start_candidates: Vec<u32> = if leading_wildcard {
                starts.clone()
            } else if starts.contains(&0) {
                vec![0]
            } else {
                continue;
            };

            for start in start_candidates {
                let mut prev_start = start;
                let mut ok = true;
                for seg_idx in 1..segment_occurrences.len() {
                    let Some(starts_next) = segment_occurrences[seg_idx]
                        .iter()
                        .find(|(t, _)| t == tid)
                        .map(|(_, s)| s)
                    else {
                        ok = false;
                        break;
                    };
                    if let Some(next_start) = starts_next
                        .iter()
                        .copied()
                        .filter(|s| *s >= prev_start)
                        .min()
                    {
                        prev_start = next_start;
                    } else {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    state.push_match(*tid);
                    continue 'tid_loop;
                }
            }
        }

        state.sort_dedup();
        state
    } else {
        ScanState::default()
    }
}

fn build_full_regex_scan_state(index_relation: pg_sys::Relation) -> ScanState {
    let mut state = ScanState::default();
    state.lossy = true;
    state.tombstones =
        crate::storage::tombstone::load_snapshot(index_relation).unwrap_or_else(|e| {
            warning!("failed to load tombstones: {e:#}");
            crate::storage::tombstone::Snapshot::default()
        });

    let Ok(index_segments) = (unsafe { read_segments(index_relation) }) else {
        return state;
    };

    for seg in &index_segments {
        let entries = match crate::storage::read_segment_entries(index_relation, seg) {
            Ok(entries) => entries,
            Err(e) => {
                warning!("failed to read segment entries: {e:#}");
                continue;
            }
        };
        for entry in &entries {
            let mut cur = match unsafe { PostingCursor::new(index_relation, entry) } {
                Ok(cur) => cur,
                Err(e) => {
                    warning!("failed to stream postings: {e:#}");
                    continue;
                }
            };
            loop {
                match cur.advance() {
                    Ok(true) => {
                        if let Some(tid) = cur.current_tid() {
                            state.push_match(tid);
                        }
                    }
                    Ok(false) => break,
                    Err(e) => {
                        warning!("failed to stream postings: {e:#}");
                        break;
                    }
                }
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
    let scan: *mut pg_sys::IndexScanDescData =
        unsafe { pg_sys::RelationGetIndexScan(index_relation, nkeys, norderbys) };
    if scan.is_null() {
        return std::ptr::null_mut();
    }

    // Keys arrive via amrescan; start with an empty state.
    let state = ScanState::default();
    unsafe {
        (*scan).opaque = Box::into_raw(Box::new(state)) as *mut std::ffi::c_void;
    }
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
    let state_ref: &mut ScanState = unsafe {
        let state_ptr = (*scan).opaque as *mut ScanState;
        if let Some(state) = state_ptr.as_mut() {
            state
        } else {
            let boxed = Box::new(ScanState::default());
            (*scan).opaque = Box::into_raw(boxed) as *mut _;
            &mut *((*scan).opaque as *mut ScanState)
        }
    };
    let new_state = unsafe { build_scan_state((*scan).indexRelation, keys, nkeys) };
    *state_ref = new_state;
}

pub unsafe extern "C-unwind" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    if scan.is_null() {
        return false;
    }
    let state_ptr = unsafe { (*scan).opaque as *mut ScanState };
    let Some(state) = (unsafe { state_ptr.as_mut() }) else {
        return false;
    };
    if let Some(tid) = state.next() {
        unsafe {
            (*scan).xs_heaptid = tid;
            (*scan).xs_recheck = state.lossy; // Recheck for regex and other lossy matches.
        }
        true
    } else {
        false
    }
}

pub unsafe extern "C-unwind" fn amgetbitmap(
    scan: pg_sys::IndexScanDesc,
    tbm: *mut pg_sys::TIDBitmap,
) -> i64 {
    if scan.is_null() || tbm.is_null() {
        return 0;
    }
    let state_ref: *mut ScanState = unsafe {
        let state_ptr = (*scan).opaque as *mut ScanState;
        if let Some(state) = state_ptr.as_mut() {
            state
        } else {
            let mut fallback =
                build_scan_state((*scan).indexRelation, (*scan).keyData, (*scan).numberOfKeys);
            fallback.sort_dedup();
            (*scan).opaque = Box::into_raw(Box::new(fallback)) as *mut std::ffi::c_void;
            (*scan).opaque as *mut ScanState
        }
    };
    let state = unsafe { &mut *state_ref };
    let mut added = 0_i64;
    for chunk in state.matches.chunks(128) {
        unsafe {
            pg_sys::tbm_add_tuples(
                tbm,
                chunk.as_ptr() as pg_sys::ItemPointer,
                chunk.len() as i32,
                state.lossy,
            );
        }
        added += chunk.len() as i64;
    }
    added
}

pub unsafe extern "C-unwind" fn amendscan(scan: pg_sys::IndexScanDesc) {
    if scan.is_null() {
        return;
    }
    let opaque = unsafe { (*scan).opaque as *mut ScanState };
    if !opaque.is_null() {
        unsafe {
            _ = Box::from_raw(opaque);
            (*scan).opaque = std::ptr::null_mut();
        }
    }
}

unsafe fn const_pattern_from_op(op: *mut pg_sys::OpExpr) -> Option<String> {
    if op.is_null() {
        return None;
    }
    let args_ptr = unsafe { (*op).args };
    if args_ptr.is_null() {
        return None;
    }
    let args = unsafe { PgList::<pg_sys::Expr>::from_pg(args_ptr) };
    if let Some(arg) = args.get_ptr(1) {
        if let Some(text) = unsafe { const_text(arg) } {
            return Some(text);
        }
    }
    if let Some(arg) = args.get_ptr(0) {
        if let Some(text) = unsafe { const_text(arg) } {
            return Some(text);
        }
    }
    None
}

unsafe fn const_text(expr: *mut pg_sys::Expr) -> Option<String> {
    if expr.is_null() {
        return None;
    }
    let mut cur = expr;
    loop {
        let node = cur as *mut pg_sys::Node;
        match unsafe { (*node).type_ } {
            pg_sys::NodeTag::T_Const => {
                let cnst = cur as *mut pg_sys::Const;
                if unsafe { (*cnst).constisnull } {
                    return None;
                }
                let ty = unsafe { (*cnst).consttype };
                let mut out_func: pg_sys::Oid = pg_sys::InvalidOid;
                let mut out_varlena = false;
                unsafe { pg_sys::getTypeOutputInfo(ty, &mut out_func, &mut out_varlena) };
                if out_func == pg_sys::InvalidOid {
                    return None;
                }
                let cstr = unsafe { pg_sys::OidOutputFunctionCall(out_func, (*cnst).constvalue) };
                if cstr.is_null() {
                    return None;
                }
                let text = unsafe { std::ffi::CStr::from_ptr(cstr) };
                return Some(text.to_string_lossy().to_string());
            }
            pg_sys::NodeTag::T_RelabelType => {
                let relabel = cur as *mut pg_sys::RelabelType;
                cur = unsafe { (*relabel).arg };
                if cur.is_null() {
                    return None;
                }
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let coerce = cur as *mut pg_sys::CoerceViaIO;
                cur = unsafe { (*coerce).arg };
                if cur.is_null() {
                    return None;
                }
            }
            pg_sys::NodeTag::T_CoerceToDomain => {
                let coerce = cur as *mut pg_sys::CoerceToDomain;
                cur = unsafe { (*coerce).arg };
                if cur.is_null() {
                    return None;
                }
            }
            pg_sys::NodeTag::T_CollateExpr => {
                let collate = cur as *mut pg_sys::CollateExpr;
                cur = unsafe { (*collate).arg };
                if cur.is_null() {
                    return None;
                }
            }
            _ => return None,
        }
    }
}

unsafe fn index_path_constant_trigram(path: *mut pg_sys::IndexPath) -> Option<bool> {
    if path.is_null() {
        return None;
    }
    let clauses_ptr = unsafe { (*path).indexclauses };
    if clauses_ptr.is_null() {
        return None;
    }
    let clauses = unsafe { PgList::<pg_sys::IndexClause>::from_pg(clauses_ptr) };
    let mut saw_constant = false;
    for clause_ptr in clauses.iter_ptr() {
        if clause_ptr.is_null() {
            continue;
        }
        let rinfo = unsafe { (*clause_ptr).rinfo };
        if rinfo.is_null() {
            continue;
        }
        let clause = unsafe { (*rinfo).clause } as *mut pg_sys::Node;
        if clause.is_null() || unsafe { (*clause).type_ } != pg_sys::NodeTag::T_OpExpr {
            continue;
        }
        let op = clause as *mut pg_sys::OpExpr;
        if let Some(pattern) = unsafe { const_pattern_from_op(op) } {
            saw_constant = true;
            let has_trigram = if unsafe { op_is_regex(op) } {
                regex_has_trigram(&pattern)
            } else {
                pattern_has_trigram(&pattern)
            };
            if has_trigram {
                return Some(true);
            }
        }
    }
    if saw_constant { Some(false) } else { None }
}

unsafe fn op_is_regex(op: *mut pg_sys::OpExpr) -> bool {
    if op.is_null() {
        return false;
    }
    let opno = unsafe { (*op).opno };
    let name = unsafe { pg_sys::get_opname(opno) };
    if name.is_null() {
        return false;
    }
    let cname = unsafe { std::ffi::CStr::from_ptr(name) };
    matches!(cname.to_bytes(), b"~" | b"~*")
}

#[allow(clippy::too_many_arguments)]
pub unsafe extern "C-unwind" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    // Cost model that favors bitmap scans over plain index scans.
    let indexinfo = unsafe { path.as_ref() }.and_then(|p| unsafe { p.indexinfo.as_ref() });

    let pages = indexinfo.map(|i| i.pages as f64).unwrap_or(1000.0).max(1.0);
    let tuples = indexinfo
        .map(|i| i.tuples as f64)
        .unwrap_or(10000.0)
        .max(1.0);

    if !index_pages.is_null() {
        unsafe {
            *index_pages = pages;
        }
    }

    let trigram_hint = unsafe { index_path_constant_trigram(path) };
    if let Some(false) = trigram_hint {
        let base_cost = 1.0e30;
        if !index_startup_cost.is_null() {
            unsafe {
                *index_startup_cost = base_cost;
            }
        }
        if !index_total_cost.is_null() {
            unsafe {
                *index_total_cost = base_cost;
            }
        }
        if !index_selectivity.is_null() {
            unsafe {
                *index_selectivity = 1.0;
            }
        }
        if !index_correlation.is_null() {
            unsafe {
                *index_correlation = 0.0;
            }
        }
        return;
    }

    // Assume moderately selective LIKE/regex predicates.
    let selectivity = (1.0 / (tuples.sqrt())).clamp(0.0001, 0.02);
    if !index_selectivity.is_null() {
        unsafe {
            *index_selectivity = selectivity;
        }
    }

    // Make plain IndexScan very expensive but keep bitmap-friendly total cost.
    // Bitmap plans use index_total_cost as the cost to build the bitmap, while
    // index scans pay startup + total. Keeping total small and startup huge
    // biases toward BitmapIndexScan.
    let startup = 100_000.0 + loop_count;
    let total = pages * selectivity * 1.0 + 5.0;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trgm::CompactTrgm;

    #[test]
    fn test_extract_literal_segments_basic() {
        let segments = extract_literal_segments("abc%def_g\\%h");
        assert_eq!(
            segments,
            vec!["abc".to_string(), "def".to_string(), "g%h".to_string()]
        );
    }

    #[test]
    fn test_extract_literal_segments_wildcards_only() {
        let segments = extract_literal_segments("%%a__b%");
        assert_eq!(segments, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_extract_pattern_segments_trigrams() {
        let segments = extract_pattern_segments("abcd%bcde");
        assert_eq!(segments.len(), 2);

        let t1: Vec<String> = segments[0]
            .trigrams
            .iter()
            .map(|pt| CompactTrgm(pt.trigram).txt())
            .collect();
        assert_eq!(t1, vec!["abc".to_string(), "bcd".to_string()]);

        let t2: Vec<String> = segments[1]
            .trigrams
            .iter()
            .map(|pt| CompactTrgm(pt.trigram).txt())
            .collect();
        assert_eq!(t2, vec!["bcd".to_string(), "cde".to_string()]);
    }

    #[test]
    fn test_extract_pattern_segments_too_short() {
        let segments = extract_pattern_segments("ab%cd");
        assert!(segments.is_empty());
    }

    #[test]
    fn test_has_short_literal_segment() {
        assert!(has_short_literal_segment("a%shadow"));
        assert!(has_short_literal_segment("ab%cd"));
        assert!(!has_short_literal_segment("abc%def"));
    }

    #[test]
    fn test_regex_to_wildcard_basic() {
        let pat = regex_to_wildcard_pattern("foo.*bar");
        assert_eq!(pat, "foo%bar");
    }

    #[test]
    fn test_regex_to_wildcard_optional_quantifier() {
        let pat = regex_to_wildcard_pattern("ab?c");
        assert_eq!(pat, "a%c");
    }

    #[test]
    fn test_regex_to_wildcard_char_class() {
        let pat = regex_to_wildcard_pattern("[a-z]+_end");
        assert_eq!(pat, "%_end");
    }
}
