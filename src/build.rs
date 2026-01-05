use std::sync::atomic::{AtomicU64, Ordering};

use pgrx::prelude::*;

use crate::storage::pgbuffer::BlockBuffer;

mod parallel;

static PARALLEL_BUILD_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct BuildCallbackState {
    key_count: usize,
    seen: u64,
    collector: crate::trgm::Collector,
    flush_threshold: usize,
    log_counter: u64,
    log_every: u64,
    total_est_tuples: Option<u64>,
}

impl BuildCallbackState {
    fn flush_if_needed(&mut self, rel: pg_sys::Relation) {
        if self.collector.memory_usage() >= self.flush_threshold {
            self.flush_segments(rel);
        }
    }

    fn flush_segments(&mut self, rel: pg_sys::Relation) {
        flush_segments(rel, &mut self.collector, self.flush_threshold);
    }

    fn log_status(&self, reason: &str) {
        if let Some(total) = self.total_est_tuples {
            let pct = (self.seen as f64 / total.max(1) as f64) * 100.0;
            info!(
                "pg_zoekt build mem: mode=serial reason={} mem_bytes={} flush_threshold={} tuples={} progress_pct={:.1}",
                reason,
                self.collector.memory_usage(),
                self.flush_threshold,
                self.seen,
                pct
            );
        } else {
            info!(
                "pg_zoekt build mem: mode=serial reason={} mem_bytes={} flush_threshold={} tuples={}",
                reason,
                self.collector.memory_usage(),
                self.flush_threshold,
                self.seen
            );
        }
    }
}

fn flush_segments(
    rel: pg_sys::Relation,
    collector: &mut crate::trgm::Collector,
    flush_threshold: usize,
) {
    let collector_bytes = collector.memory_usage();
    let trgms = collector.take_trgms();
    if trgms.is_empty() {
        return;
    }
    info!(
        "flush_segments: collector_bytes={} flush_threshold={}",
        collector_bytes, flush_threshold
    );
    // Ensure large temporary maps are dropped promptly after encoding.
    let res = crate::storage::encode::Encoder::encode_trgms(rel, &trgms);
    drop(trgms);
    match res {
        Ok(segs) => {
            let mut root = match crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, 0) {
                Ok(root) => root,
                Err(e) => {
                    error!("failed to acquire root buffer: {e:#?}");
                }
            };
            let rbl = root
                .as_struct_mut::<crate::storage::RootBlockList>(0)
                .expect("root header");
            if let Err(e) = crate::storage::segment_list_append(rel, rbl, &segs) {
                error!("failed to append segments: {e:#?}");
            }

            const MAX_ACTIVE_SEGMENTS: u32 = 512;
            const COMPACT_TARGET_SEGMENTS: usize = 64;
            if rbl.num_segments > MAX_ACTIVE_SEGMENTS {
                if let Some(_lock) = crate::storage::maintenance_lock_try(rel) {
                    let existing = crate::storage::segment_list_read(rel, rbl)
                        .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
                    let merged = crate::storage::merge_with_workers(
                        rel,
                        &existing,
                        COMPACT_TARGET_SEGMENTS,
                        flush_threshold.saturating_mul(16).max(1024 * 1024),
                        &crate::storage::tombstone::Snapshot::default(),
                        crate::storage::reloption_parallel_workers(rel),
                    )
                    .unwrap_or_else(|e| error!("failed to compact segments: {e:#?}"));
                    crate::storage::segment_list_rewrite(rel, rbl, &merged)
                        .unwrap_or_else(|e| error!("failed to rewrite segment list: {e:#?}"));
                    if merged != existing {
                        crate::storage::free_segments(rel, &existing)
                            .unwrap_or_else(|e| error!("failed to free segments: {e:#?}"));
                        crate::storage::maybe_truncate_relation(rel, rbl, &merged)
                            .unwrap_or_else(|e| error!("failed to truncate relation: {e:#?}"));
                    }
                }
            }
        }
        Err(e) => {
            error!("failed to flush segment: {e:#?}");
        }
    }
}

pub(crate) fn maintenance_work_mem_bytes() -> usize {
    let mut kb = unsafe { pg_sys::maintenance_work_mem as usize };
    if kb == 0 {
        kb = 64 * 1024;
    }
    kb * 1024
}

pub(crate) fn flush_threshold_for_budget(budget_bytes: usize) -> usize {
    // Aim lower than budget because our estimate is conservative and
    // Rust allocations aren't accounted in Postgres' memory accounting.
    let mut flush_threshold = budget_bytes.saturating_mul(7) / 10;
    if flush_threshold == 0 {
        flush_threshold = budget_bytes;
    }
    flush_threshold
}

pub(crate) fn flush_threshold_bytes() -> usize {
    flush_threshold_for_budget(maintenance_work_mem_bytes())
}

pub(crate) fn collect_trigrams_from_values<F>(
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    key_count: usize,
    ctid: crate::storage::ItemPointer,
    collector: &mut crate::trgm::Collector,
    mut flush_check: F,
) -> bool
where
    F: FnMut(&mut crate::trgm::Collector),
{
    if values.is_null() || isnull.is_null() || key_count == 0 {
        return false;
    }
    let values = unsafe { std::slice::from_raw_parts(values, key_count) };
    let isnull = unsafe { std::slice::from_raw_parts(isnull, key_count) };
    if isnull[0] {
        return true;
    }

    // Avoid allocating a Rust `String` per tuple; we only need a temporary view.
    let Some(text) = (unsafe { <&str>::from_datum(values[0], false) }) else {
        return false;
    };
    let mut interrupt = 0u32;
    let mut extracted = 0usize;
    for (trgm, pos) in crate::trgm::Extractor::extract(text) {
        interrupt = interrupt.wrapping_add(1);
        if (interrupt & 0x3ff) == 0 {
            pg_sys::check_for_interrupts!();
        }
        if pos >> 24 > 0 || pos > u32::MAX as usize {
            error!("trigram position {pos} exceeds 24-bit limit");
        }
        collector
            .add(ctid, trgm, pos as u32)
            .unwrap_or_else(|err| error!("failed to add trigram `{trgm}`: {err:#?}"));
        extracted += 1;
        if (extracted & 0xff) == 0 {
            flush_check(collector);
        }
    }
    true
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
unsafe extern "C-unwind" fn log_index_value_callback(
    index: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut std::ffi::c_void,
) {
    let state = unsafe { &mut *(state as *mut BuildCallbackState) };

    if state.key_count == 0 {
        return;
    }

    if tid.is_null() {
        return;
    }

    if !tuple_is_alive {
        return;
    }

    let ctid: crate::storage::ItemPointer = match tid.try_into() {
        Ok(ctid) => ctid,
        Err(e) => {
            error!("failed to parse tid: {e:#?}");
        }
    };

    if collect_trigrams_from_values(
        values,
        isnull,
        state.key_count,
        ctid,
        &mut state.collector,
        |collector| {
            if collector.memory_usage() >= state.flush_threshold {
                flush_segments(index, collector, state.flush_threshold);
            }
        },
    ) {
        state.seen += 1;
        state.log_counter = state.log_counter.wrapping_add(1);
        if state.log_counter % state.log_every == 0 {
            state.log_status("periodic");
        }
        pg_sys::check_for_interrupts!();
        state.flush_if_needed(index);
    }
}

fn run_serial_build(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    flush_threshold: usize,
) -> f64 {
    let total_est_tuples = unsafe {
        let mut pages: pg_sys::BlockNumber = 0;
        let mut tuples: f64 = 0.0;
        let mut allvisfrac: f64 = 0.0;
        pg_sys::table_relation_estimate_size(
            heap_relation,
            std::ptr::null_mut(),
            &mut pages,
            &mut tuples,
            &mut allvisfrac,
        );
        if tuples > 0.0 {
            Some(tuples.max(0.0) as u64)
        } else {
            None
        }
    };

    let key_count = unsafe { (*index_info).ii_NumIndexAttrs as usize };
    let mut callback_state = BuildCallbackState {
        key_count,
        seen: 0,
        collector: crate::trgm::Collector::new(),
        flush_threshold,
        log_counter: 0,
        log_every: 32768,
        total_est_tuples,
    };
    info!("Starting scan");
    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation,
            index_relation,
            index_info,
            Some(log_index_value_callback),
            &mut callback_state,
        );
    }
    callback_state.flush_segments(index_relation);
    callback_state.seen as f64
}

fn finalize_segment_list(
    index_relation: pg_sys::Relation,
    root_block: u32,
    flush_threshold: usize,
) {
    let mut root_buffer = match BlockBuffer::aquire_mut(index_relation, root_block) {
        Ok(root_buffer) => root_buffer,
        Err(e) => {
            error!("failed to acquire root buffer: {e:#?}");
        }
    };
    let rbl = root_buffer
        .as_struct_mut::<crate::storage::RootBlockList>(0)
        .expect("root header");
    let existing = crate::storage::segment_list_read(index_relation, rbl)
        .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
    let total_size: u64 = existing.iter().map(|s| s.size).sum();
    info!(
        "Wrote {} segments ({} bytes), flush_threshold={}, target_segments={}",
        existing.len(),
        total_size,
        flush_threshold,
        crate::storage::TARGET_SEGMENTS
    );
    let tombstones = crate::storage::tombstone::Snapshot::default();
    if let Some(_lock) = crate::storage::maintenance_lock_try(index_relation) {
        let merged = crate::storage::merge_with_workers(
            index_relation,
            &existing,
            crate::storage::TARGET_SEGMENTS,
            flush_threshold,
            &tombstones,
            crate::storage::reloption_parallel_workers(index_relation),
        )
        .unwrap_or_else(|e| error!("failed to merge segments: {e:#?}"));
        crate::storage::segment_list_rewrite(index_relation, rbl, &merged)
            .unwrap_or_else(|e| error!("failed to rewrite segment list: {e:#?}"));
        if merged != existing {
            crate::storage::free_segments(index_relation, &existing)
                .unwrap_or_else(|e| error!("failed to free segments: {e:#?}"));
            crate::storage::maybe_truncate_relation(index_relation, rbl, &merged)
                .unwrap_or_else(|e| error!("failed to truncate relation: {e:#?}"));
        }
    }
}

fn reloption_parallel_workers(index_relation: pg_sys::Relation) -> Option<i32> {
    if index_relation.is_null() {
        return None;
    }
    let opts = unsafe { (*index_relation).rd_options as *const pg_sys::StdRdOptions };
    if opts.is_null() {
        return None;
    }
    let workers = unsafe { (*opts).parallel_workers };
    if workers >= 0 { Some(workers) } else { None }
}

fn try_parallel_build(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    root_block: u32,
    maintenance_budget_bytes: usize,
) -> Option<f64> {
    unsafe {
        if (*index_info).ii_Concurrent {
            info!("Parallel build disabled for concurrent index build");
            return None;
        }
    }

    let Some(relopt) = reloption_parallel_workers(index_relation) else {
        info!("Parallel build disabled: no parallel_workers reloption");
        return None;
    };
    let effective = relopt;
    if effective <= 0 {
        info!("Parallel build disabled: parallel_workers <= 0");
        return None;
    }

    let original_workers = unsafe { (*index_info).ii_ParallelWorkers };
    let clamped = effective.clamp(0, i32::MAX);
    unsafe {
        (*index_info).ii_ParallelWorkers = clamped;
    }

    let workers = clamped.max(1) as usize;
    let per_worker_budget = maintenance_budget_bytes.saturating_div(workers);
    let flush_threshold = flush_threshold_for_budget(per_worker_budget);
    let result = unsafe {
        parallel::build_parallel(
            heap_relation,
            index_relation,
            index_info,
            root_block,
            flush_threshold,
            per_worker_budget,
            maintenance_budget_bytes,
        )
    };

    unsafe {
        (*index_info).ii_ParallelWorkers = original_workers;
    }

    let Some(count) = result else {
        info!("Parallel build not used; falling back to serial build");
        return None;
    };

    Some({
        PARALLEL_BUILD_COUNT.fetch_add(1, Ordering::Relaxed);
        count as f64
    })
}

#[pg_guard]
pub extern "C-unwind" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    // IMPORTANT: do not hold an exclusive lock on the root buffer throughout the
    // heap scan; callbacks need to acquire it to append segment records.
    let root_block = {
        info!("Allocating root");
        let mut root_buffer = BlockBuffer::allocate(index_relation);
        let root_block = root_buffer.block_number();
        let rbl = root_buffer
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .expect("Root should always be in bounds");
        rbl.magic = crate::storage::ROOT_MAGIC;
        rbl.num_segments = 0;
        rbl.version = crate::storage::VERSION;
        rbl.segment_list_head = pg_sys::InvalidBlockNumber;
        rbl.segment_list_tail = pg_sys::InvalidBlockNumber;
        rbl.tombstone_block = pg_sys::InvalidBlockNumber;
        rbl.tombstone_bytes = 0;
        rbl.wal_block = pg_sys::InvalidBlockNumber;
        rbl.pending_block = pg_sys::InvalidBlockNumber;
        root_block
    };

    let wal_block = {
        let mut wal_buffer = BlockBuffer::allocate(index_relation);
        let wal_block = wal_buffer.block_number();
        let wal = wal_buffer
            .as_struct_mut::<crate::storage::WALHeader>(0)
            .expect("WAL should always be in bounds");
        wal.magic = crate::storage::WAL_MAGIC;
        wal.bytes_used = 0;
        wal.head_block = pg_sys::InvalidBlockNumber;
        wal.tail_block = pg_sys::InvalidBlockNumber;
        wal.free_head = pg_sys::InvalidBlockNumber;
        wal_block
    };

    info!("Allocating pending");
    let pending_block_number = {
        let pending_block = BlockBuffer::allocate(index_relation);
        pending_block.block_number()
    };
    crate::storage::pending::init_pending(index_relation, pending_block_number)
        .unwrap_or_else(|e| error!("failed to init pending list: {e:#?}"));

    {
        let mut root_buffer = match BlockBuffer::aquire_mut(index_relation, root_block) {
            Ok(root_buffer) => root_buffer,
            Err(e) => {
                error!("failed to acquire root buffer: {e:#?}");
            }
        };
        let rbl = root_buffer
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .expect("root header");
        rbl.wal_block = wal_block;
        rbl.pending_block = pending_block_number;
    }
    let flush_threshold = flush_threshold_bytes();
    let maintenance_budget_bytes = maintenance_work_mem_bytes();
    let seen = try_parallel_build(
        heap_relation,
        index_relation,
        index_info,
        root_block,
        maintenance_budget_bytes,
    )
    .unwrap_or_else(|| {
        let serial_threshold = flush_threshold_for_budget(maintenance_budget_bytes);
        run_serial_build(heap_relation, index_relation, index_info, serial_threshold)
    });

    finalize_segment_list(index_relation, root_block, flush_threshold);

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = seen;
    result.index_tuples = seen;
    result.into_pg()
}

#[pg_extern]
fn pg_zoekt_parallel_builds() -> i64 {
    PARALLEL_BUILD_COUNT.load(Ordering::Relaxed) as i64
}
