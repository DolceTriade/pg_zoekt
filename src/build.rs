use pgrx::prelude::*;

use crate::storage::pgbuffer::BlockBuffer;

#[derive(Debug)]
struct BuildCallbackState {
    key_count: usize,
    seen: u64,
    collector: crate::trgm::Collector,
    flush_threshold: usize,
}

impl BuildCallbackState {
    fn flush_if_needed(&mut self, rel: pg_sys::Relation) {
        if self.collector.memory_usage() >= self.flush_threshold {
            self.flush_segments(rel);
        }
    }

    fn flush_segments(&mut self, rel: pg_sys::Relation) {
        let trgms = self.collector.take_trgms();
        if trgms.is_empty() {
            return;
        }
        // Ensure large temporary maps are dropped promptly after encoding.
        let res = crate::storage::encode::Encoder::encode_trgms(rel, &trgms);
        drop(trgms);
        match res {
            Ok(segs) => {
                let mut root = crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, 0);
                let rbl = root
                    .as_struct_mut::<crate::storage::RootBlockList>(0)
                    .expect("root header");
                if let Err(e) = crate::storage::segment_list_append(rel, rbl, &segs) {
                    error!("failed to append segments: {e:#?}");
                }

                // Avoid pathologically frequent compactions on tiny maintenance_work_mem.
                // We compact only when the on-disk list grows large, and we compact to a
                // moderate size (final merge later reduces further).
                const MAX_ACTIVE_SEGMENTS: u32 = 512;
                const COMPACT_TARGET_SEGMENTS: usize = 64;
                if rbl.num_segments > MAX_ACTIVE_SEGMENTS {
                    let existing = crate::storage::segment_list_read(rel, rbl)
                        .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
                    let merged = crate::storage::merge(
                        rel,
                        &existing,
                        COMPACT_TARGET_SEGMENTS,
                        self.flush_threshold.saturating_mul(16).max(1024 * 1024),
                    )
                    .unwrap_or_else(|e| error!("failed to compact segments: {e:#?}"));
                    crate::storage::segment_list_rewrite(rel, rbl, &merged)
                        .unwrap_or_else(|e| error!("failed to rewrite segment list: {e:#?}"));
                }
            }
            Err(e) => {
                error!("failed to flush segment: {e:#?}");
            }
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

pub(crate) fn flush_threshold_bytes() -> usize {
    let mem_bytes = maintenance_work_mem_bytes();
    // Aim lower than `maintenance_work_mem` because our estimate is conservative and
    // Rust allocations aren't accounted in Postgres' memory accounting.
    let mut flush_threshold = mem_bytes.saturating_mul(7) / 10;
    if flush_threshold == 0 {
        flush_threshold = mem_bytes;
    }
    flush_threshold
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
    unsafe {
        let state = &mut *(state as *mut BuildCallbackState);

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

        let values = std::slice::from_raw_parts(values, state.key_count);
        let isnull = std::slice::from_raw_parts(isnull, state.key_count);

        if !isnull[0] {
            // Avoid allocating a Rust `String` per tuple; we only need a temporary view.
            if let Some(text) = <&str>::from_datum(values[0], false) {
                // Large documents can blow past the flush threshold inside a single callback.
                // Check periodically while extracting to cap peak memory.
                let mut extracted = 0usize;
                for (trgm, pos) in crate::trgm::Extractor::extract(text) {
                    _ = state.collector.add(ctid, trgm, pos as u32);
                    extracted += 1;
                    if (extracted & 0xff) == 0 {
                        pg_sys::check_for_interrupts!();
                        state.flush_if_needed(index);
                    }
                }
            }
            state.seen += 1;
            pg_sys::check_for_interrupts!();
            state.flush_if_needed(index);
        }
    }
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

        let mut wal_buffer = BlockBuffer::allocate(index_relation);
        rbl.wal_block = wal_buffer.block_number();

        let wal = wal_buffer
            .as_struct_mut::<crate::storage::WALHeader>(0)
            .expect("WAL should always be in bounds");
        wal.magic = crate::storage::WAL_MAGIC;
        wal.bytes_used = 0;
        wal.head_block = pg_sys::InvalidBlockNumber;
        wal.tail_block = pg_sys::InvalidBlockNumber;
        wal.free_head = pg_sys::InvalidBlockNumber;
        root_block
    };
    let flush_threshold = flush_threshold_bytes();
    let key_count = unsafe { (*index_info).ii_NumIndexAttrs as usize };
    let mut callback_state = BuildCallbackState {
        key_count,
        seen: 0,
        collector: crate::trgm::Collector::new(),
        flush_threshold,
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

    // Final merge down to the target segment count and write back to the on-disk segment list.
    let mut root_buffer = BlockBuffer::aquire_mut(index_relation, root_block);
    let rbl = root_buffer
        .as_struct_mut::<crate::storage::RootBlockList>(0)
        .expect("root header");
    let existing = crate::storage::segment_list_read(index_relation, rbl)
        .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
    let total_size: u64 = existing.iter().map(|s| s.size).sum();
    info!("Wrote {} segments ({} bytes)", existing.len(), total_size);
    let merged = crate::storage::merge(
        index_relation,
        &existing,
        crate::storage::TARGET_SEGMENTS,
        callback_state.flush_threshold,
    )
    .unwrap_or_else(|e| error!("failed to merge segments: {e:#?}"));
    crate::storage::segment_list_rewrite(index_relation, rbl, &merged)
        .unwrap_or_else(|e| error!("failed to rewrite segment list: {e:#?}"));
    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = callback_state.seen as f64;
    result.index_tuples = callback_state.seen as f64;
    result.into_pg()
}
