use pgrx::prelude::*;

use crate::storage::pgbuffer::BlockBuffer;

#[derive(Debug)]
struct BuildCallbackState {
    key_count: usize,
    seen: u64,
    collector: crate::trgm::Collector,
    segments: Vec<crate::storage::Segment>,
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
        match crate::storage::encode::Encoder::encode_trgms(rel, &trgms) {
            Ok(mut segs) => {
                self.segments.append(&mut segs);
            }
            Err(e) => {
                error!("failed to flush segment: {e:#?}");
            }
        }
    }
}

fn maintenance_work_mem_bytes() -> usize {
    let mut kb = unsafe { pg_sys::maintenance_work_mem as usize };
    if kb == 0 {
        kb = 64 * 1024;
    }
    kb * 1024
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
            if let Some(text) = String::from_datum(values[0], false) {
                crate::trgm::Extractor::extract(&text).for_each(|(trgm, pos)| {
                    _ = state.collector.add(ctid, trgm, pos as u32);
                });
            }
            state.seen += 1;
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
    let mut root_buffer = BlockBuffer::allocate(index_relation);
    let rbl = root_buffer
        .as_struct_mut::<crate::storage::RootBlockList>(0)
        .expect("Root should always be in bounds");
    rbl.magic = crate::storage::ROOT_MAGIC;
    rbl.num_segments = 0;
    rbl.version = crate::storage::VERSION;

    {
        let mut wal_buffer = BlockBuffer::allocate(index_relation);
        rbl.wal_block = wal_buffer.block_number();

        let wal = wal_buffer
            .as_struct_mut::<crate::storage::WALBuckets>(0)
            .expect("WAL should always be in bounds");
        wal.magic = crate::storage::WAL_MAGIC;
    }
    let mem_bytes = maintenance_work_mem_bytes();
    let mut flush_threshold = mem_bytes.saturating_mul(9) / 10;
    if flush_threshold == 0 {
        flush_threshold = mem_bytes;
    }
    let key_count = unsafe { (*index_info).ii_NumIndexAttrs as usize };
    let mut callback_state = BuildCallbackState {
        key_count,
        seen: 0,
        collector: crate::trgm::Collector::new(),
        segments: Vec::new(),
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
    let segments = std::mem::take(&mut callback_state.segments);
    info!("Wrote segments: {:?}", segments);
    let merged_segments = match crate::storage::merge(
        index_relation,
        &segments,
        crate::storage::TARGET_SEGMENTS,
        callback_state.flush_threshold,
    ) {
        Ok(result) => result,
        Err(err) => {
            error!("failed to merge segments: {err:#?}");
            segments.clone()
        }
    };
    let final_segments = if merged_segments.is_empty() {
        segments.clone()
    } else {
        merged_segments
    };
    rbl.num_segments = final_segments.len() as u32;

    if !final_segments.is_empty() {
        let mut segment_list = root_buffer
            .as_struct_with_elems_mut::<crate::storage::Segments>(
                std::mem::size_of::<crate::storage::RootBlockList>(),
                final_segments.len(),
            )
            .expect("get segment list");
        for (idx, segment) in final_segments.iter().enumerate() {
            segment_list.entries[idx] = *segment;
        }
    }
    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = callback_state.seen as f64;
    result.index_tuples = callback_state.seen as f64;
    result.into_pg()
}
