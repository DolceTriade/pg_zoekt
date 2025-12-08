use std::{ffi::CString, str::FromStr};

use pgrx::prelude::*;

use crate::storage::pgbuffer::BlockBuffer;

#[derive(Debug)]
struct BuildCallbackState<'a> {
    key_count: usize,
    seen: u64,
    root: &'a mut crate::storage::RootBlockList,
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
unsafe extern "C-unwind" fn log_index_value_callback(
    _index: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut std::ffi::c_void,
) {
    let state = &mut *(state as *mut BuildCallbackState);

    if state.key_count == 0 {
        return;
    }

    let values = std::slice::from_raw_parts(values, state.key_count);
    let isnull = std::slice::from_raw_parts(isnull, state.key_count);

    if !isnull[0] {
        if let Some(text) = String::from_datum(values[0], false) {
            info!("pg_zoekt ambuild text: {} {}", text, tuple_is_alive);

        }
        state.seen += 1;
    }

}

#[pg_guard]
pub extern "C-unwind" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let mut root_buffer = BlockBuffer::allocate(index_relation);
    let mut rbl = root_buffer
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
    let key_count = unsafe { (*index_info).ii_NumIndexAttrs as usize };
    let mut callback_state = BuildCallbackState {
        key_count,
        seen: 0,
        root: &mut rbl,
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

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = callback_state.seen as f64;
    result.index_tuples = callback_state.seen as f64;
    result.into_pg()
}
