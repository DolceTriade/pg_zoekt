use std::{ffi::CString, str::FromStr};

use pgrx::prelude::*;

use crate::storage::pgbuffer::BlockBuffer;

#[derive(Debug)]
struct BuildCallbackState {
    key_count: usize,
    seen: u64,
    len: usize,
    buff: BlockBuffer,
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
unsafe extern "C-unwind" fn log_index_value_callback(
    _index: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
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
            let ctext = CString::from_str(&text).unwrap();
            // leak...
            info!("pg_zoekt ambuild text: {}", text);
            let ptr = unsafe { state.buff.as_ptr().add(state.len) };
            unsafe { std::ptr::copy(ctext.as_ptr(), ptr, ctext.count_bytes()) }
            state.len += ctext.count_bytes();
            _ = ctext.into_raw();
        }
    }

    state.seen += 1;
}

#[pg_guard]
pub extern "C-unwind" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let key_count = unsafe { (*index_info).ii_NumIndexAttrs as usize };
    let mut callback_state = BuildCallbackState {
        key_count,
        seen: 0,
        len: 0,
        buff: BlockBuffer::allocate(index_relation),
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
