use pgrx::prelude::*;
use crate::am::not_implemented;

pub unsafe extern "C-unwind" fn ambeginscan(
    _index_relation: pg_sys::Relation,
    _nkeys: std::os::raw::c_int,
    _norderbys: std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    info!("Begin scan");
    not_implemented()
}

pub unsafe extern "C-unwind" fn amrescan(
    _scan: pg_sys::IndexScanDesc,
    _keys: pg_sys::ScanKey,
    _nkeys: std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: std::os::raw::c_int,
) {
    info!("rescan");
    not_implemented::<()>()
}

pub unsafe extern "C-unwind" fn amgettuple(
    _scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    info!("gettuple");
    not_implemented()
}

pub unsafe extern "C-unwind" fn amendscan(_scan: pg_sys::IndexScanDesc) {
    info!("endscan");
    not_implemented::<()>()
}
