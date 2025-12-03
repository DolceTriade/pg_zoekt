use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::PgBox;

const AM_NAME: &str = "pg_zoekt";

fn not_implemented<T>() -> T {
    error!("index access method boilerplate for `{AM_NAME}` is not implemented yet");
}

#[cfg(not(feature = "pg18"))]
compile_error!("pg_zoekt currently targets Postgres 18; enable the `pg18` feature.");

#[cfg(feature = "pg18")]
mod implementation {
    use super::*;

    unsafe extern "C-unwind" fn ambuild(
        _heap_relation: pg_sys::Relation,
        _index_relation: pg_sys::Relation,
        _index_info: *mut pg_sys::IndexInfo,
    ) -> *mut pg_sys::IndexBuildResult {
        not_implemented()
    }

    unsafe extern "C-unwind" fn ambuildempty(_index_relation: pg_sys::Relation) {
        not_implemented::<()>()
    }

    #[allow(clippy::too_many_arguments)]
    unsafe extern "C-unwind" fn aminsert(
        _index_relation: pg_sys::Relation,
        _values: *mut pg_sys::Datum,
        _isnull: *mut bool,
        _heap_tid: pg_sys::ItemPointer,
        _heap_relation: pg_sys::Relation,
        _check_unique: pg_sys::IndexUniqueCheck::Type,
        _index_unchanged: bool,
        _index_info: *mut pg_sys::IndexInfo,
    ) -> bool {
        not_implemented()
    }

    unsafe extern "C-unwind" fn aminsertcleanup(_index_relation: pg_sys::Relation, _index_info: *mut pg_sys::IndexInfo) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn ambulkdelete(
        _info: *mut pg_sys::IndexVacuumInfo,
        _stats: *mut pg_sys::IndexBulkDeleteResult,
        _callback: pg_sys::IndexBulkDeleteCallback,
        _callback_state: *mut std::ffi::c_void,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amvacuumcleanup(
        _info: *mut pg_sys::IndexVacuumInfo,
        _stats: *mut pg_sys::IndexBulkDeleteResult,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amcanreturn(
        _index_relation: pg_sys::Relation,
        _attno: std::os::raw::c_int,
    ) -> bool {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amcostestimate(
        _root: *mut pg_sys::PlannerInfo,
        _path: *mut pg_sys::IndexPath,
        _loop_count: f64,
        _index_startup_cost: *mut pg_sys::Cost,
        _index_total_cost: *mut pg_sys::Cost,
        _index_selectivity: *mut pg_sys::Selectivity,
        _index_correlation: *mut f64,
        _index_pages: *mut f64,
    ) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn amgettreeheight(_rel: pg_sys::Relation) -> std::os::raw::c_int {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amoptions(_reloptions: pg_sys::Datum, _validate: bool) -> *mut pg_sys::bytea {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amproperty(
        _index_oid: pg_sys::Oid,
        _attno: std::os::raw::c_int,
        _prop: pg_sys::IndexAMProperty::Type,
        _propname: *const std::os::raw::c_char,
        _res: *mut bool,
        _isnull: *mut bool,
    ) -> bool {
        not_implemented()
    }

    unsafe extern "C-unwind" fn ambuildphasename(_phasenum: pg_sys::int64) -> *mut std::os::raw::c_char {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amvalidate(_opclassoid: pg_sys::Oid) -> bool {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amadjustmembers(
        _opfamilyoid: pg_sys::Oid,
        _opclassoid: pg_sys::Oid,
        _operators: *mut pg_sys::List,
        _functions: *mut pg_sys::List,
    ) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn ambeginscan(
        _index_relation: pg_sys::Relation,
        _nkeys: std::os::raw::c_int,
        _norderbys: std::os::raw::c_int,
    ) -> pg_sys::IndexScanDesc {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amrescan(
        _scan: pg_sys::IndexScanDesc,
        _keys: pg_sys::ScanKey,
        _nkeys: std::os::raw::c_int,
        _orderbys: pg_sys::ScanKey,
        _norderbys: std::os::raw::c_int,
    ) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn amgettuple(
        _scan: pg_sys::IndexScanDesc,
        _direction: pg_sys::ScanDirection::Type,
    ) -> bool {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amgetbitmap(
        _scan: pg_sys::IndexScanDesc,
        _tbm: *mut pg_sys::TIDBitmap,
    ) -> pg_sys::int64 {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amendscan(_scan: pg_sys::IndexScanDesc) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn ammarkpos(_scan: pg_sys::IndexScanDesc) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn amrestrpos(_scan: pg_sys::IndexScanDesc) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn amestimateparallelscan(
        _index_relation: pg_sys::Relation,
        _nkeys: std::os::raw::c_int,
        _norderbys: std::os::raw::c_int,
    ) -> pg_sys::Size {
        not_implemented()
    }

    unsafe extern "C-unwind" fn aminitparallelscan(_target: *mut std::ffi::c_void) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn amparallelrescan(_scan: pg_sys::IndexScanDesc) {
        not_implemented::<()>()
    }

    unsafe extern "C-unwind" fn amtranslatestrategy(
        _strategy: pg_sys::StrategyNumber,
        _opfamily: pg_sys::Oid,
    ) -> pg_sys::CompareType::Type {
        not_implemented()
    }

    unsafe extern "C-unwind" fn amtranslatecmptype(
        _cmptype: pg_sys::CompareType::Type,
        _opfamily: pg_sys::Oid,
    ) -> pg_sys::StrategyNumber {
        not_implemented()
    }

    fn build_index_am_routine() -> PgBox<'static, pg_sys::IndexAmRoutine> {
        let mut routine = unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc0() };

        routine.type_ = pg_sys::NodeTag::T_IndexAmRoutine;
        routine.amstrategies = 0;
        routine.amsupport = 0;
        routine.amoptsprocnum = 0;
        routine.amcanorder = false;
        routine.amcanorderbyop = false;
        routine.amcanhash = false;
        routine.amconsistentequality = false;
        routine.amconsistentordering = false;
        routine.amcanbackward = false;
        routine.amcanunique = false;
        routine.amcanmulticol = true;
        routine.amoptionalkey = false;
        routine.amsearcharray = false;
        routine.amsearchnulls = false;
        routine.amstorage = false;
        routine.amclusterable = false;
        routine.ampredlocks = false;
        routine.amcanparallel = false;
        routine.amcanbuildparallel = false;
        routine.amcaninclude = false;
        routine.amusemaintenanceworkmem = false;
        routine.amsummarizing = false;
        routine.amparallelvacuumoptions = 0;
        routine.amkeytype = pg_sys::InvalidOid;

        routine.ambuild = Some(ambuild);
        routine.ambuildempty = Some(ambuildempty);
        routine.aminsert = Some(aminsert);
        routine.aminsertcleanup = Some(aminsertcleanup);
        routine.ambulkdelete = Some(ambulkdelete);
        routine.amvacuumcleanup = Some(amvacuumcleanup);
        routine.amcanreturn = Some(amcanreturn);
        routine.amcostestimate = Some(amcostestimate);
        routine.amgettreeheight = Some(amgettreeheight);
        routine.amoptions = Some(amoptions);
        routine.amproperty = Some(amproperty);
        routine.ambuildphasename = Some(ambuildphasename);
        routine.amvalidate = Some(amvalidate);
        routine.amadjustmembers = Some(amadjustmembers);
        routine.ambeginscan = Some(ambeginscan);
        routine.amrescan = Some(amrescan);
        routine.amgettuple = Some(amgettuple);
        routine.amgetbitmap = Some(amgetbitmap);
        routine.amendscan = Some(amendscan);
        routine.ammarkpos = Some(ammarkpos);
        routine.amrestrpos = Some(amrestrpos);
        routine.amestimateparallelscan = Some(amestimateparallelscan);
        routine.aminitparallelscan = Some(aminitparallelscan);
        routine.amparallelrescan = Some(amparallelrescan);
        routine.amtranslatestrategy = Some(amtranslatestrategy);
        routine.amtranslatecmptype = Some(amtranslatecmptype);

        routine
    }

    #[pg_extern]
    pub unsafe fn pg_zoekt_handler(_fcinfo: pg_sys::FunctionCallInfo) -> *mut pg_sys::IndexAmRoutine {
        build_index_am_routine().into_pg()
    }
}

#[cfg(feature = "pg18")]
pub use implementation::pg_zoekt_handler;

pgrx::extension_sql!(
    r#"
    DROP ACCESS METHOD IF EXISTS pg_zoekt;
    CREATE ACCESS METHOD pg_zoekt TYPE INDEX HANDLER pg_zoekt_handler;
    "#,
    name = "pg_zoekt_access_method",
    bootstrap
);
