use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::PgBox;

const AM_NAME: &str = "pg_zoekt";

/// Helper to make the stubs noisy at runtime.
fn not_implemented<T>() -> T {
    error!("index access method `{AM_NAME}` is not implemented yet")
}

#[cfg(not(feature = "pg18"))]
compile_error!("pg_zoekt currently targets Postgres 18; enable the `pg18` feature.");

#[cfg(feature = "pg18")]
mod implementation {
    use super::*;

    // --- Required callbacks -------------------------------------------------

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

    unsafe extern "C-unwind" fn amendscan(_scan: pg_sys::IndexScanDesc) {
        not_implemented::<()>()
    }

    // --- Optional callbacks -------------------------------------------------

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

    unsafe extern "C-unwind" fn amoptions(_reloptions: pg_sys::Datum, _validate: bool) -> *mut pg_sys::bytea {
        not_implemented()
    }

    // --- AM routine builder -------------------------------------------------

    fn build_index_am_routine() -> PgBox<pg_sys::IndexAmRoutine, pgrx::AllocatedByRust> {
        // alloc_node zeroes the struct and sets the NodeTag
        let mut routine =
            unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

        // Capabilities: keep everything minimal/false until implemented.
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

        // Required callbacks
        routine.ambuild = Some(ambuild);
        routine.ambuildempty = Some(ambuildempty);
        routine.aminsert = Some(aminsert);
        routine.ambeginscan = Some(ambeginscan);
        routine.amrescan = Some(amrescan);
        routine.amgettuple = Some(amgettuple);
        routine.amendscan = Some(amendscan);

        // Optional callbacks left unimplemented for now
        routine.amcostestimate = Some(amcostestimate);
        routine.amoptions = Some(amoptions);

        routine
    }

    #[pg_extern(sql = "
        CREATE OR REPLACE FUNCTION pg_zoekt_handler(internal) RETURNS index_am_handler
        PARALLEL SAFE IMMUTABLE STRICT COST 0.0001
        LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';

        DO $$
        DECLARE
            c int;
        BEGIN
            SELECT count(*)
            INTO c
            FROM pg_catalog.pg_am a
            WHERE a.amname = 'pg_zoekt';

            IF c = 0 THEN
                CREATE ACCESS METHOD pg_zoekt TYPE INDEX HANDLER pg_zoekt_handler;
            END IF;
        END;
        $$;
    ")]
    pub fn pg_zoekt_handler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
        build_index_am_routine().into_pg_boxed()
    }
}

#[cfg(feature = "pg18")]
pub use implementation::pg_zoekt_handler;
