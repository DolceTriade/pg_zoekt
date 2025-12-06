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

    #[derive(Debug, Default)]
    struct BuildCallbackState {
        key_count: usize,
        seen: u64,
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
                info!("pg_zoekt ambuild text: {}", text);
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
            ..Default::default()
        };

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

// Install a default operator class for text so users can say USING pg_zoekt without specifying one.
extension_sql!(
    r#"
DO $$
DECLARE
    have_family int;
    have_class int;
BEGIN
    -- Does the operator family already exist in this schema?
    SELECT count(*)
    INTO have_family
    FROM pg_catalog.pg_opfamily f
    WHERE f.opfname = 'pg_zoekt_text_ops'
      AND f.opfmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE am.amname = 'pg_zoekt')
      AND f.opfnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='@extschema@');

    IF have_family = 0 THEN
        CREATE OPERATOR FAMILY pg_zoekt_text_ops USING pg_zoekt;
    END IF;

    -- Is the default operator class already present?
    SELECT count(*)
    INTO have_class
    FROM pg_catalog.pg_opclass c
    WHERE c.opcname = 'pg_zoekt_text_ops'
      AND c.opcmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE am.amname = 'pg_zoekt')
      AND c.opcnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='@extschema@');

    IF have_class = 0 THEN
        CREATE OPERATOR CLASS pg_zoekt_text_ops DEFAULT
        FOR TYPE text USING pg_zoekt AS
            STORAGE text;
    END IF;
END;
$$;
"#,
    name = "pg_zoekt_default_text_opclass",
    requires = [pg_zoekt_handler]
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    pub fn test_build() -> spi::Result<()> {
        let sql = "
            -- 1. Create table
            CREATE TABLE documents (id SERIAL PRIMARY KEY, text TEXT NOT NULL);

            -- 2. Insert dummy data
            INSERT INTO documents (text) VALUES
            ('The quick brown fox jumps over the lazy dog.'),
            ('PostgreSQL is a powerful, open-source object-relational database system.'),
            ('pg_zoekt is a new access method for full-text search.'),
            ('Zoekt is a fast, robust code search engine.');

            -- 3. Create the index
            CREATE INDEX idx_documents_text_zoekt ON documents
            USING pg_zoekt (text);
        ";
        Spi::run(sql)?;
        assert!(false);
        Ok(())
    }

}
