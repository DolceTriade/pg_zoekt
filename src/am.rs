use pgrx::PgBox;
use pgrx::pg_sys;
use pgrx::prelude::*;

const AM_NAME: &str = "pg_zoekt";

/// Helper to make the stubs noisy at runtime.
pub fn not_implemented<T>() -> T {
    error!("index access method `{AM_NAME}` is not implemented yet")
}

#[cfg(not(feature = "pg18"))]
compile_error!("pg_zoekt currently targets Postgres 18; enable the `pg18` feature.");

#[cfg(feature = "pg18")]
mod implementation {
    use super::*;
    use std::collections::BTreeMap;

    use anyhow::{Result as AnyResult, anyhow};

    // --- Required callbacks -------------------------------------------------

    unsafe extern "C-unwind" fn ambuildempty(_index_relation: pg_sys::Relation) {
        not_implemented::<()>()
    }

    #[allow(clippy::too_many_arguments)]
    unsafe extern "C-unwind" fn aminsert(
        _index_relation: pg_sys::Relation,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        heap_tid: pg_sys::ItemPointer,
        _heap_relation: pg_sys::Relation,
        _check_unique: pg_sys::IndexUniqueCheck::Type,
        _index_unchanged: bool,
        _index_info: *mut pg_sys::IndexInfo,
    ) -> bool {
        if values.is_null() || isnull.is_null() || heap_tid.is_null() {
            return false;
        }

        let key_count = unsafe {
            let rel = _index_relation.as_ref();
            rel.and_then(|rel| rel.rd_att.as_ref().map(|desc| desc.natts.max(0) as usize))
                .unwrap_or(0)
        };
        if key_count == 0 {
            return false;
        }
        let datums = unsafe { std::slice::from_raw_parts(values, key_count) };
        let nulls = unsafe { std::slice::from_raw_parts(isnull, key_count) };
        if nulls[0] {
            return true;
        }
        let Some(text) = (unsafe { <&str>::from_datum(datums[0], false) }) else {
            return false;
        };
        let ctid: crate::storage::ItemPointer = match heap_tid.try_into() {
            Ok(tid) => tid,
            Err(err) => error!("failed to parse heap TID: {err:#?}"),
        };

        let mut postings: BTreeMap<u32, Vec<crate::trgm::Occurance>> = BTreeMap::new();
        let mut interrupt = 0u32;
        for (trgm, pos) in crate::trgm::Extractor::extract(text) {
            interrupt = interrupt.wrapping_add(1);
            if (interrupt & 0x3ff) == 0 {
                pg_sys::check_for_interrupts!();
            }
            let compact = match crate::trgm::CompactTrgm::try_from(trgm) {
                Ok(ct) => ct,
                Err(err) => error!("failed to parse trigram `{trgm}`: {err:#?}"),
            };
            if pos >> 24 > 0 || pos > u32::MAX as usize {
                error!("trigram position {pos} exceeds 24-bit limit");
            }
            let mut occ = crate::trgm::Occurance(pos as u32);
            occ.set_flags(compact.flags());
            postings.entry(compact.trgm()).or_default().push(occ);
        }
        if postings.is_empty() {
            return true;
        }
        for occs in postings.values_mut() {
            occs.sort_unstable_by_key(|occ| occ.position());
        }

        let flush_threshold = crate::build::flush_threshold_bytes();
        match crate::storage::wal::append_document(
            _index_relation,
            0,
            ctid,
            &postings,
            flush_threshold,
        ) {
            Ok(sealed) => {
                if !sealed.is_empty() {
                    crate::storage::append_segments(_index_relation, 0, &sealed, flush_threshold);
                }
                true
            }
            Err(err) => error!("failed to append WAL document: {err:#?}"),
        }
    }

    // --- Optional callbacks -------------------------------------------------

    unsafe extern "C-unwind" fn amoptions(
        _reloptions: pg_sys::Datum,
        _validate: bool,
    ) -> *mut pg_sys::bytea {
        info!("NOT IMPLEMENTED");
        not_implemented()
    }

    unsafe extern "C-unwind" fn amvacuumcleanup(
        _info: *mut pg_sys::IndexVacuumInfo,
        _stats: *mut pg_sys::IndexBulkDeleteResult,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        not_implemented()
    }

    fn merge_segments(rel: pg_sys::Relation, flush_threshold: usize) -> AnyResult<()> {
        let mut root = crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, 0);
        let rbl = root
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .map_err(|e| anyhow!("{e}"))?;
        let existing = crate::storage::segment_list_read(rel, rbl)?;
        if existing.is_empty() || existing.len() <= crate::storage::TARGET_SEGMENTS {
            return Ok(());
        }
        let tombstones = crate::storage::tombstone::load_snapshot_for_root(rel, rbl)
            .unwrap_or_else(|e| {
                warning!("failed to load tombstones during merge: {e:#?}");
                crate::storage::tombstone::Snapshot::default()
            });
        let merged = crate::storage::merge(
            rel,
            &existing,
            crate::storage::TARGET_SEGMENTS,
            flush_threshold,
            &tombstones,
        )?;
        crate::storage::segment_list_rewrite(rel, rbl, &merged)?;
        Ok(())
    }

    #[pg_extern]
    fn pg_zoekt_seal(index: pg_sys::Oid) {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::ShareUpdateExclusiveLock as i32);
            let flush_threshold = crate::build::flush_threshold_bytes();

            match crate::storage::wal::flush_pending(rel, 0) {
                Ok(sealed) => {
                    if !sealed.is_empty() {
                        crate::storage::append_segments(rel, 0, &sealed, flush_threshold);
                    }
                }
                Err(e) => warning!("failed to flush WAL: {e:#?}"),
            }

            if let Err(e) = merge_segments(rel, flush_threshold) {
                warning!("failed to merge segments: {e:#?}");
            }

            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
        }
    }

    #[pg_extern]
    fn pg_zoekt_tombstone(index: pg_sys::Oid, tids: pgrx::Array<pg_sys::ItemPointerData>) -> i64 {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::ShareUpdateExclusiveLock as i32);
            let mut to_delete = Vec::new();
            for tid in tids.iter().flatten() {
                let converted =
                    <crate::storage::ItemPointer as From<pg_sys::ItemPointerData>>::from(tid);
                to_delete.push(converted);
            }
            let applied = match crate::storage::tombstone::apply_deletions(rel, to_delete) {
                Ok(count) => count as i64,
                Err(e) => {
                    pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
                    error!("failed to record tombstones: {e:#}");
                }
            };
            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
            applied
        }
    }

    fn build_index_am_routine() -> PgBox<pg_sys::IndexAmRoutine, pgrx::AllocatedByRust> {
        // alloc_node zeroes the struct and sets the NodeTag
        let mut routine = unsafe {
            PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine)
        };

        // Capabilities: keep everything minimal/false until implemented.
        routine.amstrategies = crate::operators::NUM_STRATEGIES.into();
        routine.amsupport = crate::operators::NUM_SUPPORT;
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
        routine.ambuild = Some(crate::build::ambuild);
        routine.ambuildempty = Some(ambuildempty);
        routine.aminsert = Some(aminsert);
        routine.ambeginscan = Some(crate::query::ambeginscan);
        routine.amrescan = Some(crate::query::amrescan);
        routine.amgettuple = Some(crate::query::amgettuple);
        routine.amgetbitmap = Some(crate::query::amgetbitmap);
        routine.amendscan = Some(crate::query::amendscan);

        // Optional callbacks left unimplemented for now
        routine.amcostestimate = Some(crate::query::amcostestimate);
        routine.amoptions = Some(amoptions);
        routine.amvacuumcleanup = Some(amvacuumcleanup);

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
#[allow(unused_imports)]
pub use implementation::pg_zoekt_handler;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn de_bruijn(k: &[u8], n: usize) -> String {
        // Classic de Bruijn sequence generator: returns a string where every length-n
        // substring over alphabet k appears exactly once (cyclic).
        fn db(t: usize, p: usize, n: usize, k: &[u8], a: &mut [usize], out: &mut Vec<u8>) {
            if t > n {
                if n % p == 0 {
                    for i in 1..=p {
                        out.push(k[a[i]]);
                    }
                }
            } else {
                a[t] = a[t - p];
                db(t + 1, p, n, k, a, out);
                for j in (a[t - p] + 1)..k.len() {
                    a[t] = j;
                    db(t + 1, t, n, k, a, out);
                }
            }
        }

        let mut a = vec![0usize; n * k.len() + 1];
        let mut out = Vec::new();
        db(1, 1, n, k, &mut a, &mut out);
        // Make it linear: append first n-1 chars so all windows appear as substrings.
        if n > 1 {
            let prefix: Vec<u8> = out[..n - 1].to_vec();
            out.extend_from_slice(&prefix);
        }
        String::from_utf8(out).expect("ascii alphabet")
    }

    #[pg_test]
    pub fn test_build() -> spi::Result<()> {
        let sql = "
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            -- 1. Create table
            CREATE TABLE documents (id SERIAL PRIMARY KEY, text TEXT NOT NULL);

            -- 2. Insert varied text to exercise trigram boundaries
            INSERT INTO documents (text) VALUES
            ('The quick brown fox jumps over the lazy dog. Shared trigram: xyz.'),
            ('PostgreSQL is a powerful, open-source object-relational database system.'),
            ('pg_zoekt is a new access method for full-text search.'),
            ('Zoekt is a fast, robust code search engine. xyz in code search.'),
            ('Edge-case!! punctuation,,, and    extra   spaces. xyz sprinkled.'),
            ('abc xyz'),
            ('abababababababababababababababab xyz'), -- repetitive trigram run
            ('Mixing_numbers_12345_and-hyphens-and/slashes xyz'),
            ('Longer paragraph with several sentences. It mixes case, repeats words words words, and ends abruptly. xyz near end'),
            ('xyzxyaxzyxyxyzxyzxyzxyzxyz  xyz xyzyxzxyz xyz  xyzxyxzxyzxyzxyxzxyz');

            -- 3. Create the index
            -- CREATE INDEX idx_chunks_text_zoekt ON chunks USING pg_zoekt (text_content);
            -- CREATE INDEX idx_documents_text_trgm ON documents USING GIN (text gin_trgm_ops);
            CREATE INDEX idx_documents_text_zoekt ON documents USING pg_zoekt (text);
        ";
        let explain_plan = Spi::connect_mut(|client| -> spi::Result<Vec<String>> {
            client.update(sql, None, &[])?;

            // Force the planner to consider our index and grab the text-format EXPLAIN output.
            client.update("SET enable_seqscan = OFF", None, &[])?;
            client
                .select(
                    "SELECT text FROM documents WHERE text LIKE '%xyz%';",
                    None,
                    &[],
                )?
                .into_iter()
                .map(|row| Ok(row.get::<String>(1)?.unwrap_or_default()))
                .collect()
        })?;
        Spi::run(
            "SELECT pg_zoekt_seal('idx_documents_text_zoekt'::regclass)",
        )?;

        explain_plan.iter().for_each(|s| info!("{}", s));
        // Intentional failure to force pgrx to print captured output during tests.
        //assert!(false);
        Ok(())
    }

    #[pg_test]
    pub fn test_merge_segments_compact() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update("CREATE EXTENSION IF NOT EXISTS pg_trgm", None, &[])?;
            client.update(
                "CREATE TABLE merge_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO merge_docs (text) SELECT repeat(md5(i::text), 10) FROM generate_series(1, 1024) s(i)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_merge_docs_text_zoekt ON merge_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;
        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_merge_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read index segments");
            assert!(
                segments.len() > 1,
                "expected multiple segments before merge"
            );
            info!("Read {}", segments.len());
            let merged = crate::storage::merge(
                rel,
                &segments,
                1,
                1024 * 1024 * 1024,
                &crate::storage::tombstone::Snapshot::default(),
            )
            .expect("merge failed");
            info!("merged.len() = {}", merged.len());
            assert!(!merged.is_empty());
            assert!(merged.len() == 1);

            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }
        let explain_plan = Spi::connect_mut(|client| -> spi::Result<Vec<String>> {
            client.update("SET enable_seqscan = OFF", None, &[])?;
            client
                .select(
                    // "EXPLAIN (ANALYZE, COSTS, BUFFERS, TIMING, VERBOSE) SELECT text FROM merge_docs WHERE text LIKE '%xyz%';",
                    "EXPLAIN (ANALYZE, COSTS, BUFFERS, TIMING, VERBOSE) SELECT text FROM merge_docs WHERE text ILIKE '%123%';",
                    None,
                    &[],
                )?
                .into_iter()
                .map(|row| Ok(row.get::<String>(1)?.unwrap_or_default()))
                .collect()
        })?;

        explain_plan.iter().for_each(|s| info!("{}", s));
        //assert!(false);
        Ok(())
    }

    #[pg_test]
    pub fn test_chunk_split_single_doc() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE chunk_split_large (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO chunk_split_large (text) VALUES (repeat(md5('linear'), 16384))",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_chunk_split_large ON chunk_split_large USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;
        Spi::connect_mut(|client| -> spi::Result<Vec<String>> {
            client.update("SET enable_seqscan = OFF", None, &[])?;
            client
                .select(
                    "EXPLAIN (ANALYZE) SELECT text FROM chunk_split_large WHERE text LIKE 'abcdef%';",
                    None,
                    &[],
                )?
                .into_iter()
                .map(|row| Ok(row.get::<String>(1)?.unwrap_or_default()))
                .collect()
        })?;
        Ok(())
    }

    #[pg_test]
    pub fn test_meta_pages_trigram_lookup() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update("CREATE EXTENSION IF NOT EXISTS pg_trgm", None, &[])?;
            client.update(
                "CREATE TABLE meta_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            // Ensure we keep a single collector flush so one segment contains >1 leaf page.
            client.update("SET maintenance_work_mem = '64MB'", None, &[])?;

            let seq = de_bruijn(b"abcdefghij", 3);
            client.update(
                "INSERT INTO meta_docs (text) VALUES ($1)",
                None,
                &[seq.as_str().into()],
            )?;
            client.update(
                "CREATE INDEX idx_meta_docs_text_zoekt ON meta_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update("SET enable_seqscan = OFF", None, &[])?;

            // Pick a trigram late in the string to ensure we traverse meta pages.
            let trgm = &seq[900..903];
            let pat = format!("%{}%", trgm);
            let count: i64 = client
                .select(
                    "SELECT count(*) FROM meta_docs WHERE text LIKE $1",
                    None,
                    &[pat.as_str().into()],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count, 1);
            Ok(())
        })
    }

    #[pg_test]
    pub fn test_tombstone_filters_results() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE tombstone_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO tombstone_docs (text) VALUES ('keep mee'), ('delete mee soon'), ('keep mee too')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_tombstone_docs_text_zoekt ON tombstone_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;
        Spi::run("SET enable_seqscan = OFF")?;
        let before: i64 =
            Spi::get_one("SELECT count(*) FROM tombstone_docs WHERE text LIKE '%mee%';")?
                .unwrap_or(0);
        assert_eq!(before, 3);
        Spi::run(
            "SELECT pg_zoekt_tombstone(
                'idx_tombstone_docs_text_zoekt'::regclass,
                array(SELECT ctid FROM tombstone_docs WHERE text LIKE 'delete mee%')
            );",
        )?;
        Spi::run("SET enable_seqscan = OFF")?;
        let after: i64 =
            Spi::get_one("SELECT count(*) FROM tombstone_docs WHERE text LIKE '%mee%';")?
                .unwrap_or(0);
        assert_eq!(after, 2);
        Ok(())
    }

    #[pg_test]
    pub fn test_wal_visibility_insert_seal_search() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update("CREATE EXTENSION IF NOT EXISTS pg_trgm", None, &[])?;
            client.update(
                "CREATE TABLE wal_visi_test (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;

            // Populate initial data
            client.update(
                "INSERT INTO wal_visi_test (text) VALUES ('visible_from_start')",
                None,
                &[],
            )?;

            // Index it
            client.update(
                "CREATE INDEX idx_wal_visi ON wal_visi_test USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update("SET enable_seqscan = OFF", None, &[])?;

            // 1. Verify initial data is visible (ambuild flushes)
            let count = client
                .select(
                    "SELECT count(*) FROM wal_visi_test WHERE text LIKE '%visible%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count, 1, "Initial data should be visible");

            // 2. Insert new row (Small, should go to WAL and stay there)
            client.update(
                "INSERT INTO wal_visi_test (text) VALUES ('hidden_in_wal')",
                None,
                &[],
            )?;

            // 3. Search for new row - Should be HIDDEN
            let count_hidden = client
                .select(
                    "SELECT count(*) FROM wal_visi_test WHERE text LIKE '%hidden%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(
                count_hidden, 0,
                "New row should be in WAL and invisible to search"
            );

            // 4. Seal (Simulate VACUUM)
            client.update("SELECT pg_zoekt_seal('idx_wal_visi'::regclass)", None, &[])?;

            // 5. Search for new row - Should be VISIBLE
            let count_visible = client
                .select(
                    "SELECT count(*) FROM wal_visi_test WHERE text LIKE '%hidden%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count_visible, 1, "New row should be visible after seal");

            Ok(())
        })
    }
}
