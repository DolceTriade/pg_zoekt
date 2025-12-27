use pgrx::PgBox;
use pgrx::pg_sys;
use pgrx::prelude::*;

#[cfg(not(feature = "pg18"))]
compile_error!("pg_zoekt currently targets Postgres 18; enable the `pg18` feature.");

#[cfg(feature = "pg18")]
mod implementation {
    use super::*;
    use anyhow::{Result as AnyResult, anyhow};

    // --- Required callbacks -------------------------------------------------

    unsafe extern "C-unwind" fn ambuildempty(index_relation: pg_sys::Relation) {
        let _root_block = {
            let mut root_buffer = crate::storage::pgbuffer::BlockBuffer::allocate(index_relation);
            let root_block = root_buffer.block_number();
            if root_block != 0 {
                error!("expected root block 0 for empty index, got {}", root_block);
            }
            let rbl = root_buffer
                .as_struct_mut::<crate::storage::RootBlockList>(0)
                .expect("root header");
            rbl.magic = crate::storage::ROOT_MAGIC;
            rbl.version = crate::storage::VERSION;
            rbl.num_segments = 0;
            rbl.segment_list_head = pg_sys::InvalidBlockNumber;
            rbl.segment_list_tail = pg_sys::InvalidBlockNumber;
            rbl.tombstone_block = pg_sys::InvalidBlockNumber;
            rbl.tombstone_bytes = 0;
            rbl.wal_block = pg_sys::InvalidBlockNumber;
            rbl.pending_block = pg_sys::InvalidBlockNumber;
            root_block
        };

        let wal_block = {
            let mut wal_buffer = crate::storage::pgbuffer::BlockBuffer::allocate(index_relation);
            let wal_block = wal_buffer.block_number();
            let wal = wal_buffer
                .as_struct_mut::<crate::storage::WALHeader>(0)
                .expect("wal header");
            wal.magic = crate::storage::WAL_MAGIC;
            wal.bytes_used = 0;
            wal.head_block = pg_sys::InvalidBlockNumber;
            wal.tail_block = pg_sys::InvalidBlockNumber;
            wal.free_head = pg_sys::InvalidBlockNumber;
            wal_block
        };

        let pending_block_number = {
            let pending_block = crate::storage::pgbuffer::BlockBuffer::allocate(index_relation);
            pending_block.block_number()
        };
        crate::storage::pending::init_pending(index_relation, pending_block_number)
            .unwrap_or_else(|e| error!("failed to init pending list: {e:#?}"));

        let mut root_buffer = crate::storage::pgbuffer::BlockBuffer::aquire_mut(index_relation, 0);
        let rbl = root_buffer
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .expect("root header");
        rbl.wal_block = wal_block;
        rbl.pending_block = pending_block_number;
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
        let nulls = unsafe { std::slice::from_raw_parts(isnull, key_count) };
        if nulls[0] {
            return true;
        }
        let ctid: crate::storage::ItemPointer = match heap_tid.try_into() {
            Ok(tid) => tid,
            Err(err) => error!("failed to parse heap TID: {err:#?}"),
        };

        crate::storage::pending::append_tid(_index_relation, 0, ctid)
            .unwrap_or_else(|err| error!("failed to append pending tid: {err:#?}"));
        true
    }

    // --- Optional callbacks -------------------------------------------------
    #[pg_guard]
    unsafe extern "C-unwind" fn amoptions(
        reloptions: pg_sys::Datum,
        validate: bool,
    ) -> *mut pg_sys::bytea {
        unsafe {
            pg_sys::default_reloptions(reloptions, validate, pg_sys::relopt_kind::RELOPT_KIND_HEAP)
        }
    }

    unsafe extern "C-unwind" fn amvacuumcleanup(
        _info: *mut pg_sys::IndexVacuumInfo,
        stats: *mut pg_sys::IndexBulkDeleteResult,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        if !stats.is_null() {
            return stats;
        }
        let stats = unsafe { PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0() };
        stats.into_pg()
    }

    unsafe extern "C-unwind" fn ambulkdelete(
        _info: *mut pg_sys::IndexVacuumInfo,
        stats: *mut pg_sys::IndexBulkDeleteResult,
        _callback: pg_sys::IndexBulkDeleteCallback,
        _callback_state: *mut std::ffi::c_void,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        if !stats.is_null() {
            return stats;
        }
        let stats = unsafe { PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0() };
        stats.into_pg()
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

    fn flush_collector(
        rel: pg_sys::Relation,
        collector: &mut crate::trgm::Collector,
        flush_threshold: usize,
    ) {
        let trgms = collector.take_trgms();
        if trgms.is_empty() {
            return;
        }
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

                const MAX_ACTIVE_SEGMENTS: u32 = 512;
                const COMPACT_TARGET_SEGMENTS: usize = 64;
                if rbl.num_segments > MAX_ACTIVE_SEGMENTS {
                    let existing = crate::storage::segment_list_read(rel, rbl)
                        .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
                    let merged = crate::storage::merge(
                        rel,
                        &existing,
                        COMPACT_TARGET_SEGMENTS,
                        flush_threshold.saturating_mul(16).max(1024 * 1024),
                        &crate::storage::tombstone::Snapshot::default(),
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

    fn seal_serial(index: pg_sys::Oid, pending_head: u32) {
        unsafe {
            info!(
                "seal_serial: index_oid={} pending_head={}",
                index, pending_head
            );
            let index_rel = pg_sys::relation_open(index, pg_sys::RowExclusiveLock as i32);
            let heap_oid = pg_sys::IndexGetRelation(index, false);
            let heap_rel = pg_sys::relation_open(heap_oid, pg_sys::AccessShareLock as i32);
            let index_info = pg_sys::BuildIndexInfo(index_rel);
            let key_count = (*index_info).ii_NumIndexAttrs as usize;
            let estate = pg_sys::CreateExecutorState();
            let slot = pg_sys::table_slot_create(heap_rel, std::ptr::null_mut());
            let mut values = vec![pg_sys::Datum::null(); key_count];
            let mut isnull = vec![true; key_count];
            let flush_threshold = crate::build::flush_threshold_bytes();
            let mut collector = crate::trgm::Collector::new();
            let snapshot = pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot());
            info!("seal_serial: snapshot registered");

            let mut seen = 0u64;
            pg_sys::PushActiveSnapshot(snapshot);
            let res = crate::storage::pending::drain_detached(index_rel, pending_head, |tid| {
                let mut item = pg_sys::ItemPointerData {
                    ip_blkid: pg_sys::BlockIdData {
                        bi_hi: (tid.block_number >> 16) as u16,
                        bi_lo: (tid.block_number & 0xffff) as u16,
                    },
                    ip_posid: tid.offset,
                };
                let tid_ptr = &mut item as *mut _ as pg_sys::ItemPointer;
                let fetched =
                    pg_sys::table_tuple_fetch_row_version(heap_rel, tid_ptr, snapshot, slot);
                if !fetched {
                    pg_sys::ExecClearTuple(slot);
                    return;
                }

                pg_sys::FormIndexDatum(
                    index_info,
                    slot,
                    estate,
                    values.as_mut_ptr(),
                    isnull.as_mut_ptr(),
                );
                if crate::build::collect_trigrams_from_values(
                    values.as_mut_ptr(),
                    isnull.as_mut_ptr(),
                    key_count,
                    tid,
                    &mut collector,
                    |collector| {
                        if collector.memory_usage() >= flush_threshold {
                            flush_collector(index_rel, collector, flush_threshold);
                        }
                    },
                ) {
                    seen = seen.saturating_add(1);
                }
                if collector.memory_usage() >= flush_threshold {
                    flush_collector(index_rel, &mut collector, flush_threshold);
                }
                pg_sys::ExecClearTuple(slot);
            });
            pg_sys::PopActiveSnapshot();
            pg_sys::UnregisterSnapshot(snapshot);

            if let Err(e) = res {
                warning!("failed to drain pending list: {e:#?}");
            }

            flush_collector(index_rel, &mut collector, flush_threshold);

            if let Err(e) = merge_segments(index_rel, flush_threshold) {
                warning!("failed to merge segments: {e:#?}");
            }

            pg_sys::ExecDropSingleTupleTableSlot(slot);
            pg_sys::FreeExecutorState(estate);
            pg_sys::relation_close(heap_rel, pg_sys::AccessShareLock as i32);
            pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);
            info!("sealed {} pending tuples", seen);
        }
    }

    #[pg_extern]
    fn pg_zoekt_seal(index: pg_sys::Oid) {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::ShareUpdateExclusiveLock as i32);
            let pending_head = match crate::storage::pending::detach_pending(rel, 0) {
                Ok(head) => head,
                Err(e) => {
                    pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
                    error!("failed to detach pending list: {e:#?}");
                }
            };
            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);

            let Some(pending_head) = pending_head else {
                return;
            };

            let index_rel = pg_sys::relation_open(index, pg_sys::RowExclusiveLock as i32);
            let workers = reloption_parallel_workers(index_rel).unwrap_or(0).max(0) as usize;
            pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);

            if workers > 0 {
                let flush_threshold = crate::build::flush_threshold_bytes();
                if let Some(seen) = crate::seal::parallel::seal_parallel(
                    index,
                    pending_head,
                    workers,
                    flush_threshold,
                ) {
                    info!("sealed {} pending tuples (parallel)", seen);
                    return;
                }
            }

            seal_serial(index, pending_head);
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
        routine.amcanbuildparallel = true;
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
        routine.ambulkdelete = Some(ambulkdelete);
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
    pub fn test_amoptions_accepts_vacuum_params() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE amopts_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX amopts_idx ON amopts_docs USING pg_zoekt (text) WITH (autovacuum_enabled = TRUE)",
                None,
                &[],
            )?;
            client.update("DROP TABLE amopts_docs", None, &[])?;
            Ok(())
        })
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
            SET log_error_verbosity = 'verbose';
            CREATE INDEX idx_documents_text_zoekt ON documents USING pg_zoekt (text) WITH (autovacuum_enabled = TRUE);
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
        Spi::run("SELECT pg_zoekt_seal('idx_documents_text_zoekt'::regclass)")?;

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
    pub fn test_planner_avoids_index_without_trigram() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE planner_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO planner_docs (text)
                 VALUES ('a'), ('ab'), ('abc'), ('abcd'), ('abcde')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_planner_docs_text_zoekt ON planner_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;
        Spi::run("SELECT pg_zoekt_seal('idx_planner_docs_text_zoekt'::regclass)")?;
        let explain =
            Spi::get_one::<String>("EXPLAIN SELECT text FROM planner_docs WHERE text LIKE 'ab%';")?
                .unwrap_or_default();
        assert!(
            !explain.contains("zoekt"),
            "planner unexpectedly chose pg_zoekt: {explain}"
        );
        Ok(())
    }

    #[pg_test]
    pub fn test_parallel_build_reloption() -> spi::Result<()> {
        let before = Spi::get_one::<i64>("SELECT pg_zoekt_parallel_builds()")?.unwrap_or(0);
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update("DROP TABLE IF EXISTS parallel_build_docs", None, &[])?;
            client.update(
                "CREATE TABLE parallel_build_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET max_parallel_maintenance_workers = 2", None, &[])?;
            client.update(
                "INSERT INTO parallel_build_docs (text) VALUES
                 ('needle stays visible'),
                 ('bourbon biscuits'),
                 ('another needle is here')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_parallel_build_docs ON parallel_build_docs USING pg_zoekt (text) WITH (parallel_workers = 2)",
                None,
                &[],
            )?;
            Ok(())
        })?;
        Spi::run("SET enable_seqscan = OFF")?;
        let hits = Spi::get_one::<i64>(
            "SELECT count(*) FROM parallel_build_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);
        assert_eq!(hits, 2, "expected two rows containing needle");
        Spi::run("RESET enable_seqscan")?;
        let after = Spi::get_one::<i64>("SELECT pg_zoekt_parallel_builds()")?.unwrap_or(0);
        assert_eq!(after, before + 1, "parallel build counter should increment");
        Spi::run("DROP TABLE IF EXISTS parallel_build_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_wal_visibility_insert_seal_search() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE wal_visi_test (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            info!("Creating index...");
            client.update(
                "CREATE INDEX idx_wal_visi ON wal_visi_test USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update("SET enable_seqscan = OFF", None, &[])?;

            // Insert ~1000 rows of ~12KiB each to force TOAST, with one row containing the needle.
            crate::metrics::reset();
            info!("Inserting rows...");
            client.update(
                "INSERT INTO wal_visi_test (text)
                 SELECT CASE
                    WHEN gs = 424 THEN left('needle 東京 café 🦀 ' || repeat(md5(gs::text), 400), 1200000)
                    ELSE left('東京 café 🦀 ' || repeat(md5(gs::text), 400), 1200000)
                 END
                 FROM generate_series(1, 1000) gs",
                None,
                &[],
            )?;

            let total = client
                .select("SELECT count(*) FROM wal_visi_test", None, &[])?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(total, 1000, "Expected 1000 rows inserted");

            info!("Seal...");

            // VACUUM cannot run inside the test transaction; seal to mimic vacuum visibility.
            client.update("SELECT pg_zoekt_seal('idx_wal_visi'::regclass)", None, &[])?;

            let count_visible = client
                .select(
                    "SELECT count(*) FROM wal_visi_test WHERE text LIKE '%needle%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(
                count_visible, 1,
                "Needle row should be visible after vacuum"
            );

            let count_rows = client
                .select(
                    "SELECT count(*) FROM wal_visi_test WHERE text LIKE '%needle%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count_rows, 1, "Expected exactly one needle row");

            let metrics = crate::metrics::snapshot();
            info!(
                "insert metrics: inserts={} trigrams={} positions={} max_positions_per_trigram={} wal_records={}",
                metrics.inserts,
                metrics.trigrams,
                metrics.positions,
                metrics.max_positions_per_trigram,
                metrics.wal_records
            );
            Ok(())
        })
    }

    #[pg_test]
    pub fn test_unicode_matches() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE unicode_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO unicode_docs (text) VALUES
                 ('naïve café 東京 needle'),
                 ('emoji 🦀 and ASCII tag'),
                 ('plain ascii needle')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_unicode_docs_text_zoekt ON unicode_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update("SET enable_seqscan = OFF", None, &[])?;

            let tokyo = client
                .select(
                    "SELECT count(*) FROM unicode_docs WHERE text LIKE '%東京 %'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(tokyo, 1, "expected 東京 match in unicode doc");

            let cafe = client
                .select(
                    "SELECT count(*) FROM unicode_docs WHERE text LIKE '%café%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(cafe, 1, "expected café match in unicode doc");

            let needle = client
                .select(
                    "SELECT count(*) FROM unicode_docs WHERE text LIKE '%needle%'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(needle, 2, "expected ASCII match in unicode docs");

            client.update("DROP TABLE unicode_docs", None, &[])?;
            Ok(())
        })
    }

    #[pg_test]
    pub fn test_regex_matches() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE regex_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO regex_docs (text) VALUES
                 ('foo 123 bar'),
                 ('foobar'),
                 ('FOO stuff BAR'),
                 ('catfish'),
                 ('dogfish'),
                 ('nope')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_regex_docs_text_zoekt ON regex_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update("SET enable_seqscan = OFF", None, &[])?;

            let count = client
                .select(
                    "SELECT count(*) FROM regex_docs WHERE text ~ 'foo.*bar'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count, 2, "expected case-sensitive regex matches");

            let count_ci = client
                .select(
                    "SELECT count(*) FROM regex_docs WHERE text ~* 'foo.*bar'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count_ci, 3, "expected case-insensitive regex matches");

            let count_alt = client
                .select(
                    "SELECT count(*) FROM regex_docs WHERE text ~ '(cat|dog)fish'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count_alt, 2, "expected alternation regex matches");

            client.update("DROP TABLE regex_docs", None, &[])?;
            Ok(())
        })
    }
}
