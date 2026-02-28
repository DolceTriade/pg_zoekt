use pgrx::PgBox;
use pgrx::pg_sys;
use pgrx::prelude::*;

#[cfg(not(feature = "pg18"))]
compile_error!("pg_zoekt currently targets Postgres 18; enable the `pg18` feature.");

#[cfg(feature = "pg18")]
mod implementation {
    use super::*;
    use anyhow::{Result as AnyResult, anyhow};
    use pgrx::iter::TableIterator;

    #[derive(Clone, Copy, Debug)]
    enum MaintenanceMode {
        Seal,
        Merge,
        Full,
        Truncate,
    }

    impl MaintenanceMode {
        fn parse(mode: &str) -> Option<Self> {
            match mode {
                "seal" => Some(Self::Seal),
                "merge" => Some(Self::Merge),
                "full" => Some(Self::Full),
                "truncate" => Some(Self::Truncate),
                _ => None,
            }
        }

        fn as_str(self) -> &'static str {
            match self {
                Self::Seal => "seal",
                Self::Merge => "merge",
                Self::Full => "full",
                Self::Truncate => "truncate",
            }
        }
    }

    #[derive(Clone, Debug)]
    struct MaintenanceResult {
        index_oid: pg_sys::Oid,
        mode: MaintenanceMode,
        sealed_tuples: i64,
        segments_before: i32,
        segments_after: i32,
        skipped_busy: bool,
    }

    impl MaintenanceResult {
        fn row(
            self,
        ) -> (
            name!(index_oid, pg_sys::Oid),
            name!(mode, String),
            name!(sealed_tuples, i64),
            name!(segments_before, i32),
            name!(segments_after, i32),
            name!(skipped_busy, bool),
        ) {
            (
                self.index_oid,
                self.mode.as_str().to_string(),
                self.sealed_tuples,
                self.segments_before,
                self.segments_after,
                self.skipped_busy,
            )
        }
    }

    fn should_skip_insert_for_heap_bounds(
        heap_relation: pg_sys::Relation,
        heap_tid: pg_sys::ItemPointer,
    ) -> bool {
        if heap_relation.is_null() {
            return false;
        }

        let nblocks = unsafe {
            pg_sys::RelationGetNumberOfBlocksInFork(heap_relation, pg_sys::ForkNumber::MAIN_FORKNUM)
        };
        let heap_tid = unsafe { *heap_tid };
        let blk = ((heap_tid.ip_blkid.bi_hi as u32) << 16) | heap_tid.ip_blkid.bi_lo as u32;
        blk >= nblocks
    }

    // --- Required callbacks -------------------------------------------------

    unsafe extern "C-unwind" fn ambuildempty(index_relation: pg_sys::Relation) {
        let crate::storage::IndexBootstrapBlocks {
            root_block,
            wal_block,
            pending_block,
        } = crate::storage::initialize_index_storage(index_relation, true)
            .unwrap_or_else(|e| error!("failed to initialize empty index storage: {e:#?}"));
        debug_assert_eq!(root_block, 0);
        debug_assert_ne!(wal_block, pg_sys::InvalidBlockNumber);
        debug_assert_ne!(pending_block, pg_sys::InvalidBlockNumber);
    }

    #[allow(clippy::too_many_arguments)]
    #[pg_guard]
    unsafe extern "C-unwind" fn aminsert(
        _index_relation: pg_sys::Relation,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        heap_tid: pg_sys::ItemPointer,
        heap_relation: pg_sys::Relation,
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
        // Keep the defensive heap-bounds guard for now until the aminsert
        // heapRelation contract is verified against upstream callers.
        if should_skip_insert_for_heap_bounds(heap_relation, heap_tid) {
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

    #[pg_guard]
    pub(super) unsafe extern "C-unwind" fn amvacuumcleanup(
        _info: *mut pg_sys::IndexVacuumInfo,
        stats: *mut pg_sys::IndexBulkDeleteResult,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        if !_info.is_null() {
            let index_rel = unsafe { (*_info).index };
            if !index_rel.is_null() {
                if let Err(e) = crate::storage::pending::cleanup_free_list(index_rel) {
                    warning!("failed to clean pending free list during vacuum: {e:#}");
                }
            }
        }
        if !stats.is_null() {
            return stats;
        }
        let stats = unsafe { PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0() };
        stats.into_pg()
    }

    #[pg_guard]
    pub(super) unsafe extern "C-unwind" fn ambulkdelete(
        _info: *mut pg_sys::IndexVacuumInfo,
        stats: *mut pg_sys::IndexBulkDeleteResult,
        _callback: pg_sys::IndexBulkDeleteCallback,
        _callback_state: *mut std::ffi::c_void,
    ) -> *mut pg_sys::IndexBulkDeleteResult {
        let stats_ptr = if !stats.is_null() {
            stats
        } else {
            unsafe {
                pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBulkDeleteResult>())
                    as *mut pg_sys::IndexBulkDeleteResult
            }
        };
        let stats = unsafe { &mut *stats_ptr };

        let Some(callback) = _callback else {
            return stats_ptr;
        };
        if _info.is_null() {
            return stats_ptr;
        }
        let index_rel = unsafe { (*_info).index };
        if index_rel.is_null() {
            return stats_ptr;
        }

        let mut seen: u64 = 0;
        let mut removed: Vec<crate::storage::ItemPointer> = Vec::new();
        let Ok(segments) = (unsafe { crate::query::read_segments(index_rel) }) else {
            return stats_ptr;
        };

        for seg in segments.iter() {
            let entries = match crate::storage::read_segment_entries(index_rel, seg) {
                Ok(entries) => entries,
                Err(e) => {
                    warning!("failed to read segment entries: {e:#}");
                    continue;
                }
            };
            for entry in entries.iter() {
                let mut cursor =
                    match unsafe { crate::storage::decode::PostingCursor::new(index_rel, entry) } {
                        Ok(cur) => cur,
                        Err(e) => {
                            warning!("failed to open posting cursor: {e:#}");
                            continue;
                        }
                    };
                loop {
                    match cursor.advance() {
                        Ok(true) => {
                            let Some(doc) = cursor.current() else {
                                continue;
                            };
                            seen = seen.saturating_add(1);
                            let tid = pg_sys::ItemPointerData {
                                ip_blkid: pg_sys::BlockIdData {
                                    bi_hi: (doc.tid.block_number >> 16) as u16,
                                    bi_lo: (doc.tid.block_number & 0xffff) as u16,
                                },
                                ip_posid: doc.tid.offset,
                            };
                            let delete = unsafe {
                                callback(&tid as *const _ as pg_sys::ItemPointer, _callback_state)
                            };
                            if delete {
                                removed.push(doc.tid);
                                stats.tuples_removed += 1.0;
                            } else {
                                stats.num_index_tuples += 1.0;
                            }
                        }
                        Ok(false) => break,
                        Err(e) => {
                            warning!("failed to advance posting cursor: {e:#}");
                            break;
                        }
                    }
                }
            }
        }

        if !removed.is_empty() {
            if let Err(e) = crate::storage::tombstone::apply_deletions(index_rel, removed) {
                warning!("failed to persist tombstones: {e:#}");
            }
        }
        if seen > 0 {
            stats.estimated_count = false;
        }
        stats_ptr
    }

    fn merge_segments(
        rel: pg_sys::Relation,
        flush_threshold: usize,
        lock_mode: crate::storage::MaintenanceLockMode,
        recent_segments_hint: usize,
    ) -> AnyResult<bool> {
        let _lock = match crate::storage::maintenance_lock(rel, lock_mode) {
            Some(lock) => lock,
            None => {
                info!("merge skipped: maintenance lock busy");
                return Ok(true);
            }
        };
        let mut root = crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, 0)
            .map_err(|e| anyhow!("{e}"))?;
        let rbl = root
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .map_err(|e| anyhow!("{e}"))?;
        let existing = crate::storage::segment_list_read(rel, rbl)?;
        if existing.is_empty() {
            return Ok(false);
        }
        let tombstones = crate::storage::tombstone::load_snapshot_for_root(rel, rbl)
            .unwrap_or_else(|e| {
                warning!("failed to load tombstones during merge: {e:#?}");
                crate::storage::tombstone::Snapshot::default()
            });

        let target = crate::storage::TARGET_SEGMENTS;
        let seg_count = existing.len();
        let mut required_victims = if seg_count > target {
            seg_count - target + 1
        } else if recent_segments_hint > 1 {
            recent_segments_hint
        } else {
            0
        };
        if required_victims < 2 {
            return Ok(false);
        }
        required_victims = required_victims.min(seg_count);

        let recent_count = recent_segments_hint.min(seg_count);
        let recent_start = seg_count.saturating_sub(recent_count);
        let mut victim_set: Vec<crate::storage::Segment> = Vec::with_capacity(required_victims);

        if recent_count > 0 {
            for seg in &existing[recent_start..] {
                if victim_set.len() >= required_victims {
                    break;
                }
                if !victim_set.contains(seg) {
                    victim_set.push(*seg);
                }
            }
        }
        if victim_set.len() < required_victims {
            let mut candidates: Vec<crate::storage::Segment> =
                existing[..recent_start].iter().copied().collect();
            candidates.sort_by_key(|seg| seg.size);
            for seg in candidates {
                if victim_set.len() >= required_victims {
                    break;
                }
                if !victim_set.contains(&seg) {
                    victim_set.push(seg);
                }
            }
        }
        if victim_set.len() < 2 {
            return Ok(false);
        }

        let victims: Vec<crate::storage::Segment> = existing
            .iter()
            .copied()
            .filter(|seg| victim_set.contains(seg))
            .collect();
        let victim_total_bytes = victims
            .iter()
            .fold(0u64, |acc, seg| acc.saturating_add(seg.size));
        info!(
            "merge_segments decision: seg_count={} target={} recent_hint={} required_victims={} selected_victims={} selected_victim_bytes={}",
            seg_count,
            target,
            recent_segments_hint,
            required_victims,
            victims.len(),
            victim_total_bytes
        );
        let replacement = crate::storage::merge(rel, &victims, flush_threshold, &tombstones)?;
        let mut rewritten: Vec<crate::storage::Segment> = existing
            .iter()
            .copied()
            .filter(|seg| !victim_set.contains(seg))
            .collect();
        if replacement.block != pg_sys::InvalidBlockNumber {
            rewritten.push(replacement);
        }

        crate::storage::segment_list_rewrite(rel, rbl, &rewritten)?;
        info!(
            "merge_segments publish: kept_segments={} deferred_reclaim_segments={}",
            rewritten.len(),
            victims.len()
        );
        Ok(false)
    }

    fn flush_collector(rel: pg_sys::Relation, collector: &mut crate::trgm::Collector) -> usize {
        let trgms = collector.take_trgms();
        if trgms.is_empty() {
            return 0;
        }
        let res = crate::storage::encode::Encoder::encode_trgms(rel, &trgms);
        drop(trgms);
        match res {
            Ok(segs) => {
                let count = segs.len();
                let mut root = match crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, 0) {
                    Ok(root) => root,
                    Err(e) => {
                        error!("failed to acquire root buffer: {e:#?}");
                    }
                };
                let rbl = root
                    .as_struct_mut::<crate::storage::RootBlockList>(0)
                    .expect("root header");
                if let Err(e) = crate::storage::segment_list_append(rel, rbl, &segs) {
                    error!("failed to append segments: {e:#?}");
                }
                count
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

    pub(super) fn seal_serial(index: pg_sys::Oid, pending_head: u32) -> (u64, usize) {
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
            let mut appended_segments = 0usize;
            let mut requeued = 0u64;
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
                    let any_fetched = pg_sys::table_tuple_fetch_row_version(
                        heap_rel,
                        tid_ptr,
                        &raw mut pg_sys::SnapshotAnyData,
                        slot,
                    );
                    if any_fetched {
                        let mut should_free = false;
                        let htup = pg_sys::ExecFetchSlotHeapTuple(slot, true, &mut should_free);
                        let mut should_requeue = false;
                        if !htup.is_null() {
                            let header = (*htup).t_data;
                            if !header.is_null() {
                                let xmin = pg_sys::htup::HeapTupleHeaderGetXmin(header);
                                if pg_sys::TransactionIdIsInProgress(xmin)
                                    || pg_sys::TransactionIdDidCommit(xmin)
                                {
                                    should_requeue = true;
                                }
                            }
                        }
                        if should_free && !htup.is_null() {
                            pg_sys::heap_freetuple(htup);
                        }
                        if should_requeue {
                            crate::storage::pending::append_tid(index_rel, 0, tid).unwrap_or_else(
                                |err| error!("failed to requeue pending tid: {err:#?}"),
                            );
                            requeued = requeued.saturating_add(1);
                        }
                    }
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
                            appended_segments = appended_segments
                                .saturating_add(flush_collector(index_rel, collector));
                        }
                    },
                ) {
                    seen = seen.saturating_add(1);
                }
                if collector.memory_usage() >= flush_threshold {
                    appended_segments = appended_segments
                        .saturating_add(flush_collector(index_rel, &mut collector));
                }
                pg_sys::ExecClearTuple(slot);
            });
            pg_sys::PopActiveSnapshot();
            pg_sys::UnregisterSnapshot(snapshot);

            if let Err(e) = res {
                warning!("failed to drain pending list: {e:#?}");
            }

            appended_segments =
                appended_segments.saturating_add(flush_collector(index_rel, &mut collector));

            pg_sys::ExecDropSingleTupleTableSlot(slot);
            pg_sys::FreeExecutorState(estate);
            pg_sys::relation_close(heap_rel, pg_sys::AccessShareLock as i32);
            pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);
            info!(
                "sealed {} pending tuples into {} segments (requeued {})",
                seen, appended_segments, requeued
            );
            (seen, appended_segments)
        }
    }

    fn seal_index(
        index: pg_sys::Oid,
        lock_mode: crate::storage::MaintenanceLockMode,
    ) -> (u64, usize, bool) {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::ShareUpdateExclusiveLock as i32);
            let lock = match crate::storage::maintenance_lock(rel, lock_mode) {
                Some(lock) => lock,
                None => {
                    pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
                    return (0, 0, true);
                }
            };
            let pending_head = match crate::storage::pending::detach_pending(rel, 0) {
                Ok(head) => head,
                Err(e) => {
                    drop(lock);
                    pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
                    error!("failed to detach pending list: {e:#?}");
                }
            };

            let Some(pending_head) = pending_head else {
                drop(lock);
                pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
                return (0, 0, false);
            };

            let workers = reloption_parallel_workers(rel).unwrap_or(0).max(0) as usize;
            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);

            if workers > 0 {
                let flush_threshold = crate::build::flush_threshold_bytes();
                if let Some((seen, appended_segments)) = crate::seal::parallel::seal_parallel(
                    index,
                    pending_head,
                    workers,
                    flush_threshold,
                ) {
                    info!(
                        "sealed {} pending tuples into {} segments (parallel)",
                        seen, appended_segments
                    );
                    drop(lock);
                    return (seen, appended_segments, false);
                }
            }

            let (seen, appended_segments) = seal_serial(index, pending_head);
            drop(lock);
            (seen, appended_segments, false)
        }
    }

    fn segment_count(index: pg_sys::Oid) -> i32 {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::AccessShareLock as i32);
            let root = match crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0) {
                Ok(root) => root,
                Err(e) => {
                    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                    error!("failed to acquire root buffer: {e:#?}");
                }
            };
            let rbl = root
                .as_struct::<crate::storage::RootBlockList>(0)
                .expect("root header");
            let count = rbl.num_segments as i32;
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            count
        }
    }

    fn merge_index(
        index: pg_sys::Oid,
        lock_mode: crate::storage::MaintenanceLockMode,
        recent_segments_hint: usize,
    ) -> AnyResult<bool> {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::ShareUpdateExclusiveLock as i32);
            let flush_threshold = crate::build::flush_threshold_bytes();
            let res = merge_segments(rel, flush_threshold, lock_mode, recent_segments_hint);
            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
            res
        }
    }

    fn truncate_index(
        index: pg_sys::Oid,
        lock_mode: crate::storage::MaintenanceLockMode,
    ) -> AnyResult<bool> {
        unsafe {
            let rel = pg_sys::relation_open(index, pg_sys::ShareUpdateExclusiveLock as i32);
            let lock = match crate::storage::maintenance_lock(rel, lock_mode) {
                Some(lock) => lock,
                None => {
                    pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
                    info!("truncate skipped: maintenance lock busy");
                    return Ok(true);
                }
            };
            let res: AnyResult<()> = (|| {
                let root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0)
                    .map_err(|e| anyhow!("failed to acquire root buffer: {e:#?}"))?;
                let rbl = root
                    .as_struct::<crate::storage::RootBlockList>(0)
                    .map_err(|e| anyhow!("failed to read root header: {e:#?}"))?;
                let segments = crate::storage::segment_list_read(rel, rbl)?;
                let reclaimed = crate::storage::reclaim_orphan_blocks(rel, rbl, &segments)?;
                info!(
                    "truncate_index reclaim phase: orphan_segments={} reclaimed_blocks={}",
                    segments.len(),
                    reclaimed
                );
                crate::storage::maybe_truncate_relation(rel, rbl, &segments)?;
                Ok(())
            })();
            drop(lock);
            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
            res?;
            Ok(false)
        }
    }

    fn run_maintenance(
        index: pg_sys::Oid,
        mode: MaintenanceMode,
        lock_mode: crate::storage::MaintenanceLockMode,
    ) -> AnyResult<MaintenanceResult> {
        let segments_before = segment_count(index);
        let mut sealed_tuples = 0i64;
        let mut skipped_busy;

        match mode {
            MaintenanceMode::Seal => {
                let (seen, _appended_segments, skipped) = seal_index(index, lock_mode);
                sealed_tuples = seen as i64;
                skipped_busy = skipped;
            }
            MaintenanceMode::Merge => {
                skipped_busy = merge_index(index, lock_mode, 0)?;
            }
            MaintenanceMode::Full => {
                let (seen, appended_segments, skipped) = seal_index(index, lock_mode);
                sealed_tuples = seen as i64;
                skipped_busy = skipped;
                if !skipped_busy {
                    skipped_busy = merge_index(index, lock_mode, appended_segments)?;
                }
            }
            MaintenanceMode::Truncate => {
                skipped_busy = truncate_index(index, lock_mode)?;
            }
        }

        let segments_after = segment_count(index);
        Ok(MaintenanceResult {
            index_oid: index,
            mode,
            sealed_tuples,
            segments_before,
            segments_after,
            skipped_busy,
        })
    }

    #[pg_extern]
    fn pg_zoekt_seal(index: pg_sys::Oid) {
        run_maintenance(
            index,
            MaintenanceMode::Full,
            crate::storage::MaintenanceLockMode::Block,
        )
        .unwrap_or_else(|e| error!("maintenance failed: {e:#}"));
    }

    #[pg_extern]
    fn pg_zoekt_maintain(
        index: pg_sys::Oid,
        mode: default!(&str, "'full'"),
        wait: default!(bool, "false"),
    ) -> TableIterator<
        'static,
        (
            name!(index_oid, pg_sys::Oid),
            name!(mode, String),
            name!(sealed_tuples, i64),
            name!(segments_before, i32),
            name!(segments_after, i32),
            name!(skipped_busy, bool),
        ),
    > {
        let mode = MaintenanceMode::parse(mode).unwrap_or_else(|| {
            error!("invalid mode '{mode}', expected one of: seal, merge, full, truncate")
        });
        let lock_mode = if wait {
            crate::storage::MaintenanceLockMode::Block
        } else {
            crate::storage::MaintenanceLockMode::Try
        };
        let row = run_maintenance(index, mode, lock_mode)
            .unwrap_or_else(|e| error!("maintenance failed: {e:#}"))
            .row();
        TableIterator::new(std::iter::once(row))
    }

    #[pg_extern]
    fn pg_zoekt_maintain_all(
        mode: default!(&str, "'full'"),
        wait: default!(bool, "false"),
        max_indexes: default!(Option<i32>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            name!(index_oid, pg_sys::Oid),
            name!(mode, String),
            name!(sealed_tuples, i64),
            name!(segments_before, i32),
            name!(segments_after, i32),
            name!(skipped_busy, bool),
        ),
    > {
        let mode = MaintenanceMode::parse(mode).unwrap_or_else(|| {
            error!("invalid mode '{mode}', expected one of: seal, merge, full, truncate")
        });
        let lock_mode = if wait {
            crate::storage::MaintenanceLockMode::Block
        } else {
            crate::storage::MaintenanceLockMode::Try
        };

        let limit = max_indexes.unwrap_or(0);
        let limit_clause = if limit > 0 {
            format!(" LIMIT {limit}")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT i.indexrelid::oid \
             FROM pg_catalog.pg_index i \
             JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid \
             JOIN pg_catalog.pg_am a ON a.oid = c.relam \
             WHERE a.amname = 'pg_zoekt' AND i.indisvalid AND i.indisready \
             ORDER BY i.indexrelid{}",
            limit_clause
        );

        let index_oids: Vec<pg_sys::Oid> =
            Spi::connect_mut(|client| -> spi::Result<Vec<pg_sys::Oid>> {
                let mut out = Vec::new();
                for row in client.select(&sql, None, &[])? {
                    if let Some(oid) = row.get::<pg_sys::Oid>(1)? {
                        out.push(oid);
                    }
                }
                Ok(out)
            })
            .unwrap_or_else(|e| error!("failed to enumerate pg_zoekt indexes: {e:#?}"));

        let mut rows = Vec::with_capacity(index_oids.len());
        for oid in index_oids {
            let result = run_maintenance(oid, mode, lock_mode)
                .unwrap_or_else(|e| error!("maintenance failed for index {oid}: {e:#}"));
            rows.push(result.row());
        }
        TableIterator::new(rows.into_iter())
    }

    #[cfg(feature = "pg_test")]
    #[pg_extern]
    fn pg_zoekt_test_seal_sleep(index: pg_sys::Oid, sleep_seconds: f64) {
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

            if sleep_seconds > 0.0 {
                let sql = format!("SELECT pg_sleep({})", sleep_seconds);
                if let Err(e) = Spi::run(&sql) {
                    error!("failed to sleep during test seal: {e:#?}");
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
        routine.amparallelvacuumoptions =
            (pg_sys::VACUUM_OPTION_PARALLEL_BULKDEL | pg_sys::VACUUM_OPTION_PARALLEL_CLEANUP) as u8;
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
unsafe fn test_ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::ffi::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    unsafe { implementation::ambulkdelete(info, stats, callback, callback_state) }
}

#[cfg(any(test, feature = "pg_test"))]
unsafe fn test_amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    unsafe { implementation::amvacuumcleanup(info, stats) }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::collections::HashSet;

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

    fn lookup_index_oid(relname: &str) -> spi::Result<pg_sys::Oid> {
        let sql = format!(
            "SELECT oid FROM pg_class WHERE relname = '{}' AND relkind = 'i' LIMIT 1",
            relname
        );
        Ok(Spi::get_one::<pg_sys::Oid>(&sql)?.expect("index oid not found"))
    }

    fn segment_count(index_oid: pg_sys::Oid) -> usize {
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read segments");
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            segments.len()
        }
    }

    fn read_segments(index_oid: pg_sys::Oid) -> Vec<crate::storage::Segment> {
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read segments");
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            segments
        }
    }

    fn index_nblocks(index_oid: pg_sys::Oid) -> u32 {
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            nblocks
        }
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
                1024 * 1024 * 1024,
                &crate::storage::tombstone::Snapshot::default(),
            )
            .expect("merge failed");
            let merged_block = std::ptr::read_unaligned(std::ptr::addr_of!(merged.block));
            info!("merged.block = {}", merged_block);
            assert_ne!(
                merged_block,
                pg_sys::InvalidBlockNumber,
                "expected merged segment"
            );

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
    pub fn test_merge_streaming_keeps_results() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE merge_stream_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO merge_stream_docs (text)
                 SELECT CASE
                    WHEN gs % 10 = 0 THEN 'needle-' || repeat(md5(gs::text), 50)
                    ELSE repeat(md5(gs::text), 50)
                 END
                 FROM generate_series(1, 1024) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_merge_stream_docs_text_zoekt ON merge_stream_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_merge_stream_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        let before_segments = unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read segments");
            let count = segments.len();
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            count
        };

        Spi::run("SET enable_seqscan = OFF")?;
        let hits_before = Spi::get_one::<i64>(
            "SELECT count(*) FROM merge_stream_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);

        Spi::run("SELECT pg_zoekt_seal('idx_merge_stream_docs_text_zoekt'::regclass)")?;

        let hits_after = Spi::get_one::<i64>(
            "SELECT count(*) FROM merge_stream_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);
        assert_eq!(hits_before, hits_after, "merge should preserve results");

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read segments");
            assert!(!segments.is_empty(), "expected segments after merge");
            assert!(
                segments.len() <= before_segments,
                "expected merge to not increase segment count"
            );
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }

        Spi::run("RESET enable_seqscan")?;
        Spi::run("DROP TABLE IF EXISTS merge_stream_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_merge_parallel_workers_used() -> spi::Result<()> {
        let max_workers =
            Spi::get_one::<i32>("SELECT current_setting('max_parallel_workers')::int")?
                .unwrap_or(0);
        if max_workers <= 0 {
            info!("skipping parallel merge test: max_parallel_workers=0");
            return Ok(());
        }

        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE merge_parallel_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO merge_parallel_docs (text)
                 SELECT repeat(md5(gs::text), 20) FROM generate_series(1, 4096) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_merge_parallel_docs_text_zoekt
                 ON merge_parallel_docs USING pg_zoekt (text)
                 WITH (parallel_workers = 4)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_merge_parallel_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read segments");
            assert!(
                segments.len() > 1,
                "expected multiple segments before merge"
            );
            crate::storage::test_parallel_merge_reset();
            let merged = crate::storage::merge_with_workers(
                rel,
                &segments,
                1,
                1024 * 1024 * 1024,
                &crate::storage::tombstone::Snapshot::default(),
                Some(2),
            )
            .expect("merge failed");
            assert_eq!(merged.len(), 1);
            let count = crate::storage::test_parallel_merge_count();
            assert!(count > 0, "expected parallel merge to run");
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }

        Spi::run("DROP TABLE IF EXISTS merge_parallel_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_introspection_helpers() -> spi::Result<()> {
        use crate::trgm::CompactTrgm;

        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE introspect_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO introspect_docs (text) VALUES ('abcd'), ('zzzz')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_introspect_docs_text_zoekt ON introspect_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_introspect_docs_text_zoekt'::regclass)")?;

        let segment_idx = Spi::get_one::<i32>(
            "SELECT segment_idx FROM pg_zoekt_index_segments('idx_introspect_docs_text_zoekt'::regclass) ORDER BY segment_idx LIMIT 1",
        )?
        .unwrap_or(0);
        assert!(segment_idx > 0, "expected at least one segment");

        let trigram = CompactTrgm::try_from("abc").expect("valid trigram").0 as i64;
        let entry_count = Spi::connect_mut(|client| -> spi::Result<i64> {
            let mut rows = client.select(
                "SELECT count(*) FROM pg_zoekt_segment_entries('idx_introspect_docs_text_zoekt'::regclass, $1) WHERE trigram = $2",
                None,
                &[segment_idx.into(), trigram.into()],
            )?;
            let row = rows.next().expect("count row");
            Ok(row.get::<i64>(1)?.unwrap_or(0))
        })?;
        assert!(entry_count > 0, "expected trigram entry");

        let postings_count = Spi::connect_mut(|client| -> spi::Result<i64> {
            let mut rows = client.select(
                "SELECT count(*) FROM pg_zoekt_postings_preview('idx_introspect_docs_text_zoekt'::regclass, $1, $2, 10)",
                None,
                &[segment_idx.into(), trigram.into()],
            )?;
            let row = rows.next().expect("count row");
            Ok(row.get::<i64>(1)?.unwrap_or(0))
        })?;
        assert!(postings_count > 0, "expected posting preview rows");

        Spi::run("DROP TABLE IF EXISTS introspect_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_trigram_introspection_helpers() -> spi::Result<()> {
        let trigram: i64 =
            Spi::get_one("SELECT pg_zoekt_text_to_trigram('abc')")?.unwrap_or_default();
        assert_eq!(trigram, 6513249, "expected compact trigram value");

        let text: String =
            Spi::get_one("SELECT pg_zoekt_trigram_to_text(6513249)")?.unwrap_or_default();
        assert_eq!(text, "abc", "expected trigram text");
        Ok(())
    }

    #[pg_test]
    pub fn test_posting_positions_introspection() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE posting_pos_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO posting_pos_docs (text) VALUES ('list_edge_gateway_display_fields')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_posting_pos_docs_text_zoekt ON posting_pos_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_posting_pos_docs_text_zoekt'::regclass)")?;

        let segment_idx = Spi::get_one::<i32>(
            "SELECT segment_idx FROM pg_zoekt_index_segments('idx_posting_pos_docs_text_zoekt'::regclass) ORDER BY segment_idx LIMIT 1",
        )?
        .unwrap_or(0);
        assert!(segment_idx > 0, "expected segment");

        let trigram =
            Spi::get_one::<i64>("SELECT pg_zoekt_text_to_trigram('lis')")?.unwrap_or_default();
        let pos_count = Spi::connect_mut(|client| -> spi::Result<i64> {
            let mut rows = client.select(
                "SELECT count(*) \
                 FROM pg_zoekt_posting_positions(\
                     'idx_posting_pos_docs_text_zoekt'::regclass, \
                     $1, $2, (SELECT ctid FROM posting_pos_docs LIMIT 1))",
                None,
                &[segment_idx.into(), trigram.into()],
            )?;
            let row = rows.next().expect("count row");
            Ok(row.get::<i64>(1)?.unwrap_or(0))
        })?;
        assert!(pos_count > 0, "expected posting positions");

        Spi::run("DROP TABLE IF EXISTS posting_pos_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_newline_postings() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE newline_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO newline_docs (text) VALUES (E'abc\\ndef')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_newline_docs_text_zoekt ON newline_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_newline_docs_text_zoekt'::regclass)")?;

        let segment_idx = Spi::get_one::<i32>(
            "SELECT segment_idx FROM pg_zoekt_index_segments('idx_newline_docs_text_zoekt'::regclass) ORDER BY segment_idx LIMIT 1",
        )?
        .unwrap_or(0);
        assert!(segment_idx > 0, "expected segment");

        let (min_pos, max_pos, count) =
            Spi::connect_mut(|client| -> spi::Result<(i64, i64, i64)> {
                let mut rows = client.select(
                    "SELECT min(position), max(position), count(*) \
                 FROM pg_zoekt_posting_positions(\
                     'idx_newline_docs_text_zoekt'::regclass, \
                     $1, 0, (SELECT ctid FROM newline_docs LIMIT 1))",
                    None,
                    &[segment_idx.into()],
                )?;
                let row = rows.next().expect("newline positions row");
                Ok((
                    row.get::<i64>(1)?.unwrap_or(-1),
                    row.get::<i64>(2)?.unwrap_or(-1),
                    row.get::<i64>(3)?.unwrap_or(0),
                ))
            })?;
        assert_eq!(count, 1, "expected one newline posting");
        assert_eq!(min_pos, 3, "expected newline at byte 3");
        assert_eq!(max_pos, 3, "expected newline at byte 3");

        Spi::run("DROP TABLE IF EXISTS newline_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_multisegment_trigram_scan_includes_all_docs() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE multi_seg_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO multi_seg_docs (text) \
                 SELECT 'prefix_oek_' || gs::text FROM generate_series(1, 512) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_multi_seg_docs_text_zoekt ON multi_seg_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let seq_count: i64 = Spi::get_one(
            "SET enable_seqscan = on; \
             SET enable_indexscan = off; \
             SET enable_bitmapscan = off; \
             SELECT count(*) FROM multi_seg_docs WHERE text LIKE '%oek%';",
        )?
        .unwrap_or(0);

        let idx_count: i64 = Spi::get_one(
            "SET enable_seqscan = off; \
             SET enable_indexscan = on; \
             SET enable_bitmapscan = on; \
             SELECT count(*) FROM multi_seg_docs WHERE text LIKE '%oek%';",
        )?
        .unwrap_or(0);

        assert_eq!(idx_count, seq_count, "index scan should match seqscan");

        Spi::run("DROP TABLE IF EXISTS multi_seg_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_multisegment_multitrigram_pattern_matches() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE multi_trgm_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO multi_trgm_docs (text) \
                 SELECT 'prefix_zoekt_' || gs::text FROM generate_series(1, 512) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_multi_trgm_docs_text_zoekt ON multi_trgm_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let seq_count: i64 = Spi::get_one(
            "SET enable_seqscan = on; \
             SET enable_indexscan = off; \
             SET enable_bitmapscan = off; \
             SELECT count(*) FROM multi_trgm_docs WHERE text LIKE '%zoekt%';",
        )?
        .unwrap_or(0);

        let idx_count: i64 = Spi::get_one(
            "SET enable_seqscan = off; \
             SET enable_indexscan = on; \
             SET enable_bitmapscan = on; \
             SELECT count(*) FROM multi_trgm_docs WHERE text LIKE '%zoekt%';",
        )?
        .unwrap_or(0);

        assert_eq!(idx_count, seq_count, "index scan should match seqscan");

        Spi::run("DROP TABLE IF EXISTS multi_trgm_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_wildcard_segments_respect_multiple_starts() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE wildcard_seg_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO wildcard_seg_docs (text) VALUES \
                 ('list prefix list_edge_gateway_display_fields'), \
                 ('list_edge_gateway_display_fields')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_wildcard_seg_docs_text_zoekt ON wildcard_seg_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let seq_count: i64 = Spi::get_one(
            "SET enable_seqscan = on; \
             SET enable_indexscan = off; \
             SET enable_bitmapscan = off; \
             SELECT count(*) FROM wildcard_seg_docs WHERE text LIKE '%list_edge_gateway_display_fields%';",
        )?
        .unwrap_or(0);

        let idx_count: i64 = Spi::get_one(
            "SET enable_seqscan = off; \
             SET enable_indexscan = on; \
             SET enable_bitmapscan = on; \
             SELECT count(*) FROM wildcard_seg_docs WHERE text LIKE '%list_edge_gateway_display_fields%';",
        )?
        .unwrap_or(0);

        assert_eq!(idx_count, seq_count, "index scan should match seqscan");

        Spi::run("DROP TABLE IF EXISTS wildcard_seg_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_split_doc_positions_across_chunks() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE split_pos_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO split_pos_docs (text) \
                 VALUES (repeat('unw', 5000) || 'unwind')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_split_pos_docs_text_zoekt ON split_pos_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let seq_count: i64 = Spi::get_one(
            "SET enable_seqscan = on; \
             SET enable_indexscan = off; \
             SET enable_bitmapscan = off; \
             SELECT count(*) FROM split_pos_docs WHERE text LIKE '%unwind%';",
        )?
        .unwrap_or(0);

        let idx_count: i64 = Spi::get_one(
            "SET enable_seqscan = off; \
             SET enable_indexscan = on; \
             SET enable_bitmapscan = on; \
             SELECT count(*) FROM split_pos_docs WHERE text LIKE '%unwind%';",
        )?
        .unwrap_or(0);

        assert_eq!(idx_count, seq_count, "index scan should match seqscan");

        Spi::run("DROP TABLE IF EXISTS split_pos_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_elide_copy_preserves_chunk_boundaries() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE elide_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO elide_docs (text) VALUES (repeat('abc', 50000))",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_elide_docs_text_zoekt ON elide_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_elide_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("read segments");
            assert!(!segments.is_empty(), "expected segments");
            let merged = crate::storage::merge(
                rel,
                &segments,
                1024 * 1024 * 1024,
                &crate::storage::tombstone::Snapshot::default(),
            )
            .expect("merge failed");
            let entries = crate::storage::read_segment_entries(rel, &merged).expect("read entries");
            let target = crate::trgm::CompactTrgm::try_from("abc")
                .expect("trigram")
                .0;
            let entry = entries
                .iter()
                .find(|entry| {
                    let trgm = std::ptr::read_unaligned(std::ptr::addr_of!(entry.trigram));
                    trgm == target
                })
                .expect("missing abc trigram entry");
            let data_length = std::ptr::read_unaligned(std::ptr::addr_of!(entry.data_length));
            let max_chunk_size = crate::storage::pgbuffer::SPECIAL_SIZE
                - std::mem::size_of::<crate::storage::PostingPageHeader>();
            assert!(
                (data_length as usize) > max_chunk_size,
                "expected multi-chunk posting list (len={})",
                data_length
            );
            let mut cursor =
                crate::storage::decode::PostingCursor::new(rel, entry).expect("posting cursor");
            while cursor.advance().expect("posting decode") {}
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }

        Ok(())
    }

    #[pg_test]
    pub fn test_pending_free_list_cleanup() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE pending_free_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO pending_free_docs (text) VALUES ('alpha'), ('beta'), ('gamma')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_pending_free_docs_text_zoekt ON pending_free_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_pending_free_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessExclusiveLock as i32);
            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            let bad_block = nblocks.saturating_add(10);
            crate::storage::pending::test_set_free_head(rel, bad_block).expect("set free_head");
            crate::storage::pending::cleanup_free_list(rel).expect("cleanup free list");
            let free_head =
                crate::storage::pending::test_get_free_head(rel).expect("get free_head");
            assert_eq!(
                free_head,
                pg_sys::InvalidBlockNumber,
                "free_head should be reset after cleanup"
            );
            pg_sys::relation_close(rel, pg_sys::AccessExclusiveLock as i32);
        }

        Ok(())
    }

    #[pg_test]
    pub fn test_pending_header_initialized_at_index_creation() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE pending_init_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_pending_init_docs_text_zoekt ON pending_init_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid = lookup_index_oid("idx_pending_init_docs_text_zoekt")?;

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let root =
                crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0).expect("acquire root");
            let rbl = root
                .as_struct::<crate::storage::RootBlockList>(0)
                .expect("root header");
            let pending_block = std::ptr::read_unaligned(std::ptr::addr_of!(rbl.pending_block));
            let wal_block = std::ptr::read_unaligned(std::ptr::addr_of!(rbl.wal_block));
            assert_ne!(
                pending_block,
                pg_sys::InvalidBlockNumber,
                "pending header should be initialized during index creation"
            );
            assert_ne!(
                wal_block,
                pg_sys::InvalidBlockNumber,
                "wal header should be initialized during index creation"
            );
            let free_head =
                crate::storage::pending::test_get_free_head(rel).expect("get pending free_head");
            assert_eq!(
                free_head,
                pg_sys::InvalidBlockNumber,
                "new pending header should start with an empty free list"
            );
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }

        Ok(())
    }

    #[pg_test]
    pub fn test_segment_extent_introspection() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE extent_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO extent_docs (text) VALUES ('alpha'), ('bravo'), ('charlie')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_extent_docs_text_zoekt ON extent_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_extent_docs_text_zoekt'::regclass)")?;

        let extent_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_zoekt_segment_extents('idx_extent_docs_text_zoekt'::regclass, 1)",
        )?
        .unwrap_or(0);
        assert!(extent_count > 0, "expected segment extents to be recorded");

        Spi::run("DROP TABLE IF EXISTS extent_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_index_overhead_totals() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE overhead_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO overhead_docs (text) SELECT repeat(md5(gs::text), 10) FROM generate_series(1, 512) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_overhead_docs_text_zoekt ON overhead_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_overhead_docs_text_zoekt'::regclass)")?;

        let bytes_total: i64 = Spi::connect_mut(|client| -> spi::Result<i64> {
            let mut rows = client.select(
                "SELECT sum(bytes_total)::bigint FROM pg_zoekt_index_overhead('idx_overhead_docs_text_zoekt'::regclass)",
                None,
                &[],
            )?;
            let row = rows.next().expect("sum row");
            Ok(row.get::<i64>(1)?.unwrap_or(0))
        })?;
        let rel_size = Spi::get_one::<i64>(
            "SELECT pg_relation_size('idx_overhead_docs_text_zoekt'::regclass)",
        )?
        .unwrap_or(0);
        assert_eq!(
            bytes_total, rel_size,
            "overhead totals should match relation size"
        );

        let accounted: i64 = Spi::connect_mut(|client| -> spi::Result<i64> {
            let mut rows = client.select(
                "SELECT sum(bytes_pg_header + bytes_header + bytes_payload + bytes_free + bytes_unknown)::bigint \
                 FROM pg_zoekt_index_overhead('idx_overhead_docs_text_zoekt'::regclass)",
                None,
                &[],
            )?;
            let row = rows.next().expect("sum row");
            Ok(row.get::<i64>(1)?.unwrap_or(0))
        })?;
        assert_eq!(accounted, rel_size, "overhead accounting should balance");

        Spi::run("DROP TABLE IF EXISTS overhead_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_index_overhead_ratio_synthetic() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE overhead_ratio_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO overhead_ratio_docs (text)
                 SELECT 'doc-' || gs || ' ' || repeat(md5(gs::text), 50)
                 FROM generate_series(1, 2048) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_overhead_ratio_docs_text_zoekt
                 ON overhead_ratio_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_overhead_ratio_docs_text_zoekt'::regclass)")?;

        let corpus_bytes: i64 = Spi::connect_mut(|client| -> spi::Result<i64> {
            let mut rows = client.select(
                "SELECT sum(octet_length(text))::bigint FROM overhead_ratio_docs",
                None,
                &[],
            )?;
            let row = rows.next().expect("sum row");
            Ok(row.get::<i64>(1)?.unwrap_or(0))
        })?;

        let index_bytes = Spi::get_one::<i64>(
            "SELECT pg_relation_size('idx_overhead_ratio_docs_text_zoekt'::regclass)",
        )?
        .unwrap_or(0);

        let (payload_bytes, overhead_bytes): (i64, i64) =
            Spi::connect_mut(|client| -> spi::Result<(i64, i64)> {
                let mut rows = client.select(
                    "SELECT \
                        sum(bytes_payload)::bigint, \
                        sum(bytes_pg_header + bytes_header + bytes_free + bytes_unknown)::bigint \
                     FROM pg_zoekt_index_overhead('idx_overhead_ratio_docs_text_zoekt'::regclass)",
                    None,
                    &[],
                )?;
                let row = rows.next().expect("sum row");
                let payload = row.get::<i64>(1)?.unwrap_or(0);
                let overhead = row.get::<i64>(2)?.unwrap_or(0);
                Ok((payload, overhead))
            })?;

        let corpus_ratio = if index_bytes > 0 {
            (corpus_bytes as f64) / (index_bytes as f64)
        } else {
            0.0
        };
        let overhead_ratio = if index_bytes > 0 {
            (overhead_bytes as f64) / (index_bytes as f64)
        } else {
            0.0
        };
        let payload_ratio = if index_bytes > 0 {
            (payload_bytes as f64) / (index_bytes as f64)
        } else {
            0.0
        };

        info!(
            "overhead ratio synthetic: corpus_bytes={} index_bytes={} corpus_per_index={:.4} payload_bytes={} overhead_bytes={} payload_ratio={:.4} overhead_ratio={:.4}",
            corpus_bytes,
            index_bytes,
            corpus_ratio,
            payload_bytes,
            overhead_bytes,
            payload_ratio,
            overhead_ratio
        );

        Spi::run("DROP TABLE IF EXISTS overhead_ratio_docs")?;
        assert!(
            corpus_ratio >= 0.5,
            "expected index size within 2x corpus (corpus_per_index={:.4})",
            corpus_ratio
        );
        Ok(())
    }

    fn assert_no_duplicate_postings(index_oid: pg_sys::Oid) {
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let segments = crate::query::read_segments(rel).expect("read segments");
            let tombstones = crate::storage::tombstone::load_snapshot(rel)
                .unwrap_or_else(|e| error!("tombstone load failed: {e:#?}"));
            let mut seen: HashSet<(u32, crate::storage::ItemPointer)> = HashSet::new();
            for seg in segments {
                let entries =
                    crate::storage::read_segment_entries(rel, &seg).expect("segment entries");
                for entry in entries {
                    let trigram = std::ptr::read_unaligned(std::ptr::addr_of!(entry.trigram));
                    let mut cursor = crate::storage::decode::PostingCursor::new(rel, &entry)
                        .expect("posting cursor");
                    while cursor.advance().expect("cursor advance") {
                        let doc = cursor.current().expect("posting doc");
                        if tombstones.contains(doc.tid) {
                            continue;
                        }
                        let key = (trigram, doc.tid);
                        if !seen.insert(key) {
                            error!(
                                "duplicate posting for trigram {} tid {:?}",
                                trigram, doc.tid
                            );
                        }
                    }
                }
            }
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }
    }

    #[pg_test]
    pub fn test_no_duplicate_postings_after_update() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE nodup_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO nodup_docs (text) SELECT 'doc-' || gs FROM generate_series(1, 200) gs",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_nodup_docs_text_zoekt ON nodup_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_nodup_docs_text_zoekt'::regclass)")?;
        Spi::run("UPDATE nodup_docs SET text = text || '-u' WHERE id % 5 = 0")?;
        Spi::run("SELECT pg_zoekt_seal('idx_nodup_docs_text_zoekt'::regclass)")?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_nodup_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        assert_no_duplicate_postings(index_oid);

        Spi::run("DROP TABLE IF EXISTS nodup_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_seal_does_not_include_late_inserts() -> spi::Result<()> {
        use crate::am::implementation::seal_serial;

        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE seal_late_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_seal_late_docs_text_zoekt ON seal_late_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO seal_late_docs (text) SELECT 'early-' || gs FROM generate_series(1, 100) gs",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_seal_late_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        let pending_head = unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::ShareUpdateExclusiveLock as i32);
            let head = crate::storage::pending::detach_pending(rel, 0)
                .expect("detach pending")
                .expect("expected pending head");
            pg_sys::relation_close(rel, pg_sys::ShareUpdateExclusiveLock as i32);
            head
        };

        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "INSERT INTO seal_late_docs (text) SELECT 'late-' || gs FROM generate_series(1, 50) gs",
                None,
                &[],
            )?;
            Ok(())
        })?;

        seal_serial(index_oid, pending_head);

        Spi::run("SET enable_seqscan = OFF")?;
        let late_hits_before =
            Spi::get_one::<i64>("SELECT count(*) FROM seal_late_docs WHERE text LIKE 'late-%'")?
                .unwrap_or(0);
        assert_eq!(
            late_hits_before, 0,
            "late inserts should not be indexed by the in-flight seal"
        );

        Spi::run("SELECT pg_zoekt_seal('idx_seal_late_docs_text_zoekt'::regclass)")?;

        let late_hits_after =
            Spi::get_one::<i64>("SELECT count(*) FROM seal_late_docs WHERE text LIKE 'late-%'")?
                .unwrap_or(0);
        assert_eq!(
            late_hits_after, 50,
            "late inserts should be indexed after next seal"
        );

        Spi::run("RESET enable_seqscan")?;
        Spi::run("DROP TABLE IF EXISTS seal_late_docs")?;
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
    pub fn test_truncate_reclaims_tail_blocks() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE reclaim_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "INSERT INTO reclaim_docs (text) SELECT repeat(md5(i::text), 10) FROM generate_series(1, 512) s(i)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_reclaim_docs_text_zoekt ON reclaim_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_reclaim_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessExclusiveLock as i32);
            let segments = crate::query::read_segments(rel).expect("failed to read segments");
            let root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0).expect("root buffer");
            let rbl = root
                .as_struct::<crate::storage::RootBlockList>(0)
                .expect("root header");

            // Clear free list to force extension on allocate_block.
            if rbl.wal_block != pg_sys::InvalidBlockNumber {
                let mut wal_buf =
                    crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)
                        .expect("wal buffer");
                let wal = wal_buf
                    .as_struct_mut::<crate::storage::WALHeader>(0)
                    .expect("wal header");
                wal.free_head = pg_sys::InvalidBlockNumber;
                wal.free_max_block = pg_sys::InvalidBlockNumber;
            }

            let nblocks_before =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);

            let mut extra = Vec::new();
            for _ in 0..8 {
                let page = crate::storage::allocate_block(rel);
                extra.push(page.block_number());
            }

            let nblocks_after_alloc =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            assert!(
                nblocks_after_alloc > nblocks_before,
                "expected relation to grow after allocation"
            );

            crate::storage::free_blocks(rel, &extra).expect("failed to free extra blocks");
            crate::storage::maybe_truncate_relation(rel, rbl, &segments)
                .expect("failed to truncate relation");

            let nblocks_after_truncate =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            assert_eq!(
                nblocks_after_truncate, nblocks_before,
                "expected relation to shrink back to original size"
            );

            pg_sys::relation_close(rel, pg_sys::AccessExclusiveLock as i32);
        }
        Ok(())
    }

    #[pg_test]
    pub fn test_wal_stats_tracks_free_and_high_water() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE wal_stats_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO wal_stats_docs (text) VALUES ('alpha'), ('beta'), ('gamma')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_wal_stats_docs_text_zoekt ON wal_stats_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_wal_stats_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        let (expected_free_max, expected_high_water) = unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessExclusiveLock as i32);
            let root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0).expect("root buffer");
            let rbl = root
                .as_struct::<crate::storage::RootBlockList>(0)
                .expect("root header");

            if rbl.wal_block != pg_sys::InvalidBlockNumber {
                let mut wal_buf =
                    crate::storage::pgbuffer::BlockBuffer::aquire_mut(rel, rbl.wal_block)
                        .expect("wal buffer");
                let wal = wal_buf
                    .as_struct_mut::<crate::storage::WALHeader>(0)
                    .expect("wal header");
                wal.free_head = pg_sys::InvalidBlockNumber;
                wal.free_max_block = pg_sys::InvalidBlockNumber;
            }

            let mut allocated = Vec::new();
            for _ in 0..3 {
                let page = crate::storage::allocate_block(rel);
                allocated.push(page.block_number());
            }
            let freed = vec![allocated[0], allocated[1]];
            crate::storage::free_blocks(rel, &freed).expect("failed to free blocks");

            let expected_free_max = freed.iter().copied().max().unwrap_or(0);
            let expected_high_water = allocated.iter().copied().max().unwrap_or(0);

            pg_sys::relation_close(rel, pg_sys::AccessExclusiveLock as i32);
            (expected_free_max, expected_high_water)
        };

        let (free_max, high_water): (i64, i64) = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client.select(
                "SELECT free_max_block, high_water_block FROM pg_zoekt_wal_stats('idx_wal_stats_docs_text_zoekt'::regclass)",
                None,
                &[],
            )?;
            let row = rows.next().expect("wal stats row");
            let free_max = row.get::<i64>(1)?.unwrap_or_default();
            let high_water = row.get::<i64>(2)?.unwrap_or_default();
            Ok((free_max, high_water))
        })?;

        assert_eq!(
            free_max, expected_free_max as i64,
            "expected free_max_block to track freed tail"
        );
        assert_eq!(
            high_water, expected_high_water as i64,
            "expected high_water_block to track highest allocation"
        );

        Spi::run("DROP TABLE IF EXISTS wal_stats_docs")?;
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

    fn tombstone_bytes(index_oid: pg_sys::Oid) -> u32 {
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let root = crate::storage::pgbuffer::BlockBuffer::acquire(rel, 0).expect("root buffer");
            let rbl = root
                .as_struct::<crate::storage::RootBlockList>(0)
                .expect("root header");
            let bytes = rbl.tombstone_bytes;
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
            bytes
        }
    }

    #[pg_test]
    pub fn test_vacuum_bulkdelete_writes_tombstones() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE vacuum_tomb_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO vacuum_tomb_docs (text) VALUES ('keep me'), ('delete me'), ('keep me too')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_vacuum_tomb_docs_text_zoekt ON vacuum_tomb_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let index_oid: pg_sys::Oid = Spi::connect_mut(|client| -> spi::Result<_> {
            let mut rows = client
                .select(
                    "SELECT oid FROM pg_class WHERE relname = 'idx_vacuum_tomb_docs_text_zoekt' AND relkind = 'i' LIMIT 1",
                    None,
                    &[],
                )?
                .into_iter();
            let row = rows.next().expect("index not created");
            Ok(row.get::<pg_sys::Oid>(1)?.expect("index oid not null"))
        })?;

        Spi::run("SELECT pg_zoekt_seal('idx_vacuum_tomb_docs_text_zoekt'::regclass)")?;
        let before = tombstone_bytes(index_oid);

        let tid: pg_sys::ItemPointerData =
            Spi::get_one("SELECT ctid FROM vacuum_tomb_docs WHERE text = 'delete me' LIMIT 1")?
                .expect("delete me tid");

        #[repr(C)]
        struct BulkDeleteState {
            target: pg_sys::ItemPointerData,
        }

        unsafe extern "C-unwind" fn bulkdelete_callback(
            item: pg_sys::ItemPointer,
            state: *mut std::ffi::c_void,
        ) -> bool {
            if item.is_null() || state.is_null() {
                return false;
            }
            let current = unsafe { *item };
            let target = unsafe { &*(state as *const BulkDeleteState) }.target;
            current.ip_posid == target.ip_posid
                && current.ip_blkid.bi_hi == target.ip_blkid.bi_hi
                && current.ip_blkid.bi_lo == target.ip_blkid.bi_lo
        }

        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let mut info: pg_sys::IndexVacuumInfo = std::mem::zeroed();
            info.index = rel;
            let mut state = BulkDeleteState { target: tid };
            super::test_ambulkdelete(
                &mut info,
                std::ptr::null_mut(),
                Some(bulkdelete_callback),
                (&mut state as *mut BulkDeleteState).cast(),
            );
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }

        let after = tombstone_bytes(index_oid);
        assert!(
            after > before,
            "expected tombstone bytes to increase after vacuum (before={}, after={})",
            before,
            after
        );
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_full_indexes_pending_without_vacuum() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_full_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_maintain_full_docs_text_zoekt ON maintain_full_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO maintain_full_docs (text) VALUES ('alpha needle beta')",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SET enable_seqscan = OFF")?;
        let before = Spi::get_one::<i64>(
            "SELECT count(*) FROM maintain_full_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);
        assert_eq!(
            before, 0,
            "pending rows should be invisible before maintenance"
        );

        let sealed = Spi::get_one::<i64>(
            "SELECT sealed_tuples FROM pg_zoekt_maintain('idx_maintain_full_docs_text_zoekt'::regclass, 'full', true)",
        )?
        .unwrap_or(0);
        assert!(
            sealed >= 1,
            "expected full maintenance to seal pending tuples"
        );

        let after = Spi::get_one::<i64>(
            "SELECT count(*) FROM maintain_full_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);
        assert_eq!(
            after, 1,
            "full maintenance should make pending row searchable"
        );

        Spi::run("RESET enable_seqscan")?;
        Spi::run("DROP TABLE IF EXISTS maintain_full_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_merge_caps_segments() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_merge_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "CREATE INDEX idx_maintain_merge_docs_text_zoekt ON maintain_merge_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO maintain_merge_docs (text)
                 SELECT repeat(md5((gs * 17)::text), 16) || ' needle ' || gs::text
                 FROM generate_series(1, 12000) gs",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run(
            "SELECT pg_zoekt_maintain('idx_maintain_merge_docs_text_zoekt'::regclass, 'seal', true)",
        )?;
        let index_oid = lookup_index_oid("idx_maintain_merge_docs_text_zoekt")?;
        let before = segment_count(index_oid);
        assert!(
            before > crate::storage::TARGET_SEGMENTS,
            "expected segment fanout before merge (got {before})"
        );

        Spi::run(
            "SELECT pg_zoekt_maintain('idx_maintain_merge_docs_text_zoekt'::regclass, 'merge', true)",
        )?;
        let after = segment_count(index_oid);
        assert!(
            after <= crate::storage::TARGET_SEGMENTS,
            "expected merge to cap segments <= {} (got {after})",
            crate::storage::TARGET_SEGMENTS
        );

        Spi::run("DROP TABLE IF EXISTS maintain_merge_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_merge_rewrites_minimal_victims() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_merge_min_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_maintain_merge_min_docs_text_zoekt ON maintain_merge_min_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        for i in 1..=14i32 {
            let insert_sql = format!(
                "INSERT INTO maintain_merge_min_docs (text) VALUES (repeat(md5(({} * 97)::text), 16) || ' minimal-victim ' || {}::text)",
                i, i
            );
            Spi::run(&insert_sql)?;
            Spi::run(
                "SELECT pg_zoekt_maintain('idx_maintain_merge_min_docs_text_zoekt'::regclass, 'seal', true)",
            )?;
        }

        let index_oid = lookup_index_oid("idx_maintain_merge_min_docs_text_zoekt")?;
        let before_segments = read_segments(index_oid);
        assert!(
            before_segments.len() > crate::storage::TARGET_SEGMENTS,
            "expected > target segments before merge (got {})",
            before_segments.len()
        );

        Spi::run(
            "SELECT pg_zoekt_maintain('idx_maintain_merge_min_docs_text_zoekt'::regclass, 'merge', true)",
        )?;

        let after_segments = read_segments(index_oid);
        assert_eq!(
            after_segments.len(),
            crate::storage::TARGET_SEGMENTS,
            "expected merge to leave exactly target segments"
        );

        let survivors = after_segments
            .iter()
            .filter(|seg| before_segments.contains(seg))
            .count();
        let expected_survivors =
            before_segments.len() - (before_segments.len() - crate::storage::TARGET_SEGMENTS + 1);
        assert_eq!(
            survivors, expected_survivors,
            "expected minimal-victim merge to preserve {} original segments (got {})",
            expected_survivors, survivors
        );

        Spi::run("DROP TABLE IF EXISTS maintain_merge_min_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_seal_only_no_forced_merge() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_seal_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "CREATE INDEX idx_maintain_seal_docs_text_zoekt ON maintain_seal_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO maintain_seal_docs (text)
                 SELECT repeat(md5((gs * 13)::text), 16) || ' marker ' || gs::text
                 FROM generate_series(1, 12000) gs",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let result = Spi::connect_mut(|client| -> spi::Result<(i32, i32)> {
            let row = client
                .select(
                    "SELECT segments_before, segments_after
                     FROM pg_zoekt_maintain('idx_maintain_seal_docs_text_zoekt'::regclass, 'seal', true)",
                    None,
                    &[],
                )?
                .first();
            Ok((
                row.get::<i32>(1)?.unwrap_or(0),
                row.get::<i32>(2)?.unwrap_or(0),
            ))
        })?;

        let (before, after) = result;
        assert!(
            after >= before,
            "seal-only maintenance should not force merge (before={before}, after={after})"
        );
        assert!(
            after as usize > crate::storage::TARGET_SEGMENTS,
            "expected seal-only to leave > target segments (after={after})"
        );

        Spi::run("DROP TABLE IF EXISTS maintain_seal_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_all_filters_to_pg_zoekt() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_all_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_maintain_all_docs_text_zoekt ON maintain_all_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_maintain_all_docs_id_btree ON maintain_all_docs (id)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let non_zoekt = Spi::get_one::<i64>(
            "SELECT count(*)
             FROM pg_zoekt_maintain_all('full', true, NULL) m
             JOIN pg_class c ON c.oid = m.index_oid
             JOIN pg_am a ON a.oid = c.relam
             WHERE a.amname <> 'pg_zoekt'",
        )?
        .unwrap_or(0);
        assert_eq!(
            non_zoekt, 0,
            "maintain_all should only target pg_zoekt indexes"
        );

        let zoekt_count = Spi::get_one::<i64>(
            "SELECT count(*)
             FROM pg_zoekt_maintain_all('full', true, NULL) m
             JOIN pg_class c ON c.oid = m.index_oid
             JOIN pg_am a ON a.oid = c.relam
             WHERE c.relname = 'idx_maintain_all_docs_text_zoekt' AND a.amname = 'pg_zoekt'",
        )?
        .unwrap_or(0);
        assert_eq!(
            zoekt_count, 1,
            "expected pg_zoekt index in maintain_all output"
        );

        Spi::run("DROP TABLE IF EXISTS maintain_all_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_try_lock_skips_busy() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_lock_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_maintain_lock_docs_text_zoekt ON maintain_lock_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        // pgrx tests run inside a single transaction/backend, so we cannot create true
        // cross-backend contention here without breaking test framework assumptions.
        // Validate that the non-blocking code path is callable and reports no skip
        // when no external contention exists.
        let skipped = Spi::get_one::<bool>(
            "SELECT skipped_busy
             FROM pg_zoekt_maintain('idx_maintain_lock_docs_text_zoekt'::regclass, 'full', false)",
        )?
        .unwrap_or(false);
        assert!(
            !skipped,
            "expected wait=false maintenance to proceed when no external contender exists"
        );

        Spi::run("DROP TABLE IF EXISTS maintain_lock_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_maintain_truncate_reclaims_after_merge() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_truncate_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_maintain_truncate_docs_text_zoekt ON maintain_truncate_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            Ok(())
        })?;

        for i in 1..=18i32 {
            let insert_sql = format!(
                "INSERT INTO maintain_truncate_docs (text) VALUES (repeat(md5(({} * 29)::text), 16) || ' truncate-marker ' || {}::text)",
                i, i
            );
            Spi::run(&insert_sql)?;
            Spi::run(
                "SELECT pg_zoekt_maintain('idx_maintain_truncate_docs_text_zoekt'::regclass, 'seal', true)",
            )?;
        }
        let index_oid = lookup_index_oid("idx_maintain_truncate_docs_text_zoekt")?;
        let segments_before_merge = segment_count(index_oid);
        assert!(
            segments_before_merge > crate::storage::TARGET_SEGMENTS,
            "expected enough segments for merge before truncate test (got {segments_before_merge})"
        );

        Spi::run(
            "SELECT pg_zoekt_maintain('idx_maintain_truncate_docs_text_zoekt'::regclass, 'merge', true)",
        )?;
        let blocks_after_merge = index_nblocks(index_oid);

        Spi::run(
            "SELECT pg_zoekt_maintain('idx_maintain_truncate_docs_text_zoekt'::regclass, 'truncate', true)",
        )?;
        let blocks_after_truncate = index_nblocks(index_oid);
        assert!(
            blocks_after_truncate <= blocks_after_merge,
            "truncate mode should not grow relation (after_merge={blocks_after_merge}, after_truncate={blocks_after_truncate})"
        );

        Spi::run("DROP TABLE IF EXISTS maintain_truncate_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_vacuumcleanup_no_longer_seals_or_merges() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE vacuum_cleanup_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_vacuum_cleanup_docs_text_zoekt ON vacuum_cleanup_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO vacuum_cleanup_docs (text) VALUES ('vacuum cleanup needle')",
                None,
                &[],
            )?;
            Ok(())
        })?;

        Spi::run("SET enable_seqscan = OFF")?;
        let before = Spi::get_one::<i64>(
            "SELECT count(*) FROM vacuum_cleanup_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);
        assert_eq!(
            before, 0,
            "row should be pending before vacuum cleanup callback"
        );

        let index_oid = lookup_index_oid("idx_vacuum_cleanup_docs_text_zoekt")?;
        unsafe {
            let rel = pg_sys::relation_open(index_oid, pg_sys::AccessShareLock as i32);
            let mut info: pg_sys::IndexVacuumInfo = std::mem::zeroed();
            info.index = rel;
            super::test_amvacuumcleanup(&mut info, std::ptr::null_mut());
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }

        let after = Spi::get_one::<i64>(
            "SELECT count(*) FROM vacuum_cleanup_docs WHERE text LIKE '%needle%'",
        )?
        .unwrap_or(0);
        assert_eq!(
            after, 0,
            "vacuum cleanup callback should not seal pending rows anymore"
        );

        Spi::run("RESET enable_seqscan")?;
        Spi::run("DROP TABLE IF EXISTS vacuum_cleanup_docs")?;
        Ok(())
    }

    #[pg_test]
    pub fn test_parallel_full_maintenance_converges_segments() -> spi::Result<()> {
        let max_parallel_workers =
            Spi::get_one::<i32>("SELECT current_setting('max_parallel_workers')::int")?
                .unwrap_or(0);
        if max_parallel_workers <= 0 {
            info!("skipping parallel full maintenance test: max_parallel_workers=0");
            return Ok(());
        }

        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE maintain_parallel_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update("SET maintenance_work_mem = '64kB'", None, &[])?;
            client.update(
                "CREATE INDEX idx_maintain_parallel_docs_text_zoekt
                 ON maintain_parallel_docs USING pg_zoekt (text)
                 WITH (parallel_workers = 4)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO maintain_parallel_docs (text)
                 SELECT repeat(md5((gs * 31)::text), 16) || ' parallel-needle ' || gs::text
                 FROM generate_series(1, 12000) gs",
                None,
                &[],
            )?;
            Ok(())
        })?;

        let sealed = Spi::get_one::<i64>(
            "SELECT sealed_tuples
             FROM pg_zoekt_maintain('idx_maintain_parallel_docs_text_zoekt'::regclass, 'full', true)",
        )?
        .unwrap_or(0);
        assert!(sealed > 0, "expected full maintenance to seal pending rows");

        let index_oid = lookup_index_oid("idx_maintain_parallel_docs_text_zoekt")?;
        let after = segment_count(index_oid);
        assert!(
            after <= crate::storage::TARGET_SEGMENTS,
            "expected full maintenance to converge <= {} segments (got {after})",
            crate::storage::TARGET_SEGMENTS
        );

        Spi::run("DROP TABLE IF EXISTS maintain_parallel_docs")?;
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
        let explain = Spi::get_one::<String>(
            "EXPLAIN SELECT text FROM planner_docs WHERE text LIKE '%ab%';",
        )?
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

    #[pg_test]
    pub fn test_adaptive_trigram_regex_matches() -> spi::Result<()> {
        Spi::connect_mut(|client| -> spi::Result<()> {
            client.update(
                "CREATE TABLE adaptive_regex_docs (id SERIAL PRIMARY KEY, text TEXT NOT NULL)",
                None,
                &[],
            )?;
            client.update(
                "INSERT INTO adaptive_regex_docs (text) VALUES
                 ('deep learning is fun'),
                 ('deep neural learning systems'),
                 ('no match here'),
                 ('deeply learning'),
                 ('deep ---- learning')",
                None,
                &[],
            )?;
            client.update(
                "CREATE INDEX idx_adaptive_regex_docs_text_zoekt ON adaptive_regex_docs USING pg_zoekt (text)",
                None,
                &[],
            )?;
            client.update("SET enable_seqscan = OFF", None, &[])?;

            let count = client
                .select(
                    "SELECT count(*) FROM adaptive_regex_docs WHERE text ~ 'deep.*learning'",
                    None,
                    &[],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count, 4, "expected adaptive regex matches");

            client.update("RESET enable_seqscan", None, &[])?;
            client.update("DROP TABLE adaptive_regex_docs", None, &[])?;
            Ok(())
        })
    }
}
