use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

use pgrx::ffi::c_char;
use pgrx::pg_sys::{self, Oid};
use pgrx::prelude::*;

const PARALLEL_BUILD_MAIN: *const c_char = c"_pg_zoekt_build_main".as_ptr();
const EXTENSION_NAME: &[u8] = b"pg_zoekt\0";

pub const SHM_TOC_SHARED_KEY: u64 = 0x5A4B540000000001; // "ZKT" namespace-ish
pub const SHM_TOC_TABLESCANDESC_KEY: u64 = 0x5A4B540000000002;
pub const SHM_TOC_FILESET_KEY: u64 = 0x5A4B540000000003;

/// Data about a parallel index build that never changes.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct ParallelSharedParams {
    heaprelid: Oid,
    indexrelid: Oid,
    is_concurrent: bool,
    worker_count: usize,
    flush_threshold: usize,
}

/// Shared build state for parallel index builds.
#[repr(C)]
struct ParallelBuildState {
    worker_slot: AtomicUsize,
    ntuples: AtomicUsize,
}

/// Status data for parallel index builds, shared among all parallel workers.
#[repr(C)]
struct ParallelShared {
    params: ParallelSharedParams,
    build_state: ParallelBuildState,
}

/// Complete definition of SharedFileSet to match C layout including the segment pointer
/// This struct is used for local copies where the 'segment' field is properly set.
/// The original pg_sys::SharedFileSet binding is incomplete.
#[repr(C)]
#[derive(Copy, Clone)]
struct SharedFileSetComplete {
    fs: pg_sys::FileSet,
    mutex: pg_sys::slock_t,
    refcnt: ::core::ffi::c_int,
    // It seems there is padding here in C struct due to alignment of pointer
    // FileSet (44) + mutex (1) + refcnt (4) = 49 bytes.
    // Next 8-byte aligned offset is 56. So 7 bytes of padding.
    // This assumes sizeof(FileSet) = 44 bytes.
    // Let's use 7 bytes padding for safety, or just trust repr(C) for padding.
    // Actually, `pg_sys::SharedFileSet` has no `_pad` field, the C struct `SharedFileSet`
    // has the `dsm_segment *segment` field *after* the `refcnt` field.
    // The `_pad` here is to account for alignment of the `dsm_segment*` pointer if `refcnt`
    // is not 8-byte aligned itself. `i32` is usually 4-byte aligned.
    // So if `refcnt` is at offset 48, then 48+4=52. Next 8-byte boundary is 56. So 4 bytes padding needed.
    _pad: [u8; 4],
    segment: *mut pg_sys::dsm_segment,
}

impl Default for SharedFileSetComplete {
    fn default() -> Self {
        SharedFileSetComplete {
            fs: pg_sys::FileSet::default(),
            mutex: 0, // slock_t is typically unsigned char, 0 is unlocked
            refcnt: 0,
            _pad: [0; 4],
            segment: std::ptr::null_mut(),
        }
    }
}

/// Reimplementation of Postgres BUFFERALIGN macro.
fn buffer_align(len: usize) -> usize {
    unsafe { pg_sys::TYPEALIGN(pg_sys::ALIGNOF_BUFFER as usize, len) }
}

/// Estimate a single chunk in the shared memory TOC.
unsafe fn toc_estimate_single_chunk(pcxt: *mut pg_sys::ParallelContext, size: usize) {
    unsafe {
        (*pcxt).estimator.space_for_chunks += buffer_align(size);
        (*pcxt).estimator.number_of_keys += 1;
    }
}

/// Is a snapshot MVCC-safe?
unsafe fn is_mvcc_snapshot(snapshot: *mut pg_sys::SnapshotData) -> bool {
    let typ = unsafe { (*snapshot).snapshot_type };
    typ == pg_sys::SnapshotType::SNAPSHOT_MVCC
        || typ == pg_sys::SnapshotType::SNAPSHOT_HISTORIC_MVCC
}

/// Cleans up a parallel context when we're done with it.
unsafe fn cleanup_parallel_context(
    pcxt: *mut pg_sys::ParallelContext,
    snapshot: *mut pg_sys::SnapshotData,
) {
    if unsafe { is_mvcc_snapshot(snapshot) } {
        unsafe { pg_sys::UnregisterSnapshot(snapshot) };
    }
    unsafe { pg_sys::DestroyParallelContext(pcxt) };
    unsafe { pg_sys::ExitParallelMode() };
}

/// Custom IndexBuildHeapScan that uses a parallel table scan descriptor.
#[allow(non_snake_case)]
unsafe fn IndexBuildHeapScanParallel<T>(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    build_callback: pg_sys::IndexBuildCallback,
    build_callback_state: *mut T,
    tablescandesc: *mut pg_sys::ParallelTableScanDescData,
) {
    let scan = unsafe { pg_sys::table_beginscan_parallel(heap_relation, tablescandesc) };

    let heap_relation_ref = unsafe { heap_relation.as_ref().expect("heap relation") };
    let table_am = unsafe { heap_relation_ref.rd_tableam.as_ref().expect("table am") };

    unsafe {
        table_am
            .index_build_range_scan
            .expect("index_build_range_scan")(
            heap_relation,
            index_relation,
            index_info,
            true,                       // allow_sync
            false,                      // anyvisible
            true,                       // progress
            0,                          // start_blockno
            pg_sys::InvalidBlockNumber, // end_blockno
            build_callback,
            build_callback_state.cast::<std::ffi::c_void>(),
            scan,
        );
    }
}

pub(super) unsafe fn build_parallel(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    root_block: u32,
    flush_threshold: usize,
) -> Option<usize> {
    let workers = unsafe { (*index_info).ii_ParallelWorkers as usize };
    info!("Building with {workers} parallel workers.");
    if workers == 0 {
        return None;
    }

    let is_concurrent = unsafe { (*index_info).ii_Concurrent };

    unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            EXTENSION_NAME.as_ptr().cast(),
            PARALLEL_BUILD_MAIN,
            workers as i32,
        );
        let snapshot = if is_concurrent {
            pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot())
        } else {
            &raw mut pg_sys::SnapshotAnyData
        };

        toc_estimate_single_chunk(pcxt, size_of::<ParallelShared>());
        // Allocate extra space for SharedFileSet because Rust definition misses the 'segment' pointer field
        let shared_fileset_size = size_of::<SharedFileSetComplete>();
        toc_estimate_single_chunk(pcxt, shared_fileset_size);
        let tablescandesc_size_estimate =
            pg_sys::table_parallelscan_estimate(heap_relation, snapshot);
        toc_estimate_single_chunk(pcxt, tablescandesc_size_estimate);

        pg_sys::InitializeParallelDSM(pcxt);
        if (*pcxt).seg.is_null() {
            cleanup_parallel_context(pcxt, snapshot);
            return None;
        }

        let parallel_shared = pg_sys::shm_toc_allocate((*pcxt).toc, size_of::<ParallelShared>())
            .cast::<ParallelShared>();
        if parallel_shared.is_null() {
            pgrx::error!("Failed to allocate ParallelShared in TOC");
        }

        parallel_shared.write(ParallelShared {
            params: ParallelSharedParams {
                heaprelid: heap_relation.as_ref().expect("heap relation").rd_id,
                indexrelid: index_relation.as_ref().expect("index relation").rd_id,
                is_concurrent,
                worker_count: workers,
                flush_threshold,
            },
            build_state: ParallelBuildState {
                worker_slot: AtomicUsize::new(0),
                ntuples: AtomicUsize::new(0),
            },
        });

        // Allocate SharedFileSetComplete in shared memory and initialize it
        let shared_fileset_ptr_raw = pg_sys::shm_toc_allocate((*pcxt).toc, shared_fileset_size);
        if shared_fileset_ptr_raw.is_null() {
            pgrx::error!("Failed to allocate SharedFileSetComplete in TOC");
        }
        let shared_fileset_ptr_complete = shared_fileset_ptr_raw.cast::<SharedFileSetComplete>();

        // Initialize the SharedFileSet with the leader's segment pointer
        pg_sys::SharedFileSetInit(
            shared_fileset_ptr_raw.cast::<pg_sys::SharedFileSet>(),
            (*pcxt).seg,
        );
        // Manually set the segment field in our complete struct to ensure it's correct for leader
        (*shared_fileset_ptr_complete).segment = (*pcxt).seg;

        let tablescandesc = pg_sys::shm_toc_allocate((*pcxt).toc, tablescandesc_size_estimate)
            .cast::<pg_sys::ParallelTableScanDescData>();
        if tablescandesc.is_null() {
            pgrx::error!("Failed to allocate tablescandesc in TOC");
        }
        pg_sys::table_parallelscan_initialize(heap_relation, tablescandesc, snapshot);

        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_SHARED_KEY, parallel_shared.cast());
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_FILESET_KEY,
            shared_fileset_ptr_raw.cast(),
        );
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_TABLESCANDESC_KEY, tablescandesc.cast());

        // Sanity check: Verify leader can find the keys immediately
        if pg_sys::shm_toc_lookup((*pcxt).toc, SHM_TOC_FILESET_KEY, true).is_null() {
            pgrx::error!(
                "Leader failed to lookup SHM_TOC_FILESET_KEY immediately after insert! Struct size: {}, FileSet size: {}",
                shared_fileset_size,
                size_of::<pg_sys::FileSet>()
            );
        }

        pg_sys::LaunchParallelWorkers(pcxt);
        if (*pcxt).nworkers_launched == 0 {
            cleanup_parallel_context(pcxt, snapshot);
            return None;
        }

        pg_sys::WaitForParallelWorkersToAttach(pcxt);
        pg_sys::WaitForParallelWorkersToFinish(pcxt);

        let parallel_shared: *mut ParallelShared =
            pg_sys::shm_toc_lookup((*pcxt).toc, SHM_TOC_SHARED_KEY, false).cast();
        let worker_slots = (*parallel_shared)
            .build_state
            .worker_slot
            .load(Ordering::Relaxed);

        // Leader must use a local copy of SharedFileSetComplete as workers might have overwritten
        // the segment pointer in the shared memory version.
        // Copy the shared fileset to a local variable.
        // The segment pointer in this local copy is already valid for the leader as set after Init.
        let mut leader_local_fileset: SharedFileSetComplete = *shared_fileset_ptr_complete;

        let mut all_segments = Vec::new();
        for slot in 0..worker_slots {
            let name = spill_file_name(slot);
            let file = pg_sys::BufFileOpenFileSet(
                &raw mut leader_local_fileset.fs, // Use the local copy's FileSet
                name.as_ptr().cast::<c_char>(),
                0,    // O_RDONLY
                true, // missing_ok
            );
            if file.is_null() {
                continue;
            }
            let worker_segments = collect_segments_from_spill_file(file);
            all_segments.extend(worker_segments);
            pg_sys::BufFileClose(file);
            pg_sys::BufFileDeleteFileSet(
                &raw mut leader_local_fileset.fs, // Use the local copy's FileSet
                name.as_ptr().cast::<c_char>(),
                true,
            );
        }

        // Now append all collected segments to the main index at once
        let mut root = match crate::storage::pgbuffer::BlockBuffer::aquire_mut(
            index_relation,
            root_block,
        ) {
            Ok(root) => root,
            Err(e) => {
                error!("failed to acquire root buffer: {e:#?}");
            }
        };
        let rbl = root
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .expect("root header");
        let magic = rbl.magic;
        let expected_magic = crate::storage::ROOT_MAGIC;
        if magic != expected_magic {
            error!(
                "corrupt root page at block {} (bad magic {}, expected {})",
                root_block, magic, expected_magic
            );
        }

        // Append all segments at once, potentially triggering compaction if needed
        crate::storage::segment_list_append(index_relation, rbl, &all_segments)
            .unwrap_or_else(|e| error!("failed to append segments: {e:#?}"));

        // Perform final merge if we have too many segments
        const MAX_ACTIVE_SEGMENTS: u32 = 512;
        const COMPACT_TARGET_SEGMENTS: usize = 64;
        if rbl.num_segments > MAX_ACTIVE_SEGMENTS {
            let existing = crate::storage::segment_list_read(index_relation, rbl)
                .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
            let tombstones = crate::storage::tombstone::Snapshot::default();
            let merged = crate::storage::merge(
                index_relation,
                &existing,
                COMPACT_TARGET_SEGMENTS,
                flush_threshold.saturating_mul(16).max(1024 * 1024),
                &tombstones,
            )
            .unwrap_or_else(|e| error!("failed to compact segments: {e:#?}"));
            crate::storage::segment_list_rewrite(index_relation, rbl, &merged)
                .unwrap_or_else(|e| error!("failed to rewrite segment list: {e:#?}"));
        }

        let ntuples = (*parallel_shared)
            .build_state
            .ntuples
            .load(Ordering::Relaxed);

        // Clean up fileset (requires shared pointer)
        // Note: pg_sys::SharedFileSetDeleteAll takes *mut pg_sys::SharedFileSet
        pg_sys::SharedFileSetDeleteAll(shared_fileset_ptr_raw.cast::<pg_sys::SharedFileSet>());

        cleanup_parallel_context(pcxt, snapshot);
        Some(ntuples)
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn _pg_zoekt_build_main(
    seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    let status_flags = unsafe { (*pg_sys::MyProc).statusFlags };
    assert!(
        status_flags == 0 || status_flags == pg_sys::PROC_IN_SAFE_IC as u8,
        "Status flags for an index build process must be unset or PROC_IN_SAFE_IC"
    );

    let parallel_shared: *mut ParallelShared = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_SHARED_KEY, false).cast::<ParallelShared>()
    };
    let shared_fileset_ptr = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_FILESET_KEY, false).cast::<pg_sys::SharedFileSet>()
    };
    let tablescandesc = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_TABLESCANDESC_KEY, false)
            .cast::<pg_sys::ParallelTableScanDescData>()
    };

    let params = unsafe { (*parallel_shared).params };

    // Attach to the shared fileset to increment refcnt. This will also write
    // the 'segment' pointer into the shared struct.
    unsafe {
        pg_sys::SharedFileSetAttach(shared_fileset_ptr, seg);
    }

    // Create a local copy to work with, avoiding race conditions on the segment pointer
    // The shared_fileset_ptr points to a SharedFileSetComplete (our full definition).
    // Transmute to SharedFileSetComplete* to access segment.
    let mut local_fileset: SharedFileSetComplete =
        unsafe { *(shared_fileset_ptr.cast::<SharedFileSetComplete>()) };
    local_fileset.segment = seg;

    let (heap_lockmode, index_lockmode) = if params.is_concurrent {
        (
            pg_sys::ShareLock as pg_sys::LOCKMODE,
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        )
    } else {
        (
            pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        )
    };

    let heaprel = unsafe { pg_sys::table_open(params.heaprelid, heap_lockmode) };
    let indexrel = unsafe { pg_sys::index_open(params.indexrelid, index_lockmode) };
    let index_info = unsafe { pg_sys::BuildIndexInfo(indexrel) };

    // Get a unique slot for this worker
    let slot = unsafe {
        (*parallel_shared)
            .build_state
            .worker_slot
            .fetch_add(1, Ordering::Acquire)
    };
    let file_name = spill_file_name(slot);

    // Create the spill file using the local copy
    let spill_file = unsafe {
        pg_sys::BufFileCreateFileSet(
            &raw mut local_fileset.fs,
            file_name.as_ptr().cast::<c_char>(),
        )
    };
    if spill_file.is_null() {
        error!("failed to create worker spill file");
    }

    let key_count = unsafe { (*index_info).ii_NumIndexAttrs as usize };
    let mut callback_state = SpillState::new(
        key_count,
        params.flush_threshold,
        indexrel,
        spill_file,
        local_fileset, // Pass the local copy to SpillState
    );
    unsafe {
        IndexBuildHeapScanParallel(
            heaprel,
            indexrel,
            index_info,
            Some(log_index_value_callback_spill),
            &mut callback_state,
            tablescandesc,
        );
    }
    callback_state.flush();

    // Export the file so the main process can access it
    unsafe { pg_sys::BufFileExportFileSet(spill_file) };
    unsafe { pg_sys::BufFileClose(spill_file) };

    let seen: usize = callback_state.seen.try_into().unwrap_or(usize::MAX);
    unsafe {
        // Update the total tuple count with release ordering for visibility
        (*parallel_shared)
            .build_state
            .ntuples
            .fetch_add(seen, Ordering::Release);
        pg_sys::index_close(indexrel, index_lockmode);
        pg_sys::table_close(heaprel, heap_lockmode);
    }
}

fn spill_file_name(slot: usize) -> [u8; 64] {
    let mut file_name = [0u8; 64];
    let name_str = format!("pg_zoekt_build_{slot}");
    if name_str.len() >= file_name.len() {
        error!("spill file name too long");
    }
    file_name[..name_str.len()].copy_from_slice(name_str.as_bytes());
    file_name[name_str.len()] = 0;
    file_name
}

struct SpillState {
    key_count: usize,
    seen: u64,
    collector: crate::trgm::Collector,
    flush_threshold: usize,
    index_relation: pg_sys::Relation,
    file: *mut pg_sys::BufFile,
    // Keep the local fileset alive as long as the file is open
    _fileset: SharedFileSetComplete,
}

impl SpillState {
    fn new(
        key_count: usize,
        flush_threshold: usize,
        index_relation: pg_sys::Relation,
        file: *mut pg_sys::BufFile,
        fileset: SharedFileSetComplete,
    ) -> Self {
        Self {
            key_count,
            seen: 0,
            collector: crate::trgm::Collector::new(),
            flush_threshold,
            index_relation,
            file,
            _fileset: fileset,
        }
    }

    fn flush_if_needed(&mut self) {
        if self.collector.memory_usage() >= self.flush_threshold {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let trgms = self.collector.take_trgms();
        if trgms.is_empty() {
            return;
        }

        // Write the actual data pages to the index relation
        let segments = crate::storage::encode::Encoder::encode_trgms(self.index_relation, &trgms)
            .unwrap_or_else(|e| error!("failed to encode segments: {e:#?}"));

        // Write the segment metadata to the spill file so the leader can find them
        write_segment_batch(self.file, &segments);
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
unsafe extern "C-unwind" fn log_index_value_callback_spill(
    _index: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut std::ffi::c_void,
) {
    unsafe {
        let state = &mut *(state as *mut SpillState);
        if state.key_count == 0 || tid.is_null() || !tuple_is_alive {
            return;
        }

        let ctid: crate::storage::ItemPointer = match tid.try_into() {
            Ok(ctid) => ctid,
            Err(e) => error!("failed to parse tid: {e:#?}"),
        };

        let values = std::slice::from_raw_parts(values, state.key_count);
        let isnull = std::slice::from_raw_parts(isnull, state.key_count);
        if isnull[0] {
            return;
        }

        if let Some(text) = <&str>::from_datum(values[0], false) {
            let mut extracted = 0usize;
            for (trgm, pos) in crate::trgm::Extractor::extract(text) {
                _ = state.collector.add(ctid, trgm, pos as u32);
                extracted += 1;
                if (extracted & 0xff) == 0 {
                    pg_sys::check_for_interrupts!();
                    state.flush_if_needed();
                }
            }
        }

        state.seen += 1;
        pg_sys::check_for_interrupts!();
        state.flush_if_needed();
    }
}

fn write_segment_batch(file: *mut pg_sys::BufFile, segments: &[crate::storage::Segment]) {
    fn write_bytes(file: *mut pg_sys::BufFile, bytes: &[u8]) {
        unsafe { pg_sys::BufFileWrite(file, bytes.as_ptr().cast(), bytes.len()) }
    }

    fn write_u32(file: *mut pg_sys::BufFile, v: u32) {
        write_bytes(file, &v.to_le_bytes());
    }

    fn write_u64(file: *mut pg_sys::BufFile, v: u64) {
        write_bytes(file, &v.to_le_bytes());
    }

    // Write the segment list to the spill file
    write_u32(file, segments.len() as u32);
    for seg in segments {
        write_u32(file, seg.block);
        write_u64(file, seg.size);
    }
}

fn collect_segments_from_spill_file(file: *mut pg_sys::BufFile) -> Vec<crate::storage::Segment> {
    fn read_exact_or_eof(file: *mut pg_sys::BufFile, buf: &mut [u8]) -> Option<()> {
        let read =
            unsafe { pg_sys::BufFileReadMaybeEOF(file, buf.as_mut_ptr().cast(), buf.len(), true) };
        if read == 0 {
            return None;
        }
        if read != buf.len() {
            error!("short read in spill file");
        }
        Some(())
    }

    fn read_u32(file: *mut pg_sys::BufFile) -> Option<u32> {
        let mut buf = [0u8; 4];
        read_exact_or_eof(file, &mut buf)?;
        Some(u32::from_le_bytes(buf))
    }

    fn read_u64(file: *mut pg_sys::BufFile) -> Option<u64> {
        let mut buf = [0u8; 8];
        read_exact_or_eof(file, &mut buf)?;
        Some(u64::from_le_bytes(buf))
    }

    let mut segments = Vec::new();

    loop {
        let Some(count) = read_u32(file) else {
            break;
        };

        for _ in 0..count {
            let block = read_u32(file).unwrap_or_else(|| error!("unexpected eof (block)"));
            let size = read_u64(file).unwrap_or_else(|| error!("unexpected eof (size)"));
            segments.push(crate::storage::Segment { block, size });
        }
    }

    segments
}
