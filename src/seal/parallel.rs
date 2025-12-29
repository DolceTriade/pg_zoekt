use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

use pgrx::ffi::c_char;
use pgrx::pg_sys::{self, Oid};
use pgrx::prelude::*;

const PARALLEL_SEAL_MAIN: *const c_char = c"_pg_zoekt_seal_main".as_ptr();
const EXTENSION_NAME: &[u8] = b"pg_zoekt\0";

const SHM_TOC_SHARED_KEY: u64 = 0x5A4B540000000101;
const SHM_TOC_FILESET_KEY: u64 = 0x5A4B540000000102;
const SHM_TOC_BLOCKS_KEY: u64 = 0x5A4B540000000103;
const SHM_TOC_SNAPSHOT_KEY: u64 = 0x5A4B540000000104;

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct ParallelSealParams {
    heaprelid: Oid,
    indexrelid: Oid,
    pending_blocks_len: usize,
    worker_count: usize,
    flush_threshold: usize,
}

#[repr(C)]
struct ParallelSealState {
    worker_slot: AtomicUsize,
    next_block: AtomicUsize,
    ntuples: AtomicUsize,
}

#[repr(C)]
struct ParallelSealShared {
    params: ParallelSealParams,
    state: ParallelSealState,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct SharedFileSetComplete {
    fs: pg_sys::FileSet,
    mutex: pg_sys::slock_t,
    refcnt: ::core::ffi::c_int,
    _pad: [u8; 4],
    segment: *mut pg_sys::dsm_segment,
}

impl Default for SharedFileSetComplete {
    fn default() -> Self {
        Self {
            fs: pg_sys::FileSet::default(),
            mutex: 0,
            refcnt: 0,
            _pad: [0; 4],
            segment: std::ptr::null_mut(),
        }
    }
}

fn buffer_align(len: usize) -> usize {
    unsafe { pg_sys::TYPEALIGN(pg_sys::ALIGNOF_BUFFER as usize, len) }
}

unsafe fn toc_estimate_single_chunk(pcxt: *mut pg_sys::ParallelContext, size: usize) {
    unsafe {
        (*pcxt).estimator.space_for_chunks += buffer_align(size);
        (*pcxt).estimator.number_of_keys += 1;
    }
}

unsafe fn cleanup_parallel_context(pcxt: *mut pg_sys::ParallelContext) {
    unsafe {
        pg_sys::DestroyParallelContext(pcxt);
        pg_sys::ExitParallelMode();
    }
}

pub(crate) unsafe fn seal_parallel(
    index_oid: Oid,
    pending_head: u32,
    workers: usize,
    flush_threshold: usize,
) -> Option<u64> {
    unsafe {
        if workers == 0 {
            return None;
        }

        let index_rel = pg_sys::relation_open(index_oid, pg_sys::RowExclusiveLock as i32);
        let blocks = crate::storage::pending::collect_blocks(index_rel, pending_head)
            .unwrap_or_else(|e| error!("failed to collect pending blocks: {e:#?}"));
        if blocks.is_empty() {
            pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);
            return None;
        }

        pg_sys::EnterParallelMode();
        let pcxt = pg_sys::CreateParallelContext(
            EXTENSION_NAME.as_ptr().cast(),
            PARALLEL_SEAL_MAIN,
            workers as i32,
        );

        let heap_oid = pg_sys::IndexGetRelation(index_oid, false);
        let params = ParallelSealParams {
            heaprelid: heap_oid,
            indexrelid: index_oid,
            pending_blocks_len: blocks.len(),
            worker_count: workers,
            flush_threshold,
        };

        let snapshot = pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot());
        let snapshot_size = pg_sys::EstimateSnapshotSpace(snapshot) as usize;

        toc_estimate_single_chunk(pcxt, size_of::<ParallelSealShared>());
        let shared_fileset_size = size_of::<SharedFileSetComplete>();
        toc_estimate_single_chunk(pcxt, shared_fileset_size);
        toc_estimate_single_chunk(pcxt, blocks.len() * size_of::<u32>());
        toc_estimate_single_chunk(pcxt, snapshot_size);

        pg_sys::InitializeParallelDSM(pcxt);
        if (*pcxt).seg.is_null() {
            cleanup_parallel_context(pcxt);
            pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);
            return None;
        }

        let parallel_shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, size_of::<ParallelSealShared>())
                .cast::<ParallelSealShared>();
        if parallel_shared.is_null() {
            pgrx::error!("failed to allocate ParallelSealShared");
        }
        parallel_shared.write(ParallelSealShared {
            params,
            state: ParallelSealState {
                worker_slot: AtomicUsize::new(0),
                next_block: AtomicUsize::new(0),
                ntuples: AtomicUsize::new(0),
            },
        });

        let shared_fileset_ptr_raw = pg_sys::shm_toc_allocate((*pcxt).toc, shared_fileset_size);
        if shared_fileset_ptr_raw.is_null() {
            pgrx::error!("failed to allocate SharedFileSetComplete");
        }
        let shared_fileset_ptr_complete = shared_fileset_ptr_raw.cast::<SharedFileSetComplete>();
        pg_sys::SharedFileSetInit(
            shared_fileset_ptr_raw.cast::<pg_sys::SharedFileSet>(),
            (*pcxt).seg,
        );
        (*shared_fileset_ptr_complete).segment = (*pcxt).seg;

        let blocks_ptr =
            pg_sys::shm_toc_allocate((*pcxt).toc, blocks.len() * size_of::<u32>()).cast::<u32>();
        if blocks_ptr.is_null() {
            pgrx::error!("failed to allocate pending blocks array");
        }
        std::ptr::copy_nonoverlapping(blocks.as_ptr(), blocks_ptr, blocks.len());

        let snapshot_ptr = pg_sys::shm_toc_allocate((*pcxt).toc, snapshot_size).cast::<c_char>();
        if snapshot_ptr.is_null() {
            pgrx::error!("failed to allocate seal snapshot buffer");
        }
        pg_sys::SerializeSnapshot(snapshot, snapshot_ptr);

        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_SHARED_KEY, parallel_shared.cast());
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_FILESET_KEY,
            shared_fileset_ptr_raw.cast(),
        );
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_BLOCKS_KEY, blocks_ptr.cast());
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_SNAPSHOT_KEY, snapshot_ptr.cast());

        pg_sys::LaunchParallelWorkers(pcxt);
        if (*pcxt).nworkers_launched == 0 {
            pg_sys::UnregisterSnapshot(snapshot);
            cleanup_parallel_context(pcxt);
            pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);
            return None;
        }
        pg_sys::WaitForParallelWorkersToAttach(pcxt);
        pg_sys::WaitForParallelWorkersToFinish(pcxt);

        let parallel_shared: *mut ParallelSealShared =
            pg_sys::shm_toc_lookup((*pcxt).toc, SHM_TOC_SHARED_KEY, false).cast();
        let worker_slots = (*parallel_shared).state.worker_slot.load(Ordering::Relaxed);

        let mut leader_local_fileset: SharedFileSetComplete = *shared_fileset_ptr_complete;
        let mut all_segments = Vec::new();
        for slot in 0..worker_slots {
            let name = spill_file_name(slot);
            let file = pg_sys::BufFileOpenFileSet(
                &raw mut leader_local_fileset.fs,
                name.as_ptr().cast::<c_char>(),
                0,
                true,
            );
            if file.is_null() {
                continue;
            }
            let worker_segments = collect_segments_from_spill_file(file);
            all_segments.extend(worker_segments);
            pg_sys::BufFileClose(file);
            pg_sys::BufFileDeleteFileSet(
                &raw mut leader_local_fileset.fs,
                name.as_ptr().cast::<c_char>(),
                true,
            );
        }

        let mut root = match crate::storage::pgbuffer::BlockBuffer::aquire_mut(index_rel, 0) {
            Ok(root) => root,
            Err(e) => {
                error!("failed to acquire root buffer: {e:#?}");
            }
        };
        let rbl = root
            .as_struct_mut::<crate::storage::RootBlockList>(0)
            .expect("root header");
        if let Err(e) = crate::storage::segment_list_append(index_rel, rbl, &all_segments) {
            error!("failed to append segments: {e:#?}");
        }

        const MAX_ACTIVE_SEGMENTS: u32 = 512;
        const COMPACT_TARGET_SEGMENTS: usize = 64;
        if rbl.num_segments > MAX_ACTIVE_SEGMENTS {
            let existing = crate::storage::segment_list_read(index_rel, rbl)
                .unwrap_or_else(|e| error!("failed to read segment list: {e:#?}"));
            let merged = crate::storage::merge(
                index_rel,
                &existing,
                COMPACT_TARGET_SEGMENTS,
                flush_threshold.saturating_mul(16).max(1024 * 1024),
                &crate::storage::tombstone::Snapshot::default(),
            )
            .unwrap_or_else(|e| error!("failed to compact segments: {e:#?}"));
            crate::storage::segment_list_rewrite(index_rel, rbl, &merged)
                .unwrap_or_else(|e| error!("failed to rewrite segment list: {e:#?}"));
        }

        drop(root);
        crate::storage::pending::free_blocks(index_rel, &blocks)
            .unwrap_or_else(|e| error!("failed to free pending blocks: {e:#?}"));

        let ntuples = (*parallel_shared).state.ntuples.load(Ordering::Relaxed) as u64;
        pg_sys::SharedFileSetDeleteAll(shared_fileset_ptr_raw.cast::<pg_sys::SharedFileSet>());
        pg_sys::UnregisterSnapshot(snapshot);
        cleanup_parallel_context(pcxt);
        pg_sys::relation_close(index_rel, pg_sys::RowExclusiveLock as i32);

        Some(ntuples)
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn _pg_zoekt_seal_main(
    seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    unsafe {
        let parallel_shared: *mut ParallelSealShared =
            pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_SHARED_KEY, false).cast();
        let shared_fileset_ptr = pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_FILESET_KEY, false)
            .cast::<pg_sys::SharedFileSet>();
        let blocks_ptr = pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_BLOCKS_KEY, false).cast();
        let snapshot_ptr =
            pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_SNAPSHOT_KEY, false).cast::<c_char>();
        let params = (*parallel_shared).params;

        pg_sys::SharedFileSetAttach(shared_fileset_ptr, seg);

        let mut local_fileset: SharedFileSetComplete =
            *(shared_fileset_ptr.cast::<SharedFileSetComplete>());
        local_fileset.segment = seg;

        let heaprel = pg_sys::table_open(params.heaprelid, pg_sys::AccessShareLock as i32);
        let indexrel = pg_sys::index_open(params.indexrelid, pg_sys::RowExclusiveLock as i32);
        let index_info = pg_sys::BuildIndexInfo(indexrel);

        let slot = pg_sys::table_slot_create(heaprel, std::ptr::null_mut());
        let estate = pg_sys::CreateExecutorState();
        let key_count = (*index_info).ii_NumIndexAttrs as usize;
        let mut values = vec![pg_sys::Datum::null(); key_count];
        let mut isnull = vec![true; key_count];

        let slot_id = (*parallel_shared)
            .state
            .worker_slot
            .fetch_add(1, Ordering::Acquire);
        let file_name = spill_file_name(slot_id);
        let spill_file = pg_sys::BufFileCreateFileSet(
            &raw mut local_fileset.fs,
            file_name.as_ptr().cast::<c_char>(),
        );
        if spill_file.is_null() {
            error!("failed to create seal spill file");
        }

        let mut state = SealSpillState::new(indexrel, spill_file, local_fileset);
        let spill_indexrel = state.index_relation;
        let spill_file_ptr = state.file;
        let blocks = blocks_ptr as *const u32;
        let mut seen = 0u64;
        let snapshot = pg_sys::RegisterSnapshot(pg_sys::RestoreSnapshot(snapshot_ptr));
        pg_sys::PushActiveSnapshot(snapshot);

        loop {
            let idx = (*parallel_shared)
                .state
                .next_block
                .fetch_add(1, Ordering::Acquire);
            if idx >= params.pending_blocks_len {
                break;
            }
            let block = *blocks.add(idx);
            let _ = crate::storage::pending::drain_block_entries(indexrel, block, |tid| {
                let mut item = pg_sys::ItemPointerData {
                    ip_blkid: pg_sys::BlockIdData {
                        bi_hi: (tid.block_number >> 16) as u16,
                        bi_lo: (tid.block_number & 0xffff) as u16,
                    },
                    ip_posid: tid.offset,
                };
                let tid_ptr = &mut item as *mut _ as pg_sys::ItemPointer;
                let fetched =
                    pg_sys::table_tuple_fetch_row_version(heaprel, tid_ptr, snapshot, slot);
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
                    &mut state.collector,
                    |collector| {
                        if collector.memory_usage() >= params.flush_threshold {
                            flush_collector_to_spill(spill_indexrel, spill_file_ptr, collector);
                        }
                    },
                ) {
                    seen = seen.saturating_add(1);
                }
                if state.collector.memory_usage() >= params.flush_threshold {
                    state.flush();
                }
                pg_sys::ExecClearTuple(slot);
            });
        }

        pg_sys::PopActiveSnapshot();
        pg_sys::UnregisterSnapshot(snapshot);
        state.flush();

        pg_sys::BufFileExportFileSet(spill_file);
        pg_sys::BufFileClose(spill_file);

        (*parallel_shared)
            .state
            .ntuples
            .fetch_add(seen as usize, Ordering::Release);
        pg_sys::ExecDropSingleTupleTableSlot(slot);
        pg_sys::FreeExecutorState(estate);
        pg_sys::index_close(indexrel, pg_sys::RowExclusiveLock as i32);
        pg_sys::table_close(heaprel, pg_sys::AccessShareLock as i32);
    }
}

fn spill_file_name(slot: usize) -> [u8; 64] {
    let mut file_name = [0u8; 64];
    let name_str = format!("pg_zoekt_seal_{slot}");
    if name_str.len() >= file_name.len() {
        error!("spill file name too long");
    }
    file_name[..name_str.len()].copy_from_slice(name_str.as_bytes());
    file_name[name_str.len()] = 0;
    file_name
}

struct SealSpillState {
    collector: crate::trgm::Collector,
    index_relation: pg_sys::Relation,
    file: *mut pg_sys::BufFile,
    _fileset: SharedFileSetComplete,
}

impl SealSpillState {
    fn new(
        index_relation: pg_sys::Relation,
        file: *mut pg_sys::BufFile,
        fileset: SharedFileSetComplete,
    ) -> Self {
        Self {
            collector: crate::trgm::Collector::new(),
            index_relation,
            file,
            _fileset: fileset,
        }
    }

    fn flush(&mut self) {
        flush_collector_to_spill(self.index_relation, self.file, &mut self.collector);
    }
}

fn flush_collector_to_spill(
    index_relation: pg_sys::Relation,
    file: *mut pg_sys::BufFile,
    collector: &mut crate::trgm::Collector,
) {
    let trgms = collector.take_trgms();
    if trgms.is_empty() {
        return;
    }
    let segments = crate::storage::encode::Encoder::encode_trgms(index_relation, &trgms)
        .unwrap_or_else(|e| error!("failed to encode segments: {e:#?}"));
    write_segment_batch(file, &segments);
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
            error!("short read in seal spill file");
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
