use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

use pgrx::ffi::c_char;
use pgrx::pg_sys::{self, Oid};
use pgrx::prelude::*;

use crate::storage::{Segment, tombstone};

const PARALLEL_MERGE_MAIN: *const c_char = c"_pg_zoekt_merge_main".as_ptr();
const EXTENSION_NAME: &[u8] = b"pg_zoekt\0";

const SHM_TOC_SHARED_KEY: u64 = 0x5A4B540000000201;
const SHM_TOC_FILESET_KEY: u64 = 0x5A4B540000000202;
const MERGE_INPUT_FILE_NAME: &str = "pg_zoekt_merge_input";

#[cfg(feature = "pg_test")]
static PARALLEL_MERGE_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct ParallelMergeParams {
    indexrelid: Oid,
    group_count: usize,
    flush_threshold: usize,
    segments_len: usize,
    offsets_len: usize,
    tombstones_empty: bool,
}

#[repr(C)]
struct ParallelMergeState {
    worker_slot: AtomicUsize,
    next_group: AtomicUsize,
}

#[repr(C)]
struct ParallelMergeShared {
    params: ParallelMergeParams,
    state: ParallelMergeState,
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

pub(crate) unsafe fn merge_parallel(
    rel: pg_sys::Relation,
    segments: &[Segment],
    offsets: &[u32],
    flush_threshold: usize,
    tombstones_empty: bool,
    workers: usize,
) -> Option<Vec<Segment>> {
    unsafe {
        if workers == 0 || segments.is_empty() {
            return None;
        }

        let group_count = offsets.len().saturating_sub(1);
        let leader_pid = pg_sys::MyProcPid;
        info!(
            "merge_parallel leader: pid={} workers={} group_count={} tombstones_empty={}",
            leader_pid, workers, group_count, tombstones_empty
        );
        if group_count == 0 {
            return Some(Vec::new());
        }

        let index_oid = (*rel).rd_id;

        pg_sys::EnterParallelMode();
        let pcxt = pg_sys::CreateParallelContext(
            EXTENSION_NAME.as_ptr().cast(),
            PARALLEL_MERGE_MAIN,
            workers as i32,
        );

        let params = ParallelMergeParams {
            indexrelid: index_oid,
            group_count,
            flush_threshold,
            segments_len: segments.len(),
            offsets_len: offsets.len(),
            tombstones_empty,
        };

        toc_estimate_single_chunk(pcxt, size_of::<ParallelMergeShared>());
        let shared_fileset_size = size_of::<SharedFileSetComplete>();
        toc_estimate_single_chunk(pcxt, shared_fileset_size);

        pg_sys::InitializeParallelDSM(pcxt);
        if (*pcxt).seg.is_null() {
            let est = (*pcxt).estimator;
            warning!(
                "merge_parallel leader: pid={} dsm_init_failed segments_len={} offsets_len={} group_count={} workers={} est_space={} est_keys={}",
                leader_pid,
                segments.len(),
                offsets.len(),
                group_count,
                (*pcxt).nworkers,
                est.space_for_chunks,
                est.number_of_keys
            );
            pg_sys::InitializeParallelDSM(pcxt);
            if (*pcxt).seg.is_null() {
                cleanup_parallel_context(pcxt);
                return None;
            }
            info!(
                "merge_parallel leader: pid={} dsm_retry_succeeded",
                leader_pid
            );
        }

        let shared = pg_sys::shm_toc_allocate((*pcxt).toc, size_of::<ParallelMergeShared>())
            .cast::<ParallelMergeShared>();
        if shared.is_null() {
            pgrx::error!("failed to allocate ParallelMergeShared");
        }
        shared.write(ParallelMergeShared {
            params,
            state: ParallelMergeState {
                worker_slot: AtomicUsize::new(0),
                next_group: AtomicUsize::new(0),
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

        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_SHARED_KEY, shared.cast());
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_FILESET_KEY,
            shared_fileset_ptr_raw.cast(),
        );
        write_merge_input(
            &raw mut (*shared_fileset_ptr_complete).fs,
            segments,
            offsets,
        );

        pg_sys::LaunchParallelWorkers(pcxt);
        info!(
            "merge_parallel leader: pid={} launched_workers={}",
            leader_pid,
            (*pcxt).nworkers_launched
        );
        if (*pcxt).nworkers_launched == 0 {
            warning!(
                "merge_parallel leader: pid={} requested_workers={} launched_workers=0",
                leader_pid,
                workers
            );
            cleanup_parallel_context(pcxt);
            return None;
        }
        #[cfg(feature = "pg_test")]
        PARALLEL_MERGE_COUNT.fetch_add(1, Ordering::Relaxed);
        pg_sys::WaitForParallelWorkersToAttach(pcxt);
        pg_sys::WaitForParallelWorkersToFinish(pcxt);

        let parallel_shared: *mut ParallelMergeShared =
            pg_sys::shm_toc_lookup((*pcxt).toc, SHM_TOC_SHARED_KEY, false).cast();
        let worker_slots = (*parallel_shared).state.worker_slot.load(Ordering::Relaxed);
        info!(
            "merge_parallel leader: pid={} worker_slots={} group_count={}",
            leader_pid, worker_slots, group_count
        );

        let mut leader_local_fileset: SharedFileSetComplete = *shared_fileset_ptr_complete;
        let mut interim = Vec::new();
        let mut interim_total = 0usize;
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
            interim_total = interim_total.saturating_add(worker_segments.len());
            interim.extend(worker_segments);
            pg_sys::BufFileClose(file);
            pg_sys::BufFileDeleteFileSet(
                &raw mut leader_local_fileset.fs,
                name.as_ptr().cast::<c_char>(),
                true,
            );
        }
        info!(
            "merge_parallel leader: pid={} collected_interim_segments={} workers={}",
            leader_pid, interim_total, worker_slots
        );

        pg_sys::SharedFileSetDeleteAll(shared_fileset_ptr_raw.cast::<pg_sys::SharedFileSet>());
        cleanup_parallel_context(pcxt);

        Some(interim)
    }
}

#[cfg(feature = "pg_test")]
pub(crate) fn test_parallel_merge_reset() {
    PARALLEL_MERGE_COUNT.store(0, Ordering::Relaxed);
}

#[cfg(feature = "pg_test")]
pub(crate) fn test_parallel_merge_count() -> usize {
    PARALLEL_MERGE_COUNT.load(Ordering::Relaxed)
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn _pg_zoekt_merge_main(
    seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    unsafe {
        let shared: *mut ParallelMergeShared =
            pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_SHARED_KEY, false).cast();
        let shared_fileset_ptr = pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_FILESET_KEY, false)
            .cast::<pg_sys::SharedFileSet>();
        let params = (*shared).params;
        let worker_pid = pg_sys::MyProcPid;
        info!(
            "merge_parallel worker enter: pid={} groups={} segments_len={}",
            worker_pid, params.group_count, params.segments_len
        );

        pg_sys::SharedFileSetAttach(shared_fileset_ptr, seg);
        let mut local_fileset: SharedFileSetComplete =
            *(shared_fileset_ptr.cast::<SharedFileSetComplete>());
        local_fileset.segment = seg;

        let indexrel = pg_sys::index_open(params.indexrelid, pg_sys::RowExclusiveLock as i32);
        let tombstones = if params.tombstones_empty {
            tombstone::Snapshot::default()
        } else {
            tombstone::load_snapshot(indexrel).unwrap_or_else(|e| {
                warning!("failed to load tombstones during merge: {e:#?}");
                tombstone::Snapshot::default()
            })
        };

        let slot_id = (*shared).state.worker_slot.fetch_add(1, Ordering::Acquire);
        info!(
            "merge_parallel worker start: pid={} slot={}",
            worker_pid, slot_id
        );
        let file_name = spill_file_name(slot_id);
        let spill_file = pg_sys::BufFileCreateFileSet(
            &raw mut local_fileset.fs,
            file_name.as_ptr().cast::<c_char>(),
        );
        if spill_file.is_null() {
            error!("failed to create merge spill file");
        }

        let (segments, offsets) = read_merge_input(&raw mut local_fileset.fs);

        let mut local_groups = 0usize;
        let mut local_segments = 0usize;
        loop {
            let group_idx = (*shared).state.next_group.fetch_add(1, Ordering::Acquire);
            if group_idx >= params.group_count {
                break;
            }
            pg_sys::check_for_interrupts!();
            let start = offsets.get(group_idx as usize).copied().unwrap_or(0) as usize;
            let end = offsets
                .get(group_idx as usize + 1)
                .copied()
                .unwrap_or(start as u32) as usize;
            if start >= end {
                continue;
            }
            let slice = &segments[start..end];
            let merged =
                crate::storage::merge(indexrel, slice, params.flush_threshold, &tombstones)
                    .unwrap_or_else(|e| error!("failed to merge group segments: {e:#?}"));
            pg_sys::check_for_interrupts!();
            write_segment_batch(spill_file, std::slice::from_ref(&merged));
            local_groups = local_groups.saturating_add(1);
            local_segments = local_segments.saturating_add(1);
        }
        info!(
            "merge_parallel worker: pid={} slot={} groups={} merged_segments={}",
            worker_pid, slot_id, local_groups, local_segments
        );

        pg_sys::BufFileExportFileSet(spill_file);
        pg_sys::BufFileClose(spill_file);
        pg_sys::index_close(indexrel, pg_sys::RowExclusiveLock as i32);
    }
}

fn spill_file_name(slot: usize) -> [u8; 64] {
    let mut file_name = [0u8; 64];
    let name_str = format!("pg_zoekt_merge_{slot}");
    if name_str.len() >= file_name.len() {
        error!("spill file name too long");
    }
    file_name[..name_str.len()].copy_from_slice(name_str.as_bytes());
    file_name[name_str.len()] = 0;
    file_name
}

fn merge_input_file_name() -> [u8; 64] {
    let mut name = [0u8; 64];
    let name_str = MERGE_INPUT_FILE_NAME;
    if name_str.len() >= name.len() {
        error!("merge input file name too long");
    }
    name[..name_str.len()].copy_from_slice(name_str.as_bytes());
    name[name_str.len()] = 0;
    name
}

fn write_bytes(file: *mut pg_sys::BufFile, bytes: &[u8]) {
    unsafe {
        pg_sys::BufFileWrite(file, bytes.as_ptr().cast(), bytes.len());
    }
}

fn read_bytes(file: *mut pg_sys::BufFile, bytes: &mut [u8]) {
    let read = unsafe { pg_sys::BufFileRead(file, bytes.as_mut_ptr().cast(), bytes.len()) };
    if read != bytes.len() {
        error!("short read in merge input file");
    }
}

fn write_u32(file: *mut pg_sys::BufFile, v: u32) {
    write_bytes(file, &v.to_le_bytes());
}

fn write_u64(file: *mut pg_sys::BufFile, v: u64) {
    write_bytes(file, &v.to_le_bytes());
}

fn read_u32(file: *mut pg_sys::BufFile) -> u32 {
    let mut buf = [0u8; 4];
    read_bytes(file, &mut buf);
    u32::from_le_bytes(buf)
}

fn read_u64(file: *mut pg_sys::BufFile) -> u64 {
    let mut buf = [0u8; 8];
    read_bytes(file, &mut buf);
    u64::from_le_bytes(buf)
}

fn write_merge_input(fileset: *mut pg_sys::FileSet, segments: &[Segment], offsets: &[u32]) {
    let name = merge_input_file_name();
    let file = unsafe { pg_sys::BufFileCreateFileSet(fileset, name.as_ptr().cast::<c_char>()) };
    if file.is_null() {
        error!("failed to create merge input file");
    }
    write_u32(file, segments.len() as u32);
    write_u32(file, offsets.len() as u32);
    for seg in segments {
        write_u32(file, seg.block);
        write_u64(file, seg.size);
        write_u32(file, seg.extent_head);
        write_u32(file, seg.extent_count);
    }
    for offset in offsets {
        write_u32(file, *offset);
    }
    unsafe {
        pg_sys::BufFileClose(file);
    }
}

fn read_merge_input(fileset: *mut pg_sys::FileSet) -> (Vec<Segment>, Vec<u32>) {
    let name = merge_input_file_name();
    let file =
        unsafe { pg_sys::BufFileOpenFileSet(fileset, name.as_ptr().cast::<c_char>(), 0, false) };
    if file.is_null() {
        error!("failed to open merge input file");
    }
    let segments_len = read_u32(file) as usize;
    let offsets_len = read_u32(file) as usize;
    let mut segments = Vec::with_capacity(segments_len);
    for _ in 0..segments_len {
        let block = read_u32(file);
        let size = read_u64(file);
        let extent_head = read_u32(file);
        let extent_count = read_u32(file);
        segments.push(Segment {
            block,
            size,
            extent_head,
            extent_count,
        });
    }
    let mut offsets = Vec::with_capacity(offsets_len);
    for _ in 0..offsets_len {
        offsets.push(read_u32(file));
    }
    unsafe {
        pg_sys::BufFileClose(file);
    }
    (segments, offsets)
}

fn write_segment_batch(file: *mut pg_sys::BufFile, segments: &[Segment]) {
    write_u32(file, segments.len() as u32);
    for seg in segments {
        write_u32(file, seg.block);
        write_u64(file, seg.size);
        write_u32(file, seg.extent_head);
        write_u32(file, seg.extent_count);
    }
}

fn collect_segments_from_spill_file(file: *mut pg_sys::BufFile) -> Vec<Segment> {
    fn read_exact_or_eof(file: *mut pg_sys::BufFile, buf: &mut [u8]) -> Option<()> {
        let read =
            unsafe { pg_sys::BufFileReadMaybeEOF(file, buf.as_mut_ptr().cast(), buf.len(), true) };
        if read == 0 {
            return None;
        }
        if read != buf.len() {
            error!("short read in merge spill file");
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
            let extent_head =
                read_u32(file).unwrap_or_else(|| error!("unexpected eof (extent head)"));
            let extent_count =
                read_u32(file).unwrap_or_else(|| error!("unexpected eof (extent count)"));
            segments.push(Segment {
                block,
                size,
                extent_head,
                extent_count,
            });
        }
    }

    segments
}
