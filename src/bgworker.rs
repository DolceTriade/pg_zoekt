use std::ffi::CString;
use std::time::Duration;

use pgrx::PgTryBuilder;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi;

static BGWORKER_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
static BGWORKER_DATABASE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static BGWORKER_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static BGWORKER_MAX_INDEXES_PER_CYCLE: GucSetting<i32> = GucSetting::<i32>::new(1);
static BGWORKER_SEAL_PENDING_BYTES: GucSetting<i32> = GucSetting::<i32>::new(33_554_432);
static BGWORKER_MERGE_SEGMENT_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(16);
static BGWORKER_MODE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"seal_then_merge"));
static BGWORKER_LOG_VERBOSE: GucSetting<bool> = GucSetting::<bool>::new(false);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerMode {
    SealOnly,
    SealThenMerge,
    MergeOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkReason {
    SealBacklog,
    SegmentPressure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Candidate {
    index_oid: pg_sys::Oid,
    pending_bytes: u32,
    segment_count: i32,
    reason: WorkReason,
}

pub(crate) fn init() {
    register_gucs();
    maybe_register_worker();
}

fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pg_zoekt.bgworker_enabled",
        c"Enable the pg_zoekt background maintenance worker.",
        c"Controls whether the pg_zoekt background worker performs bounded seal/merge maintenance.",
        &BGWORKER_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_zoekt.bgworker_database",
        c"Database name for the pg_zoekt background worker.",
        c"The single database the pg_zoekt background worker connects to for maintenance in v1.",
        &BGWORKER_DATABASE,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_zoekt.bgworker_interval_ms",
        c"Wake interval for the pg_zoekt background worker.",
        c"How long the pg_zoekt background worker sleeps between maintenance cycles, in milliseconds.",
        &BGWORKER_INTERVAL_MS,
        1_000,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );
    GucRegistry::define_int_guc(
        c"pg_zoekt.bgworker_max_indexes_per_cycle",
        c"Maximum indexes serviced per background-worker cycle.",
        c"Caps how many pg_zoekt indexes the background worker will touch in a single wake cycle.",
        &BGWORKER_MAX_INDEXES_PER_CYCLE,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_zoekt.bgworker_seal_pending_bytes",
        c"Pending-byte threshold for background sealing.",
        c"Pending-list bytes required before the background worker attempts a seal pass.",
        &BGWORKER_SEAL_PENDING_BYTES,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );
    GucRegistry::define_int_guc(
        c"pg_zoekt.bgworker_merge_segment_threshold",
        c"Segment-count threshold for background merge.",
        c"Visible segment count required before the background worker considers a merge pass.",
        &BGWORKER_MERGE_SEGMENT_THRESHOLD,
        2,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_zoekt.bgworker_mode",
        c"Maintenance policy for the pg_zoekt background worker.",
        c"Allowed values are seal_only, seal_then_merge, and merge_only.",
        &BGWORKER_MODE,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_zoekt.bgworker_log_verbose",
        c"Enable verbose pg_zoekt background-worker logs.",
        c"Logs per-cycle background-worker decisions when enabled.",
        &BGWORKER_LOG_VERBOSE,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn maybe_register_worker() {
    unsafe {
        if !pg_sys::process_shared_preload_libraries_in_progress {
            return;
        }
    }

    BackgroundWorkerBuilder::new("pg_zoekt_maintainer")
        .set_library("pg_zoekt")
        .set_function("pg_zoekt_bgworker_main")
        .set_restart_time(Some(Duration::from_secs(10)))
        .enable_spi_access()
        .load();
}

fn current_mode() -> WorkerMode {
    let raw = BGWORKER_MODE
        .get()
        .and_then(|v| v.into_string().ok())
        .unwrap_or_else(|| "seal_then_merge".to_string());
    let mode = parse_mode_from_str(&raw);
    if mode == WorkerMode::SealThenMerge && raw != "seal_then_merge" {
        warning!(
            "invalid pg_zoekt.bgworker_mode '{}'; falling back to seal_then_merge",
            raw
        );
    }
    mode
}

fn parse_mode_from_str(value: &str) -> WorkerMode {
    match value {
        "seal_only" => WorkerMode::SealOnly,
        "seal_then_merge" => WorkerMode::SealThenMerge,
        "merge_only" => WorkerMode::MergeOnly,
        _ => WorkerMode::SealThenMerge,
    }
}

fn configured_database() -> Option<String> {
    BGWORKER_DATABASE
        .get()
        .and_then(|v| v.into_string().ok())
        .filter(|s| !s.trim().is_empty())
}

fn verbose_logging() -> bool {
    BGWORKER_LOG_VERBOSE.get()
}

fn select_candidates(
    candidates: &[Candidate],
    max_indexes_per_cycle: usize,
    mode: WorkerMode,
) -> Vec<Candidate> {
    let mut filtered: Vec<Candidate> = candidates
        .iter()
        .copied()
        .filter(|candidate| match mode {
            WorkerMode::SealOnly => matches!(candidate.reason, WorkReason::SealBacklog),
            WorkerMode::MergeOnly => matches!(candidate.reason, WorkReason::SegmentPressure),
            WorkerMode::SealThenMerge => true,
        })
        .collect();

    filtered.sort_by(|a, b| {
        let a_priority = match a.reason {
            WorkReason::SealBacklog => 0u8,
            WorkReason::SegmentPressure => 1u8,
        };
        let b_priority = match b.reason {
            WorkReason::SealBacklog => 0u8,
            WorkReason::SegmentPressure => 1u8,
        };
        a_priority
            .cmp(&b_priority)
            .then_with(|| b.pending_bytes.cmp(&a.pending_bytes))
            .then_with(|| b.segment_count.cmp(&a.segment_count))
            .then_with(|| u32::from(a.index_oid).cmp(&u32::from(b.index_oid)))
    });
    filtered.truncate(max_indexes_per_cycle);
    filtered
}

fn enumerate_candidates(
    seal_threshold: u32,
    merge_threshold: i32,
) -> Result<Vec<Candidate>, spi::Error> {
    let sql = "SELECT i.indexrelid::oid \
               FROM pg_catalog.pg_index i \
               JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid \
               JOIN pg_catalog.pg_am a ON a.oid = c.relam \
               WHERE a.amname = 'pg_zoekt' AND i.indisvalid AND i.indisready \
               ORDER BY i.indexrelid";

    let index_oids: Vec<pg_sys::Oid> =
        Spi::connect_mut(|client| -> spi::Result<Vec<pg_sys::Oid>> {
            let mut out = Vec::new();
            for row in client.select(sql, None, &[])? {
                if let Some(oid) = row.get::<pg_sys::Oid>(1)? {
                    out.push(oid);
                }
            }
            Ok(out)
        })?;

    let mut candidates = Vec::new();
    for oid in index_oids {
        let pending_bytes = match crate::am::bgworker_pending_bytes(oid) {
            Ok(bytes) => bytes,
            Err(e) => {
                warning!(
                    "bgworker skipped index {} while reading pending bytes: {e:#}",
                    oid
                );
                continue;
            }
        };
        let segment_count = crate::am::bgworker_segment_count(oid);
        let reason = if pending_bytes >= seal_threshold {
            Some(WorkReason::SealBacklog)
        } else if segment_count >= merge_threshold {
            Some(WorkReason::SegmentPressure)
        } else {
            None
        };

        if let Some(reason) = reason {
            candidates.push(Candidate {
                index_oid: oid,
                pending_bytes,
                segment_count,
                reason,
            });
        }
    }
    Ok(candidates)
}

fn run_worker_cycle() {
    let seal_threshold = BGWORKER_SEAL_PENDING_BYTES.get().max(1) as u32;
    let merge_threshold = BGWORKER_MERGE_SEGMENT_THRESHOLD.get().max(2);
    let max_indexes = BGWORKER_MAX_INDEXES_PER_CYCLE.get().max(1) as usize;
    let mode = current_mode();

    let candidates = match enumerate_candidates(seal_threshold, merge_threshold) {
        Ok(candidates) => candidates,
        Err(e) => {
            warning!("pg_zoekt bgworker failed to enumerate indexes: {e:#}");
            return;
        }
    };

    if verbose_logging() {
        info!(
            "pg_zoekt bgworker cycle: candidates={} mode={:?} seal_threshold={} merge_threshold={} max_indexes={}",
            candidates.len(),
            mode,
            seal_threshold,
            merge_threshold,
            max_indexes
        );
    }

    for candidate in select_candidates(&candidates, max_indexes, mode) {
        let start = std::time::Instant::now();
        let result = match candidate.reason {
            WorkReason::SealBacklog => {
                PgTryBuilder::new(|| crate::am::bgworker_try_seal(candidate.index_oid))
                    .catch_others(|e| {
                        warning!(
                            "pg_zoekt bgworker seal failed for index {}: {e:?}",
                            candidate.index_oid
                        );
                        Err(anyhow::anyhow!("bgworker seal failed"))
                    })
                    .execute()
            }
            WorkReason::SegmentPressure => {
                PgTryBuilder::new(|| crate::am::bgworker_try_merge(candidate.index_oid))
                    .catch_others(|e| {
                        warning!(
                            "pg_zoekt bgworker merge failed for index {}: {e:?}",
                            candidate.index_oid
                        );
                        Err(anyhow::anyhow!("bgworker merge failed"))
                    })
                    .execute()
            }
        };

        match result {
            Ok(summary) => {
                let action = match candidate.reason {
                    WorkReason::SealBacklog => "seal",
                    WorkReason::SegmentPressure => "merge",
                };
                if summary.skipped_busy {
                    info!(
                        "pg_zoekt bgworker {} skipped_busy: index_oid={} pending_bytes={} segment_count={} elapsed_ms={}",
                        action,
                        candidate.index_oid,
                        candidate.pending_bytes,
                        candidate.segment_count,
                        start.elapsed().as_millis()
                    );
                } else {
                    info!(
                        "pg_zoekt bgworker {}: index_oid={} pending_bytes={} segment_count={} sealed_tuples={} segments_before={} segments_after={} elapsed_ms={}",
                        action,
                        candidate.index_oid,
                        candidate.pending_bytes,
                        candidate.segment_count,
                        summary.sealed_tuples,
                        summary.segments_before,
                        summary.segments_after,
                        start.elapsed().as_millis()
                    );
                }
            }
            Err(e) => {
                warning!(
                    "pg_zoekt bgworker failed for index {}: {e:#}",
                    candidate.index_oid
                );
            }
        }
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_zoekt_bgworker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let mut connected_db: Option<String> = None;

    loop {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        if BGWORKER_ENABLED.get() {
            let target_db = configured_database();
            if connected_db.is_none() {
                if let Some(target_db) = target_db {
                    if CString::new(target_db.clone()).is_err() {
                        warning!(
                            "pg_zoekt bgworker database name contains an interior NUL; skipping"
                        );
                        continue;
                    }
                    let connect_result = PgTryBuilder::new(|| {
                        BackgroundWorker::connect_worker_to_spi(Some(&target_db), None);
                        true
                    })
                    .catch_others(|e| {
                        warning!(
                            "pg_zoekt bgworker failed to connect to database '{}': {e:?}",
                            target_db
                        );
                        false
                    })
                    .execute();
                    if connect_result {
                        connected_db = Some(target_db.clone());
                        info!("pg_zoekt bgworker connected to database '{}'", target_db);
                    }
                } else if verbose_logging() {
                    info!("pg_zoekt bgworker idle: no database configured");
                }
            } else {
                if let Some(target_db) = target_db.as_ref() {
                    if connected_db.as_ref() != Some(target_db) {
                        warning!(
                            "pg_zoekt bgworker database changed from '{}' to '{}' after connection; restart PostgreSQL to move the worker",
                            connected_db.as_deref().unwrap_or(""),
                            target_db
                        );
                    }
                }
                if connected_db.is_some() {
                    BackgroundWorker::transaction(run_worker_cycle);
                }
            }
        } else if verbose_logging() {
            info!("pg_zoekt bgworker idle: disabled");
        }

        let interval_ms = BGWORKER_INTERVAL_MS.get().max(1_000) as u64;
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(interval_ms))) {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_candidates_prefers_seal_backlog_then_volume() {
        let candidates = vec![
            Candidate {
                index_oid: 3.into(),
                pending_bytes: 1,
                segment_count: 30,
                reason: WorkReason::SegmentPressure,
            },
            Candidate {
                index_oid: 2.into(),
                pending_bytes: 500,
                segment_count: 2,
                reason: WorkReason::SealBacklog,
            },
            Candidate {
                index_oid: 1.into(),
                pending_bytes: 100,
                segment_count: 20,
                reason: WorkReason::SealBacklog,
            },
        ];

        let selected = select_candidates(&candidates, 3, WorkerMode::SealThenMerge);
        assert_eq!(selected[0].index_oid, 2.into());
        assert_eq!(selected[1].index_oid, 1.into());
        assert_eq!(selected[2].index_oid, 3.into());
    }

    #[test]
    fn select_candidates_respects_mode_and_limit() {
        let candidates = vec![
            Candidate {
                index_oid: 11.into(),
                pending_bytes: 100,
                segment_count: 5,
                reason: WorkReason::SealBacklog,
            },
            Candidate {
                index_oid: 12.into(),
                pending_bytes: 0,
                segment_count: 25,
                reason: WorkReason::SegmentPressure,
            },
        ];

        let selected = select_candidates(&candidates, 1, WorkerMode::MergeOnly);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].index_oid, 12.into());
    }

    #[test]
    fn mode_parser_falls_back_to_default() {
        assert_eq!(parse_mode_from_str("seal_only"), WorkerMode::SealOnly);
        assert_eq!(parse_mode_from_str("merge_only"), WorkerMode::MergeOnly);
        assert_eq!(
            parse_mode_from_str("totally_invalid"),
            WorkerMode::SealThenMerge
        );
    }
}
