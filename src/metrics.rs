#[cfg(feature = "pg_test")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering};

    static INSERTS: AtomicU64 = AtomicU64::new(0);
    static TRIGRAMS: AtomicU64 = AtomicU64::new(0);
    static POSITIONS: AtomicU64 = AtomicU64::new(0);
    static MAX_POSITIONS_PER_TRIGRAM: AtomicU64 = AtomicU64::new(0);
    static WAL_RECORDS: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Clone, Copy)]
    pub struct Snapshot {
        pub inserts: u64,
        pub trigrams: u64,
        pub positions: u64,
        pub max_positions_per_trigram: u64,
        pub wal_records: u64,
    }

    pub fn reset() {
        INSERTS.store(0, Ordering::Relaxed);
        TRIGRAMS.store(0, Ordering::Relaxed);
        POSITIONS.store(0, Ordering::Relaxed);
        MAX_POSITIONS_PER_TRIGRAM.store(0, Ordering::Relaxed);
        WAL_RECORDS.store(0, Ordering::Relaxed);
    }

    pub fn snapshot() -> Snapshot {
        Snapshot {
            inserts: INSERTS.load(Ordering::Relaxed),
            trigrams: TRIGRAMS.load(Ordering::Relaxed),
            positions: POSITIONS.load(Ordering::Relaxed),
            max_positions_per_trigram: MAX_POSITIONS_PER_TRIGRAM.load(Ordering::Relaxed),
            wal_records: WAL_RECORDS.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "pg_test")]
pub use imp::*;
