use pgrx::pg_guard;

mod am;
mod bgworker;
mod build;
mod context;
mod introspect;
mod metrics;
mod operators;
mod query;
mod regex_ffi;
mod regex_plan;
mod seal;
mod storage;
mod trgm;

::pgrx::pg_module_magic!(name, version);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    storage::pgbuffer::init();
    bgworker::init();
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        if std::env::var("CI").ok().is_some() {
            return vec![
                // Raise the test cluster buffer pool so we can distinguish a real
                // merge correctness bug from buffer-pin exhaustion under pgrx tests.
                "shared_buffers = '1GB'",
                // Ensure parallel index build tests actually have worker capacity.
                "max_worker_processes = 8",
                "max_parallel_workers = 8",
                "max_parallel_maintenance_workers = 4",
            ];
        }
        vec![]
    }
}
