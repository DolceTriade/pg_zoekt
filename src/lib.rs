use pgrx::prelude::*;

mod am;
pub use am::pg_zoekt_handler;

::pgrx::pg_module_magic!(name, version);

#[pg_extern]
fn hello_pg_zoekt() -> &'static str {
    "Hello, pg_zoekt"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_zoekt() {
        assert_eq!("Hello, pg_zoekt", crate::hello_pg_zoekt());
    }

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
        vec![]
    }
}
