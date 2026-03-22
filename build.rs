use pgrx_pg_config::Pgrx;

fn main() {
    println!("cargo:rerun-if-changed=src/regex_ffi_shim.c");

    let label = if std::env::var_os("CARGO_FEATURE_PG18").is_some() {
        "pg18"
    } else if std::env::var_os("CARGO_FEATURE_PG17").is_some() {
        "pg17"
    } else if std::env::var_os("CARGO_FEATURE_PG16").is_some() {
        "pg16"
    } else if std::env::var_os("CARGO_FEATURE_PG15").is_some() {
        "pg15"
    } else if std::env::var_os("CARGO_FEATURE_PG14").is_some() {
        "pg14"
    } else {
        "pg13"
    };

    let pgrx = Pgrx::from_config().expect("load pgrx config");
    let pg_config = pgrx
        .get(label)
        .expect("resolve pg_config for active feature");
    let includedir = pg_config
        .includedir_server()
        .expect("resolve PostgreSQL server include directory");

    cc::Build::new()
        .file("src/regex_ffi_shim.c")
        .include(includedir)
        .compile("pg_zoekt_regex_ffi");
}
