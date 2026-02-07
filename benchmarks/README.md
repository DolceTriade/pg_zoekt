# pg_zoekt Benchmarks

This harness builds a deterministic "real-world" corpus (a curated list of everyday sentences with keywords for the LIKE/ILIKE/REGEX/IREGEX paths) and measures how quickly `pg_zoekt` answers those queries when 70% of the rows are toasted.

## Requirements

1. A Postgres instance with `pg_zoekt` installed (for example, `cargo pgrx run` will give you one).
2. Connection info via `DATABASE_URL` or the standard `PGHOST`/`PGPORT`/`PGUSER`/`PGDATABASE`/`PGPASSWORD` environment variables.

## Running the Benchmark

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- --rows 200000 --report report.md
```

- `--rows N` controls how many rows are generated (default 200_000).
- `--toast-compression {default|pglz|zstd|lz4}` controls `bench_realworld.doc` TOAST compression (default `default`).
- The first 70% of rows are made long enough to trigger TOAST storage; the remainder stay inline.
- The corpus reuses a fixed sentence list so every run is deterministic.
- The CLI builds the `bench_realworld` table, creates a `pg_zoekt` index, seals it, and runs the four predicate benchmarks (LIKE/ILIKE/REGEX/IREGEX) using `EXPLAIN (ANALYZE TRUE, FORMAT TEXT)`.
- By default the Markdown report is emitted to stdout. Use `--report path` to save it to a file.

## Report Contents

1. A header with the generation timestamp and connection summary.
2. A totals table with rows, toasted vs inline distribution, and per-query runtime.
3. Detailed sections per query that include the plan text so you can confirm the index is being used and check the row counts/loops alongside the actual match counts.
4. The query mix exercises a singleton match (`LIKE`), a broad match covering ~90% of the corpus (`ILIKE`), and regex/iregex filters that hit smaller, predictable subsets; this exposes the I/O cost difference between 1 vs. 90k rows.
5. The header now also reports how long building and sealing the `pg_zoekt` index took so you can see the full ingestion cost before query timings.

Ensure you seal or rebuild the index outside of the benchmark if you want to evaluate different index states.
