use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use postgres::{Client, Config, NoTls};
use regex::Regex;
use std::{env, fs, io::Write, path::PathBuf, time::Instant};

const CORPUS: &[&str] = &[
    "City council members discussed ferry safety and market zoning.",
    "The harbor master noted the cool breeze over the sleeping boats.",
    "Librarians counted the repaired books before summer reading.",
    "Coffee roasters in the old factory remembered the first roast.",
    "Farmers traded stories about the new irrigation canal.",
    "Railway workers watched the rain-slicked rails near Canyon Summit.",
    "Bakers mixed cinnamon and cardamom for the morning market.",
    "The neighborhood choir rehearsed near the 7th Street station.",
    "Cyclists praised the tunnels under the university campus.",
    "Artists painted murals of snow-covered piers and bright flags.",
    "Engineers tuned the radar that kept the ferry schedule steady.",
    "A courier delivered blueprints for the new art museum in 2025.",
];

const UNIQUE_LIKE_TOKEN: &str = "unique-like-match-2026";
const COMMON_LIKE_TOKEN: &str = "COMMON-LIKE-TOKEN-2026";
const REGEX_MATCH_PREFIX: &str = "regex-match-zone";
const IREGEX_MATCH_PREFIX: &str = "iregex-match-zone";

struct QuerySpec {
    name: &'static str,
    where_clause: &'static str,
    match_target: &'static str,
    not_match_target: &'static str,
}

const QUERY_SPECS: &[QuerySpec] = &[
    QuerySpec {
        name: "LIKE (singleton)",
        where_clause: "doc LIKE '%unique-like-match-2026%'",
        match_target: UNIQUE_LIKE_TOKEN,
        not_match_target: "no-unique-match-2026",
    },
    QuerySpec {
        name: "ILIKE (wide)",
        where_clause: "doc ILIKE '%common-like-token-2026%'",
        match_target: COMMON_LIKE_TOKEN,
        not_match_target: "NoMatchHere-2026",
    },
    QuerySpec {
        name: "REGEX (subset)",
        where_clause: "doc ~ 'regex-match-zone-[0-9]+'",
        match_target: "regex-match-zone-[0-9]+",
        not_match_target: "regex-match-zone-X",
    },
    QuerySpec {
        name: "IREGEX (sparse)",
        where_clause: "doc ~* 'iregex-match-zone-[0-9]+'",
        match_target: "iregex-match-zone-[0-9]+",
        not_match_target: "iregex-match-zone-Z",
    },
];

#[derive(Parser)]
#[command(author, version, about = "pg_zoekt real-world benchmark harness")]
struct Args {
    /// Number of rows to insert for the benchmark
    #[arg(long, short = 'n', default_value_t = 200_000)]
    rows: u64,

    /// Path to save the generated Markdown report. Defaults to stdout only.
    #[arg(long, short = 'o')]
    report: Option<PathBuf>,

    /// TOAST compression setting for bench_realworld.doc
    #[arg(long, value_enum, default_value_t = ToastCompression::Default)]
    toast_compression: ToastCompression,

    /// Fraction of rows to tombstone+delete before running reclaim maintenance.
    #[arg(long, default_value_t = 0.0)]
    delete_fraction: f64,

    /// Run reclaim maintenance after creating tombstones and deleting rows.
    #[arg(long, default_value_t = false)]
    run_reclaim: bool,
}

struct ConnectionMetadata {
    summary: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ToastCompression {
    Default,
    Pglz,
    Zstd,
    Lz4,
}

impl ToastCompression {
    fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Pglz => "pglz",
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
        }
    }
}

struct QueryResult {
    name: String,
    rows: u64,
    loops: u64,
    runtime_ms: f64,
    match_count: u64,
    plan: String,
}

struct ReclaimResult {
    delete_fraction: f64,
    deleted_rows: u64,
    maintenance_ms: f64,
    index_bytes_before: i64,
    index_bytes_after: i64,
    reclaimed_bytes: i64,
    skipped_busy: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let (mut client, conn_meta) = connect_to_database()?;
    eprintln!(
        "Running benchmark with {} rows ({} toasted, {} inline)",
        args.rows,
        (args.rows * 70) / 100,
        args.rows - (args.rows * 70) / 100
    );
    eprintln!("TOAST compression: {}", args.toast_compression.label());
    let (report, index_duration_ms) = run_benchmark(&mut client, &args, &conn_meta)?;
    eprintln!("Index build + seal took {:.3} ms", index_duration_ms);
    println!("{}", report);
    if let Some(path) = &args.report {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &report)?;
    }
    Ok(())
}

fn connect_to_database() -> Result<(Client, ConnectionMetadata)> {
    if let Ok(url) = env::var("DATABASE_URL") {
        let client = Client::connect(&url, NoTls)
            .context("failed to connect using DATABASE_URL environment variable")?;
        let info = ConnectionMetadata {
            summary: "from DATABASE_URL".to_string(),
        };
        return Ok((client, info));
    }

    let host = env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PGPORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(5432);
    let user = env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
    let dbname = env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string());

    let mut config = Config::new();
    config.host(&host).port(port).user(&user).dbname(&dbname);
    if let Ok(password) = env::var("PGPASSWORD") {
        config.password(password);
    }

    let client = config
        .connect(NoTls)
        .context("failed to connect to Postgres with env config")?;

    let info = ConnectionMetadata {
        summary: format!("host={} port={} db={} user={}", host, port, dbname, user),
    };
    Ok((client, info))
}

fn run_benchmark(
    client: &mut Client,
    args: &Args,
    conn_meta: &ConnectionMetadata,
) -> Result<(String, f64)> {
    let toasted_rows = (args.rows * 70) / 100;
    let inline_rows = args.rows - toasted_rows;

    client.batch_execute(
        "CREATE EXTENSION IF NOT EXISTS pg_zoekt;
        DROP TABLE IF EXISTS bench_realworld;
        CREATE TABLE bench_realworld (id serial PRIMARY KEY, doc text NOT NULL);
    ",
    )?;
    if args.toast_compression != ToastCompression::Default {
        let sql = format!(
            "ALTER TABLE bench_realworld ALTER COLUMN doc SET COMPRESSION {};",
            args.toast_compression.label()
        );
        client
            .batch_execute(&sql)
            .with_context(|| format!("failed to set TOAST compression to {}", args.toast_compression.label()))?;
    }

    let mut tx = client.transaction()?;
    {
        let mut writer = tx.copy_in("COPY bench_realworld (doc) FROM STDIN")?;
        for idx in 0..args.rows {
            let base = CORPUS[(idx as usize) % CORPUS.len()];
            let doc = build_document(base, idx, args.rows, idx < toasted_rows);
            writer
                .write_all(doc.as_bytes())
                .context("writing row to COPY stream")?;
            writer.write_all(b"\n").context("terminating COPY row")?;
        }
        writer.finish()?;
    }
    tx.commit()?;

    let index_start = Instant::now();
    client.batch_execute(
        "DROP INDEX IF EXISTS idx_bench_realworld_zoekt;
        CREATE INDEX idx_bench_realworld_zoekt ON bench_realworld USING pg_zoekt (doc);
        SELECT pg_zoekt_seal('idx_bench_realworld_zoekt'::regclass);
    ",
    )?;
    let index_duration_ms = index_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "Index build and seal finished in {:.3} ms",
        index_duration_ms
    );

    let reclaim_result = if args.run_reclaim && args.delete_fraction > 0.0 {
        Some(run_reclaim_phase(client, args.rows, args.delete_fraction)?)
    } else {
        None
    };

    let mut query_results = Vec::new();
    for spec in QUERY_SPECS {
        eprintln!("Running {} query", spec.name);
        eprintln!("  WHERE clause: {}", spec.where_clause);
        eprintln!("  match target: {}", spec.match_target);
        eprintln!("  not match target: {}", spec.not_match_target);
        let explain_sql = format!(
            "EXPLAIN (ANALYZE TRUE, FORMAT TEXT) SELECT id FROM bench_realworld WHERE {}",
            spec.where_clause
        );
        let rows = client.query(&explain_sql, &[])?;
        let plan = rows
            .iter()
            .map(|row| row.get::<usize, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        eprintln!("SQL output:\n{}", plan);
        let runtime_ms = parse_runtime_ms(&plan).unwrap_or(0.0);
        let (rows_read, loops) = parse_row_loops(&plan).unwrap_or((0, 0));
        let count_sql = format!(
            "SELECT COUNT(*) FROM bench_realworld WHERE {}",
            spec.where_clause
        );
        let match_count: i64 = client.query_one(&count_sql, &[])?.get(0);
        eprintln!("  actual matches (COUNT): {}", match_count);
        query_results.push(QueryResult {
            name: spec.name.to_string(),
            rows: rows_read,
            loops,
            runtime_ms,
            match_count: match_count as u64,
            plan,
        });
    }

    let report = format_markdown_report(
        args.rows,
        toasted_rows,
        inline_rows,
        args.toast_compression,
        index_duration_ms,
        conn_meta,
        reclaim_result.as_ref(),
        &query_results,
    );
    eprintln!(
        "{}",
        format_stderr_summary(
            args.rows,
            toasted_rows,
            inline_rows,
            args.toast_compression,
            index_duration_ms,
            conn_meta,
            reclaim_result.as_ref(),
            &query_results
        )
    );
    Ok((report, index_duration_ms))
}

fn run_reclaim_phase(client: &mut Client, total_rows: u64, delete_fraction: f64) -> Result<ReclaimResult> {
    let bounded_fraction = delete_fraction.clamp(0.0, 1.0);
    let deleted_rows = ((total_rows as f64) * bounded_fraction).round() as u64;
    if deleted_rows == 0 {
        return Ok(ReclaimResult {
            delete_fraction: bounded_fraction,
            deleted_rows: 0,
            maintenance_ms: 0.0,
            index_bytes_before: 0,
            index_bytes_after: 0,
            reclaimed_bytes: 0,
            skipped_busy: false,
        });
    }

    let index_bytes_before: i64 = client
        .query_one("SELECT pg_relation_size('idx_bench_realworld_zoekt'::regclass)", &[])?
        .get(0);
    let delete_limit = deleted_rows as i64;
    let tombstone_sql = format!(
        "SELECT pg_zoekt_tombstone(
            'idx_bench_realworld_zoekt'::regclass,
            array(SELECT ctid FROM bench_realworld WHERE id <= {delete_limit})
        )"
    );
    client.batch_execute(&tombstone_sql)?;
    let delete_sql = format!("DELETE FROM bench_realworld WHERE id <= {delete_limit}");
    client.batch_execute(&delete_sql)?;

    let maintenance_start = Instant::now();
    let skipped_busy: bool = client
        .query_one(
            "SELECT skipped_busy
             FROM pg_zoekt_maintain('idx_bench_realworld_zoekt'::regclass, 'merge', true)",
            &[],
        )?
        .get(0);
    let maintenance_ms = maintenance_start.elapsed().as_secs_f64() * 1000.0;
    let index_bytes_after: i64 = client
        .query_one("SELECT pg_relation_size('idx_bench_realworld_zoekt'::regclass)", &[])?
        .get(0);
    let reclaimed_bytes = index_bytes_before.saturating_sub(index_bytes_after);

    Ok(ReclaimResult {
        delete_fraction: bounded_fraction,
        deleted_rows,
        maintenance_ms,
        index_bytes_before,
        index_bytes_after,
        reclaimed_bytes,
        skipped_busy,
    })
}

fn build_document(base: &str, idx: u64, total_rows: u64, make_long: bool) -> String {
    let mut doc = String::with_capacity(512);
    if make_long {
        for _ in 0..40 {
            doc.push_str(base);
            doc.push(' ');
        }
    } else {
        doc.push_str(base);
        doc.push(' ');
    }
    doc.push_str("row-");
    doc.push_str(&idx.to_string());
    doc.push(' ');
    if idx == total_rows.saturating_sub(1) {
        doc.push_str(UNIQUE_LIKE_TOKEN);
        doc.push(' ');
    }
    let common_threshold = total_rows.saturating_mul(90) / 100;
    if idx < common_threshold {
        doc.push_str(COMMON_LIKE_TOKEN);
        doc.push(' ');
    }
    if idx % 25 == 0 {
        doc.push_str(REGEX_MATCH_PREFIX);
        doc.push('-');
        doc.push_str(&(idx % 10).to_string());
        doc.push(' ');
    }
    if idx % 13 == 0 {
        doc.push_str(IREGEX_MATCH_PREFIX);
        doc.push('-');
        doc.push_str(&(idx % 7).to_string());
        doc.push(' ');
    }
    doc.trim_end().to_string()
}

fn parse_runtime_ms(plan: &str) -> Option<f64> {
    let runtime_re = Regex::new(
        r"(?m)(?:Total runtime|Execution Time):\s*([0-9]+(?:\.[0-9]+)?)\s*ms",
    )
    .ok()?;
    runtime_re
        .captures_iter(plan)
        .last()
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok())
}

fn parse_row_loops(plan: &str) -> Option<(u64, u64)> {
    let rows_re = Regex::new(r"rows=(?P<rows>[0-9]+(?:\.[0-9]+)?)\s+loops=(?P<loops>\d+)").ok()?;
    rows_re.captures_iter(plan).last().and_then(|caps| {
        let rows = caps.name("rows")?.as_str().parse::<f64>().ok()?;
        let loops = caps.name("loops")?.as_str().parse::<u64>().ok()?;
        Some((rows.round() as u64, loops))
    })
}

fn format_markdown_report(
    total_rows: u64,
    toasted_rows: u64,
    inline_rows: u64,
    toast_compression: ToastCompression,
    index_duration_ms: f64,
    conn_meta: &ConnectionMetadata,
    reclaim_result: Option<&ReclaimResult>,
    query_results: &[QueryResult],
) -> String {
    let now: DateTime<Utc> = Utc::now();
    let mut report = String::new();

    report.push_str("# pg_zoekt Real-World Benchmark\n\n");
    report.push_str(&format!(
        "**Generated:** {} UTC\n\n",
        now.format("%Y-%m-%d %H:%M:%S")
    ));
    report.push_str(&format!("**Connection:** {}\n\n", conn_meta.summary));
    report.push_str(&format!(
        "**Rows:** {} ({} toasted, {} inline)\n\n",
        total_rows, toasted_rows, inline_rows
    ));
    report.push_str(&format!(
        "**TOAST compression:** {}\n\n",
        toast_compression.label()
    ));
    report.push_str(&format!(
        "**Index build + seal:** {:.3} ms\n\n",
        index_duration_ms
    ));
    if let Some(reclaim) = reclaim_result {
        report.push_str(&format!(
            "**Reclaim phase:** delete_fraction={:.3}, deleted_rows={}, maintenance={:.3} ms, reclaimed_bytes={}, skipped_busy={}\n\n",
            reclaim.delete_fraction,
            reclaim.deleted_rows,
            reclaim.maintenance_ms,
            reclaim.reclaimed_bytes,
            reclaim.skipped_busy
        ));
    }

    report.push_str("| Query | Plan Rows | Loops | Total Runtime (ms) | Actual Matches |\n");
    report.push_str("| --- | --- | --- | --- | --- |\n");
    for result in query_results {
        report.push_str(&format!(
            "| {} | {} | {} | {:.3} | {} |\n",
            result.name, result.rows, result.loops, result.runtime_ms, result.match_count
        ));
    }
    report.push_str("\n");

    report.push_str("## Run Summary\n\n");
    report.push_str("| Metric | Value |\n");
    report.push_str("| --- | --- |\n");
    let total_queries = query_results.len() as u64;
    let total_runtime: f64 = query_results.iter().map(|q| q.runtime_ms).sum();
    let average_runtime = if total_queries > 0 {
        total_runtime / total_queries as f64
    } else {
        0.0
    };
    let (fastest_label, fastest_ms) = query_results
        .iter()
        .min_by(|a, b| {
            a.runtime_ms
                .partial_cmp(&b.runtime_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|q| (q.name.clone(), q.runtime_ms))
        .unwrap_or(("n/a".to_string(), 0.0));
    let (slowest_label, slowest_ms) = query_results
        .iter()
        .max_by(|a, b| {
            a.runtime_ms
                .partial_cmp(&b.runtime_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|q| (q.name.clone(), q.runtime_ms))
        .unwrap_or(("n/a".to_string(), 0.0));
    let total_matches: u64 = query_results.iter().map(|q| q.match_count).sum();
    report.push_str(&format!("| Total queries | {} |\n", total_queries));
    report.push_str(&format!(
        "| Average runtime | {:.3} ms |\n",
        average_runtime
    ));
    report.push_str(&format!(
        "| Fastest query | {} ({:.3} ms) |\n",
        fastest_label, fastest_ms
    ));
    report.push_str(&format!(
        "| Slowest query | {} ({:.3} ms) |\n",
        slowest_label, slowest_ms
    ));
    report.push_str(&format!("| Total matched rows | {} |\n", total_matches));
    if let Some(reclaim) = reclaim_result {
        report.push_str(&format!("| Reclaim deleted rows | {} |\n", reclaim.deleted_rows));
        report.push_str(&format!(
            "| Reclaim bytes recovered | {} |\n",
            reclaim.reclaimed_bytes
        ));
        report.push_str(&format!(
            "| Reclaim maintenance | {:.3} ms |\n",
            reclaim.maintenance_ms
        ));
    }
    report.push_str("\n");

    if let Some(reclaim) = reclaim_result {
        report.push_str("## Reclaim Phase\n\n");
        report.push_str(&format!(
            "- **Delete fraction:** {:.3}\n",
            reclaim.delete_fraction
        ));
        report.push_str(&format!("- **Deleted rows:** {}\n", reclaim.deleted_rows));
        report.push_str(&format!(
            "- **Maintenance runtime:** {:.3} ms\n",
            reclaim.maintenance_ms
        ));
        report.push_str(&format!(
            "- **Index bytes before:** {}\n",
            reclaim.index_bytes_before
        ));
        report.push_str(&format!(
            "- **Index bytes after:** {}\n",
            reclaim.index_bytes_after
        ));
        report.push_str(&format!(
            "- **Reclaimed bytes:** {}\n",
            reclaim.reclaimed_bytes
        ));
        report.push_str(&format!(
            "- **Skipped due to busy lock:** {}\n\n",
            reclaim.skipped_busy
        ));
    }

    for result in query_results {
        report.push_str(&format!("## {} Query\n\n", result.name));
        report.push_str(&format!("- **Rows matched:** {}\n", result.rows));
        report.push_str(&format!(
            "- **Actual matches (COUNT): {}\n",
            result.match_count
        ));
        report.push_str(&format!("- **Loops:** {}\n", result.loops));
        report.push_str(&format!(
            "- **Total runtime:** {:.3} ms\n\n",
            result.runtime_ms
        ));
        report.push_str("```text\n");
        report.push_str(&result.plan);
        report.push_str("\n```\n\n");
    }
    report
}

fn format_stderr_summary(
    total_rows: u64,
    toasted_rows: u64,
    inline_rows: u64,
    toast_compression: ToastCompression,
    index_duration_ms: f64,
    conn_meta: &ConnectionMetadata,
    reclaim_result: Option<&ReclaimResult>,
    query_results: &[QueryResult],
) -> String {
    let total_queries = query_results.len() as u64;
    let total_runtime: f64 = query_results.iter().map(|q| q.runtime_ms).sum();
    let average_runtime = if total_queries > 0 {
        total_runtime / total_queries as f64
    } else {
        0.0
    };
    let total_matches: u64 = query_results.iter().map(|q| q.match_count).sum();
    let (fastest_label, fastest_ms) = query_results
        .iter()
        .min_by(|a, b| {
            a.runtime_ms
                .partial_cmp(&b.runtime_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|q| (q.name.as_str(), q.runtime_ms))
        .unwrap_or(("n/a", 0.0));
    let (slowest_label, slowest_ms) = query_results
        .iter()
        .max_by(|a, b| {
            a.runtime_ms
                .partial_cmp(&b.runtime_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|q| (q.name.as_str(), q.runtime_ms))
        .unwrap_or(("n/a", 0.0));

    let query_col_width = query_results
        .iter()
        .map(|q| q.name.len())
        .max()
        .unwrap_or("Query".len())
        .max("Query".len());

    let mut out = String::new();
    out.push('\n');
    out.push_str("========== Benchmark Summary ==========\n");
    out.push_str(&format!("Connection: {}\n", conn_meta.summary));
    out.push_str(&format!(
        "Rows: {} ({} toasted, {} inline)\n",
        total_rows, toasted_rows, inline_rows
    ));
    out.push_str(&format!(
        "TOAST compression: {}\n",
        toast_compression.label()
    ));
    out.push_str(&format!("Index build + seal: {:.3} ms\n", index_duration_ms));
    if let Some(reclaim) = reclaim_result {
        out.push_str(&format!(
            "Reclaim: delete_fraction={:.3} deleted_rows={} runtime={:.3} ms reclaimed_bytes={} skipped_busy={}\n",
            reclaim.delete_fraction,
            reclaim.deleted_rows,
            reclaim.maintenance_ms,
            reclaim.reclaimed_bytes,
            reclaim.skipped_busy
        ));
    }
    out.push_str(&format!(
        "Queries: {}  Total runtime: {:.3} ms  Avg: {:.3} ms\n",
        total_queries, total_runtime, average_runtime
    ));
    out.push_str(&format!(
        "Fastest: {} ({:.3} ms)  Slowest: {} ({:.3} ms)\n",
        fastest_label, fastest_ms, slowest_label, slowest_ms
    ));
    out.push_str(&format!("Total matched rows: {}\n", total_matches));
    out.push('\n');

    out.push_str(&format!(
        "{:<query_width$}  {:>10}  {:>5}  {:>12}  {:>8}\n",
        "Query",
        "Plan Rows",
        "Loops",
        "Runtime (ms)",
        "Matches",
        query_width = query_col_width
    ));
    out.push_str(&format!(
        "{:-<query_width$}  {:-<10}  {:-<5}  {:-<12}  {:-<8}\n",
        "",
        "",
        "",
        "",
        "",
        query_width = query_col_width
    ));
    for result in query_results {
        out.push_str(&format!(
            "{:<query_width$}  {:>10}  {:>5}  {:>12.3}  {:>8}\n",
            result.name,
            result.rows,
            result.loops,
            result.runtime_ms,
            result.match_count,
            query_width = query_col_width
        ));
    }
    out.push_str("=======================================\n");
    out
}
