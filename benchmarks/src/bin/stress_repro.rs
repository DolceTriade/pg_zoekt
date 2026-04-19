use std::env;
use std::ffi::OsString;
use std::process::{Command, Output};

struct Config {
    database_url: String,
    cycles: u32,
    bootstrap_segments: u32,
    seed: u64,
    keep_objects: bool,
}

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn range_u32(&mut self, low: u32, high_inclusive: u32) -> u32 {
        debug_assert!(low <= high_inclusive);
        let span = (high_inclusive - low) as u64 + 1;
        low + (self.next_u64() % span) as u32
    }

    fn chance_percent(&mut self, percent: u32) -> bool {
        self.range_u32(0, 99) < percent
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        let idx = (self.next_u64() % items.len() as u64) as usize;
        &items[idx]
    }
}

struct QuerySpec {
    label: String,
    predicate_sql: String,
}

fn main() {
    let config = parse_args(env::args_os().collect());
    if let Err(err) = run(config) {
        eprintln!("stress reproduction failed: {err}");
        std::process::exit(1);
    }
}

fn parse_args(args: Vec<OsString>) -> Config {
    let mut database_url = env::var("DATABASE_URL").unwrap_or_default();
    let mut cycles = 5_000;
    let mut bootstrap_segments = 10;
    let mut seed = 0xC0DE_570Du64;
    let mut keep_objects = false;

    let mut i = 1usize;
    while i < args.len() {
        let arg = args[i].to_string_lossy();
        match arg.as_ref() {
            "--database-url" => {
                i += 1;
                database_url = value_arg(&args, i, "--database-url");
            }
            "--cycles" => {
                i += 1;
                cycles = parse_u32_arg(&args, i, "--cycles");
            }
            "--bootstrap-segments" => {
                i += 1;
                bootstrap_segments = parse_u32_arg(&args, i, "--bootstrap-segments");
            }
            "--seed" => {
                i += 1;
                seed = parse_u64_arg(&args, i, "--seed");
            }
            "--keep" => {
                keep_objects = true;
            }
            "--help" | "-h" => {
                print_usage_and_exit();
            }
            _ => {
                eprintln!("unknown argument: {arg}");
                print_usage_and_exit();
            }
        }
        i += 1;
    }

    if database_url.is_empty() {
        eprintln!("DATABASE_URL is required (env var or --database-url)");
        print_usage_and_exit();
    }

    Config {
        database_url,
        cycles,
        bootstrap_segments,
        seed,
        keep_objects,
    }
}

fn value_arg(args: &[OsString], index: usize, flag: &str) -> String {
    args.get(index)
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| {
            eprintln!("missing value for {flag}");
            print_usage_and_exit();
        })
}

fn parse_u32_arg(args: &[OsString], index: usize, flag: &str) -> u32 {
    value_arg(args, index, flag).parse().unwrap_or_else(|_| {
        eprintln!("invalid integer for {flag}");
        print_usage_and_exit();
    })
}

fn parse_u64_arg(args: &[OsString], index: usize, flag: &str) -> u64 {
    value_arg(args, index, flag).parse().unwrap_or_else(|_| {
        eprintln!("invalid integer for {flag}");
        print_usage_and_exit();
    })
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "usage: cargo run --bin stress_repro -- \
         [--database-url URL] [--cycles N] [--bootstrap-segments N] [--seed N] [--keep]"
    );
    std::process::exit(2);
}

fn run(config: Config) -> Result<(), String> {
    let mut rng = Rng::new(config.seed);
    let table_name = format!("stress_docs_{:016x}", config.seed);
    let index_name = format!("idx_{}_text_zoekt", table_name);

    println!(
        "starting stress reproduction: seed={} cycles={} bootstrap_segments={} table={} index={}",
        config.seed, config.cycles, config.bootstrap_segments, table_name, index_name
    );

    run_sql(
        &config.database_url,
        &format!(
            "DROP TABLE IF EXISTS {table} CASCADE;
             CREATE TABLE {table} (
                 id BIGSERIAL PRIMARY KEY,
                 text TEXT NOT NULL
             );
             SET maintenance_work_mem = '64kB';
             CREATE INDEX {index} ON {table} USING pg_zoekt (text) WITH (parallel_workers = 4);",
            table = table_name,
            index = index_name,
        ),
    )?;

    for i in 1..=config.bootstrap_segments {
        run_sql(
            &config.database_url,
            &format!(
                "INSERT INTO {table} (text)
                 VALUES (repeat(md5(({i} * 131)::text), 16) || ' bootstrap ' || {i}::text);
                 SELECT * FROM pg_zoekt_maintain('{index}'::regclass, 'seal', true);",
                table = table_name,
                index = index_name,
                i = i,
            ),
        )?;
    }

    let baseline = run_sql_capture(
        &config.database_url,
        &format!(
            "SELECT segments_before, segments_after, sealed_tuples
             FROM pg_zoekt_maintain('{index}'::regclass, 'full', true);",
            index = index_name
        ),
    )?;
    println!("baseline full maintain: {}", baseline.stdout.trim());
    let baseline_validation = run_sql_capture(
        &config.database_url,
        &format!(
            "SELECT pg_zoekt_validate_segments('{index}'::regclass::oid);",
            index = index_name
        ),
    )?;
    println!("baseline segment validation: {}", baseline_validation.stdout.trim());
    validate_query_correctness(
        &config.database_url,
        &table_name,
        &index_name,
        0,
        "baseline",
        &baseline_query_specs(),
    )?;

    for cycle in 1..=config.cycles {
        let inserts = rng.range_u32(1, 12);
        let update_limit = rng.range_u32(0, 6);
        let tombstone_limit = rng.range_u32(0, 4);
        let delete_limit = rng.range_u32(0, 3);
        let update_mod = rng.range_u32(2, 17);
        let update_match = rng.range_u32(0, update_mod - 1);
        let delete_mod = rng.range_u32(2, 19);
        let delete_match = rng.range_u32(0, delete_mod - 1);
        let tombstone_mod = rng.range_u32(2, 23);
        let tombstone_match = rng.range_u32(0, tombstone_mod - 1);
        let do_update = update_limit > 0 && rng.chance_percent(60);
        let do_tombstone = tombstone_limit > 0 && rng.chance_percent(45);
        let do_delete = delete_limit > 0 && rng.chance_percent(35);

        let dml_sql = format!(
            "INSERT INTO {table} (text)
             SELECT repeat(md5(({cycle} * 100000 + gs)::text), 24) || ' cycle ' || {cycle}::text || ' row ' || gs::text
             FROM generate_series(1, {inserts}) gs;
             {update_stmt}
             {tombstone_stmt}
             {delete_stmt}
             SELECT * FROM pg_zoekt_maintain('{index}'::regclass, 'full', true);",
            table = table_name,
            index = index_name,
            cycle = cycle,
            inserts = inserts,
            update_stmt = if do_update {
                format!(
                    "UPDATE {table}
                     SET text = text || ' update-' || {cycle}::text
                     WHERE id IN (
                         SELECT id FROM {table}
                         WHERE (id % {update_mod}) = {update_match}
                         ORDER BY id
                         LIMIT {update_limit}
                     );",
                    table = table_name,
                    cycle = cycle,
                    update_mod = update_mod,
                    update_match = update_match,
                    update_limit = update_limit,
                )
            } else {
                String::new()
            },
            tombstone_stmt = if do_tombstone {
                format!(
                    "SELECT pg_zoekt_tombstone(
                         '{index}'::regclass,
                         ARRAY(
                             SELECT ctid FROM {table}
                             WHERE (id % {tombstone_mod}) = {tombstone_match}
                             ORDER BY id
                             LIMIT {tombstone_limit}
                         )
                     );",
                    index = index_name,
                    table = table_name,
                    tombstone_mod = tombstone_mod,
                    tombstone_match = tombstone_match,
                    tombstone_limit = tombstone_limit,
                )
            } else {
                String::new()
            },
            delete_stmt = if do_delete {
                format!(
                    "DELETE FROM {table}
                     WHERE id IN (
                         SELECT id FROM {table}
                         WHERE (id % {delete_mod}) = {delete_match}
                         ORDER BY id
                         LIMIT {delete_limit}
                     );",
                    table = table_name,
                    delete_mod = delete_mod,
                    delete_match = delete_match,
                    delete_limit = delete_limit,
                )
            } else {
                String::new()
            },
        );

        let query_specs = query_specs_for_cycle(cycle, do_update);

        match run_sql_capture(&config.database_url, &dml_sql) {
            Ok(output) => {
                let pre_ops = random_pre_maintenance_ops(&mut rng);
                let post_ops = random_post_maintenance_ops(&mut rng);
                let primary_mode = if rng.chance_percent(50) {
                    "seal"
                } else {
                    "full"
                };
                let mut phases = Vec::new();
                phases.extend(pre_ops);
                phases.push(primary_mode.to_string());
                phases.extend(post_ops);

                for phase in &phases {
                    let phase_sql = format!(
                        "SELECT * FROM pg_zoekt_maintain('{index}'::regclass, '{mode}', true);",
                        index = index_name,
                        mode = phase
                    );
                    if let Err(err) = run_sql_capture(&config.database_url, &phase_sql) {
                        emit_cycle_failure(
                            &config,
                            &table_name,
                            &index_name,
                            cycle,
                            inserts,
                            do_update,
                            do_tombstone,
                            do_delete,
                            update_limit,
                            update_mod,
                            update_match,
                            tombstone_limit,
                            tombstone_mod,
                            tombstone_match,
                            delete_limit,
                            delete_mod,
                            delete_match,
                            &dml_sql,
                            Some(phase),
                            &phases,
                        );
                        return Err(err);
                    }

                    let validation_sql = format!(
                        "SELECT pg_zoekt_validate_segments('{index}'::regclass::oid);",
                        index = index_name
                    );
                    if let Err(err) = run_sql_capture(&config.database_url, &validation_sql) {
                        emit_cycle_failure(
                            &config,
                            &table_name,
                            &index_name,
                            cycle,
                            inserts,
                            do_update,
                            do_tombstone,
                            do_delete,
                            update_limit,
                            update_mod,
                            update_match,
                            tombstone_limit,
                            tombstone_mod,
                            tombstone_match,
                            delete_limit,
                            delete_mod,
                            delete_match,
                            &dml_sql,
                            Some(phase),
                            &phases,
                        );
                        return Err(err);
                    }

                    if matches!(phase.as_str(), "seal" | "full" | "merge" | "truncate") {
                        if let Err(err) = validate_query_correctness(
                            &config.database_url,
                            &table_name,
                            &index_name,
                            cycle,
                            phase,
                            &query_specs,
                        ) {
                            emit_cycle_failure(
                                &config,
                                &table_name,
                                &index_name,
                                cycle,
                                inserts,
                                do_update,
                                do_tombstone,
                                do_delete,
                                update_limit,
                                update_mod,
                                update_match,
                                tombstone_limit,
                                tombstone_mod,
                                tombstone_match,
                                delete_limit,
                                delete_mod,
                                delete_match,
                                &dml_sql,
                                Some(phase),
                                &phases,
                            );
                            return Err(err);
                        }
                    }
                }

                println!(
                    "cycle={} inserts={} update={} tombstone={} delete={} phases={} result={}",
                    cycle,
                    inserts,
                    do_update,
                    do_tombstone,
                    do_delete,
                    phases.join("->"),
                    output.stdout.trim()
                );
            }
            Err(err) => {
                emit_cycle_failure(
                    &config,
                    &table_name,
                    &index_name,
                    cycle,
                    inserts,
                    do_update,
                    do_tombstone,
                    do_delete,
                    update_limit,
                    update_mod,
                    update_match,
                    tombstone_limit,
                    tombstone_mod,
                    tombstone_match,
                    delete_limit,
                    delete_mod,
                    delete_match,
                    &dml_sql,
                    None,
                    &[],
                );
                return Err(err);
            }
        }
    }

    if !config.keep_objects {
        run_sql(
            &config.database_url,
            &format!("DROP TABLE IF EXISTS {table} CASCADE;", table = table_name),
        )?;
    }

    println!("completed without reproduction");
    Ok(())
}

fn run_sql(database_url: &str, sql: &str) -> Result<(), String> {
    let output = run_sql_capture(database_url, sql)?;
    if !output.stdout.trim().is_empty() {
        println!("{}", output.stdout.trim());
    }
    Ok(())
}

struct SqlOutput {
    stdout: String,
}

fn run_sql_capture(database_url: &str, sql: &str) -> Result<SqlOutput, String> {
    let output = Command::new("psql")
        .env("DATABASE_URL", database_url)
        .arg(database_url)
        .arg("-X")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-At")
        .arg("-c")
        .arg(sql)
        .output()
        .map_err(|e| format!("failed to execute psql: {e}"))?;

    interpret_output(output)
}

fn interpret_output(output: Output) -> Result<SqlOutput, String> {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if output.status.success() {
        return Ok(SqlOutput { stdout });
    }
    Err(format!(
        "psql exited with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        stdout.trim(),
        stderr.trim()
    ))
}

fn random_pre_maintenance_ops(rng: &mut Rng) -> Vec<String> {
    let mut ops = Vec::new();
    if rng.chance_percent(20) {
        ops.push((*rng.choose(&["merge", "truncate"])).to_string());
    }
    if rng.chance_percent(10) {
        ops.push("truncate".to_string());
    }
    ops
}

fn random_post_maintenance_ops(rng: &mut Rng) -> Vec<String> {
    let mut ops = Vec::new();
    if rng.chance_percent(55) {
        ops.push("merge".to_string());
    }
    if rng.chance_percent(35) {
        ops.push("truncate".to_string());
    }
    if rng.chance_percent(20) {
        ops.push((*rng.choose(&["seal", "full"])).to_string());
    }
    if rng.chance_percent(15) {
        ops.push("truncate".to_string());
    }
    ops
}

fn baseline_query_specs() -> Vec<QuerySpec> {
    vec![
        QuerySpec {
            label: "bootstrap_like".to_string(),
            predicate_sql: "text LIKE '%bootstrap%'".to_string(),
        },
        QuerySpec {
            label: "bootstrap_ilike".to_string(),
            predicate_sql: "text ILIKE '%BOOTSTRAP%'".to_string(),
        },
    ]
}

fn query_specs_for_cycle(cycle: u32, include_update: bool) -> Vec<QuerySpec> {
    let mut specs = baseline_query_specs();
    specs.push(QuerySpec {
        label: format!("cycle_{cycle}_like"),
        predicate_sql: format!("text LIKE '%cycle {cycle}%'"),
    });
    specs.push(QuerySpec {
        label: format!("cycle_{cycle}_row1_and"),
        predicate_sql: format!("text LIKE '%cycle {cycle}%' AND text LIKE '%row 1%'"),
    });
    specs.push(QuerySpec {
        label: format!("cycle_{cycle}_ilike"),
        predicate_sql: format!("text ILIKE '%CYCLE {cycle}%'"),
    });
    if cycle > 1 {
        let prev = cycle - 1;
        specs.push(QuerySpec {
            label: format!("cycle_{prev}_carryover"),
            predicate_sql: format!("text LIKE '%cycle {prev}%'"),
        });
    }
    if include_update {
        specs.push(QuerySpec {
            label: format!("update_{cycle}_like"),
            predicate_sql: format!("text LIKE '%update-{cycle}%'"),
        });
    }
    specs
}

fn validate_query_correctness(
    database_url: &str,
    table_name: &str,
    index_name: &str,
    cycle: u32,
    phase: &str,
    specs: &[QuerySpec],
) -> Result<(), String> {
    for spec in specs {
        let heap_sig = query_signature(
            database_url,
            table_name,
            index_name,
            &spec.predicate_sql,
            QueryPath::Heap,
        )?;
        let index_sig = query_signature(
            database_url,
            table_name,
            index_name,
            &spec.predicate_sql,
            QueryPath::Index,
        )?;
        if heap_sig != index_sig {
            let explain = explain_index_query(database_url, table_name, &spec.predicate_sql)
                .unwrap_or_else(|e| format!("failed to explain index query: {e}"));
            let heap_ids = query_ids(
                database_url,
                table_name,
                index_name,
                &spec.predicate_sql,
                QueryPath::Heap,
            )
            .unwrap_or_else(|e| format!("failed to fetch heap ids: {e}"));
            let index_ids = query_ids(
                database_url,
                table_name,
                index_name,
                &spec.predicate_sql,
                QueryPath::Index,
            )
            .unwrap_or_else(|e| format!("failed to fetch index ids: {e}"));
            return Err(format!(
                "query mismatch at cycle={cycle} phase={phase} label={} index={} predicate={} heap_sig={} index_sig={}\nheap_ids={}\nindex_ids={}\nindex explain:\n{}",
                spec.label,
                index_name,
                spec.predicate_sql,
                heap_sig,
                index_sig,
                heap_ids,
                index_ids,
                explain
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum QueryPath {
    Heap,
    Index,
}

fn query_signature(
    database_url: &str,
    table_name: &str,
    index_name: &str,
    predicate_sql: &str,
    path: QueryPath,
) -> Result<String, String> {
    let gucs = match path {
        QueryPath::Heap => {
            "SET enable_indexscan = off;
             SET enable_indexonlyscan = off;
             SET enable_bitmapscan = off;"
        }
        QueryPath::Index => {
            "SET enable_seqscan = off;
             SET enable_bitmapscan = off;"
        }
    };
    let where_sql = filtered_predicate_sql(index_name, predicate_sql, path);
    let sql = format!(
        "{gucs}
         SELECT count(*)::bigint,
                COALESCE(md5(string_agg(id::text, ',' ORDER BY id)), 'empty')
         FROM {table} q
         WHERE {predicate};",
        gucs = gucs,
        table = table_name,
        predicate = where_sql
    );
    let output = run_sql_capture(database_url, &sql)?;
    Ok(last_data_line(&output.stdout).to_string())
}

fn query_ids(
    database_url: &str,
    table_name: &str,
    index_name: &str,
    predicate_sql: &str,
    path: QueryPath,
) -> Result<String, String> {
    let gucs = match path {
        QueryPath::Heap => {
            "SET enable_indexscan = off;
             SET enable_indexonlyscan = off;
             SET enable_bitmapscan = off;"
        }
        QueryPath::Index => {
            "SET enable_seqscan = off;
             SET enable_bitmapscan = off;"
        }
    };
    let where_sql = filtered_predicate_sql(index_name, predicate_sql, path);
    let sql = format!(
        "{gucs}
         SELECT COALESCE(string_agg(id::text, ',' ORDER BY id), 'empty')
         FROM (
             SELECT id
             FROM {table} q
             WHERE {predicate}
             ORDER BY id
             LIMIT 32
         ) s;",
        gucs = gucs,
        table = table_name,
        predicate = where_sql
    );
    let output = run_sql_capture(database_url, &sql)?;
    Ok(last_data_line(&output.stdout).to_string())
}

fn filtered_predicate_sql(index_name: &str, predicate_sql: &str, path: QueryPath) -> String {
    match path {
        QueryPath::Heap => format!(
            "({predicate}) AND NOT EXISTS (
                 SELECT 1
                 FROM pg_zoekt_tombstones('{index}'::regclass::oid) t
                 WHERE t.ctid = q.ctid
             )",
            predicate = predicate_sql,
            index = index_name,
        ),
        QueryPath::Index => predicate_sql.to_string(),
    }
}

fn explain_index_query(
    database_url: &str,
    table_name: &str,
    predicate_sql: &str,
) -> Result<String, String> {
    let sql = format!(
        "SET enable_seqscan = off;
         SET enable_bitmapscan = off;
         EXPLAIN SELECT id FROM {table} WHERE {predicate};",
        table = table_name,
        predicate = predicate_sql
    );
    let output = run_sql_capture(database_url, &sql)?;
    Ok(output.stdout)
}

fn last_data_line(stdout: &str) -> &str {
    stdout
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty() && *line != "SET")
        .unwrap_or("")
}

#[allow(clippy::too_many_arguments)]
fn emit_cycle_failure(
    config: &Config,
    table_name: &str,
    index_name: &str,
    cycle: u32,
    inserts: u32,
    do_update: bool,
    do_tombstone: bool,
    do_delete: bool,
    update_limit: u32,
    update_mod: u32,
    update_match: u32,
    tombstone_limit: u32,
    tombstone_mod: u32,
    tombstone_match: u32,
    delete_limit: u32,
    delete_mod: u32,
    delete_match: u32,
    dml_sql: &str,
    failed_phase: Option<&str>,
    phases: &[String],
) {
    eprintln!(
        "failure at cycle={} seed={} inserts={} update={} tombstone={} delete={} failed_phase={}",
        cycle,
        config.seed,
        inserts,
        do_update,
        do_tombstone,
        do_delete,
        failed_phase.unwrap_or("dml")
    );
    eprintln!(
        "parameters: update_limit={} update_mod={} update_match={} tombstone_limit={} tombstone_mod={} tombstone_match={} delete_limit={} delete_mod={} delete_match={}",
        update_limit,
        update_mod,
        update_match,
        tombstone_limit,
        tombstone_mod,
        tombstone_match,
        delete_limit,
        delete_mod,
        delete_match
    );
    if !phases.is_empty() {
        eprintln!("phase sequence: {}", phases.join("->"));
    }
    eprintln!("failing dml sql:\n{dml_sql}");
    let _ = run_sql_capture(
        &config.database_url,
        &format!(
            "SELECT * FROM pg_zoekt_index_segments('{index}'::regclass);",
            index = index_name
        ),
    )
    .map(|segments| eprintln!("segments at failure:\n{}", segments.stdout.trim()));
    if !config.keep_objects {
        let _ = run_sql(
            &config.database_url,
            &format!("DROP TABLE IF EXISTS {table} CASCADE;", table = table_name),
        );
    }
}
