use pgrx::prelude::*;

/// Strategy numbers advertised for the pg_zoekt access method.
pub const STRATEGY_LIKE: u16 = 1;
pub const STRATEGY_ILIKE: u16 = 2;
pub const STRATEGY_REGEX: u16 = 3;
pub const STRATEGY_IREGEX: u16 = 4;
pub const NUM_STRATEGIES: u16 = STRATEGY_IREGEX;

/// Support procs advertised for pg_zoekt operator classes.
pub const NUM_SUPPORT: u16 = 1;

/// Placeholder support function so we can register operator class wiring.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn pg_zoekt_text_consistent(_value: &str, _query: &str) -> bool {
    // Until the index scan path is implemented, keep the planner from erroring out by
    // simply reporting that the clause is safe to consider. The actual filtering is
    // still done by Postgres.
    true
}

extension_sql!(
    r#"
DO $$
BEGIN
    -- Ensure the operator family exists before adding operators.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_opfamily f
        WHERE f.opfname = 'pg_zoekt_text_ops'
          AND f.opfmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE amname = 'pg_zoekt')
          AND f.opfnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='@extschema@')
    ) THEN
        CREATE OPERATOR FAMILY pg_zoekt_text_ops USING pg_zoekt;
    END IF;

    -- Ensure the default text operator class exists.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_opclass c
        WHERE c.opcname = 'pg_zoekt_text_ops'
          AND c.opcmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE amname = 'pg_zoekt')
          AND c.opcnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='@extschema@')
    ) THEN
        CREATE OPERATOR CLASS pg_zoekt_text_ops DEFAULT
        FOR TYPE text USING pg_zoekt AS
            STORAGE text;
    END IF;

    -- Wire LIKE/ILIKE/regex operators into the family if they aren't present yet.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_amop o
        JOIN pg_catalog.pg_opfamily f ON f.oid = o.amopfamily
        WHERE f.opfname = 'pg_zoekt_text_ops'
          AND f.opfnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='@extschema@')
          AND f.opfmethod = (SELECT oid FROM pg_catalog.pg_am WHERE amname = 'pg_zoekt')
    ) THEN
        ALTER OPERATOR FAMILY pg_zoekt_text_ops USING pg_zoekt ADD
            OPERATOR 1 ~~ (text, text),
            OPERATOR 2 ~~* (text, text),
            OPERATOR 3 ~ (text, text),
            OPERATOR 4 ~* (text, text);
    END IF;

    -- Add a placeholder support function so we can hook planning later.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_amproc p
        JOIN pg_catalog.pg_opfamily f ON f.oid = p.amprocfamily
        WHERE f.opfname = 'pg_zoekt_text_ops'
          AND f.opfnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='@extschema@')
          AND f.opfmethod = (SELECT oid FROM pg_catalog.pg_am WHERE amname = 'pg_zoekt')
          AND p.amprocnum = 1
    ) THEN
        ALTER OPERATOR FAMILY pg_zoekt_text_ops USING pg_zoekt ADD
            FUNCTION 1 (text, text) pg_zoekt_text_consistent(text, text);
    END IF;
END;
$$;
"#,
    name = "pg_zoekt_text_operators",
    requires = [pg_zoekt_handler, pg_zoekt_text_consistent]
);
