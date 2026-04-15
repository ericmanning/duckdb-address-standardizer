#!/usr/bin/env bash
# Benchmark the DuckDB addrust_parse() extension function.
# Parses all addresses via the extension (default + custom config),
# writing results into the benchmark DuckDB database.
source "$(dirname "$0")/../config.sh"

ADDRUST_DB="$RESULTS_DIR/addrust_bench.duckdb"
CUSTOM_CONFIG="$(cd "$(dirname "$0")" && pwd)/config_short_suffix.toml"

if [ ! -f "$ADDRUST_DB" ]; then
  echo "ERROR: benchmark database not found. Run 01_setup.sh first."
  exit 1
fi

echo "=== DuckDB Extension Benchmark ==="

# ─── Default config ─────────────────────────────────────────
echo ""
echo "--- Extension: default config ---"
$DUCKDB_CLI "$ADDRUST_DB" <<SQL
LOAD '$DUCKDB_EXT';

DROP TABLE IF EXISTS addr_ext_default;

.timer on
CREATE TABLE addr_ext_default AS
  SELECT id, ap.*
  FROM (
    SELECT id, addrust_parse(address) AS ap
    FROM addr
  );
.timer off

SELECT count(*) AS ext_default_rows FROM addr_ext_default;
SQL

# ─── Custom config ──────────────────────────────────────────
echo ""
echo "--- Extension: custom config ($CUSTOM_CONFIG) ---"
$DUCKDB_CLI "$ADDRUST_DB" <<SQL
LOAD '$DUCKDB_EXT';

DROP TABLE IF EXISTS addr_ext_custom;

.timer on
CREATE TABLE addr_ext_custom AS
  SELECT id, ap.*
  FROM (
    SELECT id, addrust_parse(address, '$CUSTOM_CONFIG') AS ap
    FROM addr
  );
.timer off

SELECT count(*) AS ext_custom_rows FROM addr_ext_custom;
SQL

echo ""
echo "=== Extension benchmark complete ==="
