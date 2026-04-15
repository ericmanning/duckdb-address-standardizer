#!/usr/bin/env bash
# Benchmark the addrust CLI tool in DuckDB mode.
# Parses all addresses via the CLI (default config + custom config),
# writing results into the benchmark DuckDB database.
source "$(dirname "$0")/../config.sh"

ADDRUST_DIR="$REPO_DIR/addrust"
ADDRUST_BIN="$ADDRUST_DIR/target/release/addrust"
ADDRUST_DB="$RESULTS_DIR/addrust_bench.duckdb"
CUSTOM_CONFIG="$(cd "$(dirname "$0")" && pwd)/config_short_suffix.toml"

if [ ! -f "$ADDRUST_BIN" ]; then
  echo "ERROR: addrust binary not found. Run 01_setup.sh first."
  exit 1
fi
if [ ! -f "$ADDRUST_DB" ]; then
  echo "ERROR: benchmark database not found. Run 01_setup.sh first."
  exit 1
fi

echo "=== Addrust CLI Benchmark ==="

# ─── Default config ─────────────────────────────────────────
echo ""
echo "--- CLI: default config ---"
"$ADDRUST_BIN" parse \
  --duckdb "$ADDRUST_DB" \
  --input-table addr \
  --output-table addr_cli_default \
  --column address \
  --overwrite \
  --time

# ─── Custom config ──────────────────────────────────────────
echo ""
echo "--- CLI: custom config ($CUSTOM_CONFIG) ---"
"$ADDRUST_BIN" --config "$CUSTOM_CONFIG" parse \
  --duckdb "$ADDRUST_DB" \
  --input-table addr \
  --output-table addr_cli_custom \
  --column address \
  --overwrite \
  --time

# ─── Row counts ─────────────────────────────────────────────
echo ""
echo "--- Row counts ---"
$DUCKDB_CLI "$ADDRUST_DB" <<SQL
SELECT 'addr_cli_default' AS tbl, count(*) AS rows FROM addr_cli_default
UNION ALL
SELECT 'addr_cli_custom', count(*) FROM addr_cli_custom;
SQL

echo ""
echo "=== CLI benchmark complete ==="
