#!/usr/bin/env bash
# Set up data for addrust benchmarks: build the addrust CLI, build the DuckDB
# extension, and create a DuckDB database with a single-column address table.
source "$(dirname "$0")/../config.sh"

ADDRUST_DIR="$REPO_DIR/addrust"
ADDRUST_BIN="$ADDRUST_DIR/target/release/addrust"
ADDRUST_DB="$RESULTS_DIR/addrust_bench.duckdb"

echo "=== Addrust Benchmark Setup ==="

# ─── Build addrust CLI ──────────────────────────────────────
echo ""
echo "--- Building addrust CLI (release) ---"
if [ ! -d "$ADDRUST_DIR" ]; then
  echo "ERROR: addrust submodule not found at $ADDRUST_DIR"
  echo "Run: git submodule update --init addrust"
  exit 1
fi
(cd "$ADDRUST_DIR" && cargo build --release 2>&1)
if [ ! -f "$ADDRUST_BIN" ]; then
  echo "ERROR: addrust binary not found at $ADDRUST_BIN"
  exit 1
fi
echo "addrust CLI: $ADDRUST_BIN"
"$ADDRUST_BIN" parse --format full <<< "123 Main St" | head -1

# ─── Build DuckDB extension ────────────────────────────────
echo ""
echo "--- Building DuckDB extension (release) ---"
if [ ! -f "$DUCKDB_EXT" ]; then
  (cd "$REPO_DIR" && make configure && make release)
fi
echo "Extension: $DUCKDB_EXT"

# ─── Create DuckDB database with address column ────────────
echo ""
echo "--- Creating DuckDB database ---"
rm -f "$ADDRUST_DB" "$ADDRUST_DB.wal"

$DUCKDB_CLI "$ADDRUST_DB" <<SQL
-- Create a single 'address' column by concatenating components.
-- This matches what addrust expects: one full address string per row.
CREATE TABLE addr AS
  SELECT
    id,
    concat_ws(', ',
      concat_ws(' ', addr1, addr2),
      concat_ws(' ', city, state, zip)
    ) AS address
  FROM read_parquet('$PARQUET_FILE');

SELECT count(*) AS row_count FROM addr;
SQL

echo ""
echo "=== Setup complete ==="
echo "Database: $ADDRUST_DB"
echo "Addrust CLI: $ADDRUST_BIN"
echo "DuckDB extension: $DUCKDB_EXT"
