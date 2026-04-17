#!/usr/bin/env bash
# Load parquet data into both PostgreSQL and DuckDB.
source "$(dirname "$0")/../config.sh"

echo "=== Loading Data ==="
echo "Parquet file: $PARQUET_FILE"

# ─── PostgreSQL ───────────────────────────────────────────────
echo ""
echo "--- PostgreSQL ---"

# Convert parquet to CSV via DuckDB
CSV_FILE="$RESULTS_DIR/addresses.csv"
echo "Converting parquet to CSV..."
$DUCKDB_CLI -c "
  COPY (
    SELECT id, addr1, addr2, city, state, zip
    FROM read_parquet('$PARQUET_FILE')
  ) TO '$CSV_FILE' (HEADER, DELIMITER ',');
"
ROW_COUNT=$(wc -l < "$CSV_FILE" | tr -d ' ')
echo "CSV written: $CSV_FILE ($((ROW_COUNT - 1)) data rows)"

# Create table and load
"$PSQL" -d "$PG_DB" <<'SQL'
DROP TABLE IF EXISTS addr;
CREATE TABLE addr (
  id TEXT,
  addr1 TEXT,
  addr2 TEXT,
  city TEXT,
  state TEXT,
  zip TEXT
);
SQL

echo "Loading into PostgreSQL..."
"$PSQL" -d "$PG_DB" -c "\COPY addr FROM '$CSV_FILE' WITH (FORMAT csv, HEADER true);"
"$PSQL" -d "$PG_DB" -c "ANALYZE addr;"
PG_COUNT=$("$PSQL" -d "$PG_DB" -tAc "SELECT count(*) FROM addr;")
echo "PostgreSQL: $PG_COUNT rows loaded."

# ─── DuckDB ───────────────────────────────────────────────────
echo ""
echo "--- DuckDB ---"

# Remove old DB if it exists
rm -f "$DUCKDB_DB" "$DUCKDB_DB.wal"

echo "Creating DuckDB database and loading data..."
$DUCKDB_CLI "$DUCKDB_DB" <<SQL
LOAD '$DUCKDB_EXT';

-- Load built-in US reference data
SELECT load_us_address_data();

-- Address data
CREATE TABLE addr AS
  SELECT id, addr1, addr2, city, state, zip
  FROM read_parquet('$PARQUET_FILE');

SELECT count(*) AS duckdb_row_count FROM addr;
SQL

echo ""
echo "=== Data loading complete ==="
