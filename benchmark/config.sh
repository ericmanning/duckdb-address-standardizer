#!/usr/bin/env bash
# Shared configuration for benchmark scripts.
# Set PARQUET_FILE before running any script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PG_REPO_DIR="$REPO_DIR/address-standardizer"

# --- User must set this ---
PARQUET_FILE="${PARQUET_FILE:?Set PARQUET_FILE env var to your .parquet path}"

# --- PostgreSQL ---
PG_DB="address_bench"
# Adjust if your pg_config is elsewhere:
PG_CONFIG="/opt/homebrew/opt/postgresql@18/bin/pg_config"
PG_BIN="$("$PG_CONFIG" --bindir 2>/dev/null)"
PSQL="$PG_BIN/psql"
CREATEDB="$PG_BIN/createdb"

# --- DuckDB ---
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
DUCKDB_CLI="$DUCKDB_BIN -unsigned"
DUCKDB_DB="/tmp/address_bench.duckdb"
DUCKDB_EXT="$REPO_DIR/build/release/address_standardizer.duckdb_extension"

# --- Benchmark parameters ---
RESULTS_DIR="/tmp/bench_results"
SIZES=(1000 10000 0)  # 0 = all rows (no LIMIT)
RUNS=3
DUCKDB_THREAD_COUNTS=(1 4 8 0)  # 0 = default (all cores)
PG_JOBS=8  # Number of parallel psql connections for PG export

mkdir -p "$RESULTS_DIR"
