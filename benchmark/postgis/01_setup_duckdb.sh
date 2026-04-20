#!/usr/bin/env bash
# Build the DuckDB us_address_standardizer extension (release).
source "$(dirname "$0")/../config.sh"

echo "=== DuckDB Extension Setup ==="

# 1. Check DuckDB CLI
if ! command -v "$DUCKDB_BIN" &>/dev/null; then
  echo "ERROR: DuckDB CLI not found at '$DUCKDB_BIN'. Install via: brew install duckdb"
  exit 1
fi
echo "DuckDB CLI: $($DUCKDB_CLI -c 'SELECT version();' 2>/dev/null | tail -1)"

# 2. Build release extension
echo ""
echo "=== Building DuckDB extension (release) ==="
cd "$REPO_DIR"
make configure 2>/dev/null || true  # idempotent; may already be configured
make release

# 3. Verify extension was built
if [ ! -f "$DUCKDB_EXT" ]; then
  echo "ERROR: Extension not found at $DUCKDB_EXT"
  exit 1
fi
echo ""
echo "=== DuckDB extension built ==="
echo "Extension: $DUCKDB_EXT"
ls -lh "$DUCKDB_EXT"
