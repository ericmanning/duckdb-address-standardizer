#!/usr/bin/env bash
# Set up PostgreSQL 18, build+install the address_standardizer extension
# from the address-standardizer submodule, and create the benchmark database.
source "$(dirname "$0")/config.sh"

echo "=== PostgreSQL Setup ==="

# 1. Check PG 18 is installed
if ! brew list postgresql@18 &>/dev/null; then
  echo "Installing postgresql@18 via Homebrew..."
  brew install postgresql@18
fi

# Ensure pg_config is valid
PG_VERSION=$("$PG_CONFIG" --version)
echo "pg_config reports: $PG_VERSION"
if ! echo "$PG_VERSION" | grep -q "PostgreSQL 18"; then
  echo "ERROR: Expected PostgreSQL 18.x, got: $PG_VERSION"
  exit 1
fi

# 2. Ensure PG server is running
if ! "$PG_BIN/pg_isready" -q 2>/dev/null; then
  echo "Starting PostgreSQL..."
  brew services start postgresql@18
  sleep 2
  if ! "$PG_BIN/pg_isready" -q; then
    echo "ERROR: PostgreSQL failed to start"
    exit 1
  fi
fi
echo "PostgreSQL is running."

# 3. Verify server version
SERVER_VER=$("$PSQL" -d postgres -tAc "SHOW server_version;" 2>/dev/null)
echo "Server version: $SERVER_VER"
if ! echo "$SERVER_VER" | grep -q "^18\."; then
  echo "WARNING: Server is not 18.x — you may be connected to a different cluster."
fi

# 4. Build extension from address-standardizer submodule
echo ""
echo "=== Building PG extension from address-standardizer submodule ==="
cd "$PG_REPO_DIR"
PCRE2_PREFIX="$(brew --prefix pcre2)"
AS_VER=$(grep default_version "$PG_REPO_DIR/address_standardizer.control" | cut -f2 -d= | tr -d "' ")
PG_BINDIR=$("$PG_CONFIG" --bindir)
CPPFLAGS="-I$PCRE2_PREFIX/include -DAS_VERSION=\\\"$AS_VER\\\" -DPCRE_VERSION=2"
SHLIB="-lpcre2-8 -L$PCRE2_PREFIX/lib -bundle_loader $PG_BINDIR/postgres"
make clean  PG_CONFIG="$PG_CONFIG" PG_CPPFLAGS="$CPPFLAGS" SHLIB_LINK="$SHLIB"
make        PG_CONFIG="$PG_CONFIG" PG_CPPFLAGS="$CPPFLAGS" SHLIB_LINK="$SHLIB"
sudo make install PG_CONFIG="$PG_CONFIG" PG_CPPFLAGS="$CPPFLAGS" SHLIB_LINK="$SHLIB"
echo "Extension installed."

# 5. Create database and extensions
echo ""
echo "=== Creating benchmark database ==="
"$CREATEDB" "$PG_DB" 2>/dev/null || echo "Database '$PG_DB' already exists."
"$PSQL" -d "$PG_DB" -c "CREATE EXTENSION IF NOT EXISTS address_standardizer;"
"$PSQL" -d "$PG_DB" -c "CREATE EXTENSION IF NOT EXISTS address_standardizer_data_us;"

echo ""
echo "=== PG setup complete ==="
"$PSQL" -d "$PG_DB" -c "SELECT extname, extversion FROM pg_extension WHERE extname LIKE 'address%';"
