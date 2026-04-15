#!/usr/bin/env bash
# Sanity check: run a known address through both engines and compare output.
source "$(dirname "$0")/../config.sh"

MICRO="123 Main Street"
MACRO="Kansas City, MO 45678"

echo "=== Verification ==="
echo "Input:  micro='$MICRO'  macro='$MACRO'"
echo ""

echo "--- PostgreSQL ---"
"$PSQL" -d "$PG_DB" -c "
  SELECT (sa).building, (sa).house_num, (sa).predir, (sa).qual, (sa).pretype,
         (sa).name, (sa).suftype, (sa).sufdir, (sa).ruralroute, (sa).extra,
         (sa).city, (sa).state, (sa).country, (sa).postcode, (sa).box, (sa).unit
  FROM (
    SELECT standardize_address('us_lex', 'us_gaz', 'us_rules',
      '$MICRO', '$MACRO') AS sa
  ) t;
"

echo "--- DuckDB ---"
$DUCKDB_CLI "$DUCKDB_DB" <<SQL
LOAD '$DUCKDB_EXT';
SELECT sa.*
FROM (
  SELECT standardize_address('us_lex', 'us_gaz', 'us_rules',
    '$MICRO', '$MACRO') AS sa
);
SQL

echo ""
echo "--- First row from data file ---"

echo "PostgreSQL:"
"$PSQL" -d "$PG_DB" -c "
  SELECT id, (sa).*
  FROM (
    SELECT id, standardize_address('us_lex', 'us_gaz', 'us_rules',
      concat_ws(', ', addr1, addr2),
      concat_ws(', ', city, state, zip)) AS sa
    FROM addr LIMIT 1
  ) t;
"

echo "DuckDB:"
$DUCKDB_CLI "$DUCKDB_DB" <<SQL
LOAD '$DUCKDB_EXT';
SELECT id, sa.*
FROM (
  SELECT id, standardize_address('us_lex', 'us_gaz', 'us_rules',
    concat_ws(', ', addr1, addr2),
    concat_ws(', ', city, state, zip)) AS sa
  FROM addr LIMIT 1
);
SQL

echo ""
echo "=== Verify that outputs match above ==="
