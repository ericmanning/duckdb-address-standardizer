#!/usr/bin/env bash
# Compare outputs from the addrust CLI vs the DuckDB addrust_parse() extension.
# Both should produce identical results since they use the same Rust library.
# Any diff is a bug in the FFI/wrapper layer.
#
# Requires: 02_bench_cli.sh and 03_bench_extension.sh have both run.
source "$(dirname "$0")/../config.sh"

ADDRUST_DB="$RESULTS_DIR/addrust_bench.duckdb"

if [ ! -f "$ADDRUST_DB" ]; then
  echo "ERROR: benchmark database not found. Run 01_setup.sh first."
  exit 1
fi

# The addrust CLI output table has an 'address' column (the original input)
# plus the 15 parsed field columns. The extension output has 'id' plus the
# 15 fields. We join on id (which the CLI table lacks) so we need the addr
# table to map address → id. However, addresses may not be unique, so
# instead we export both to CSV ordered by id and compare directly.

FIELDS="street_number, pre_direction, street_name, suffix, post_direction, unit_type, unit, po_box, building, building_type, extra_front, extra_back, city, state, zip"

echo "=== Comparing CLI vs Extension (Default Config) ==="

$DUCKDB_CLI "$ADDRUST_DB" <<SQL

-- The CLI table has 'address' + fields but no id.
-- The extension table has 'id' + fields.
-- The addr table has 'id' + 'address'.
-- Join CLI results back via address text to get the id.
-- Note: if duplicate addresses exist, this may produce extra rows.
-- We handle that by using row_number() partitioned by address.

-- Approach: both tables were created from the same addr table in the same
-- order. The CLI sorts by street_name before inserting (for compression),
-- so row order differs. We need to join on the original address text.

-- Simpler approach: export both to CSV with the address column, sort, diff.

-- Actually, let's just compare by joining ext (has id) to addr (has address)
-- to cli (has address + fields).

-- Step 1: Attach id to CLI results via the addr table
CREATE TEMP TABLE cli_with_id AS
  SELECT a.id, c.*
  FROM addr_cli_default c
  JOIN addr a ON a.address = c.address;

-- Step 2: Per-field mismatch counts
SELECT
  count(*) AS total_joined,
  count(*) FILTER (WHERE c.street_number IS DISTINCT FROM e.street_number) AS street_number_diff,
  count(*) FILTER (WHERE c.pre_direction IS DISTINCT FROM e.pre_direction) AS pre_direction_diff,
  count(*) FILTER (WHERE c.street_name IS DISTINCT FROM e.street_name) AS street_name_diff,
  count(*) FILTER (WHERE c.suffix IS DISTINCT FROM e.suffix) AS suffix_diff,
  count(*) FILTER (WHERE c.post_direction IS DISTINCT FROM e.post_direction) AS post_direction_diff,
  count(*) FILTER (WHERE c.unit_type IS DISTINCT FROM e.unit_type) AS unit_type_diff,
  count(*) FILTER (WHERE c.unit IS DISTINCT FROM e.unit) AS unit_diff,
  count(*) FILTER (WHERE c.po_box IS DISTINCT FROM e.po_box) AS po_box_diff,
  count(*) FILTER (WHERE c.building IS DISTINCT FROM e.building) AS building_diff,
  count(*) FILTER (WHERE c.building_type IS DISTINCT FROM e.building_type) AS building_type_diff,
  count(*) FILTER (WHERE c.extra_front IS DISTINCT FROM e.extra_front) AS extra_front_diff,
  count(*) FILTER (WHERE c.extra_back IS DISTINCT FROM e.extra_back) AS extra_back_diff,
  count(*) FILTER (WHERE c.city IS DISTINCT FROM e.city) AS city_diff,
  count(*) FILTER (WHERE c.state IS DISTINCT FROM e.state) AS state_diff,
  count(*) FILTER (WHERE c.zip IS DISTINCT FROM e.zip) AS zip_diff
FROM cli_with_id c
JOIN addr_ext_default e ON c.id = e.id;

-- Step 3: Sample mismatches (first 20)
SELECT c.id,
  CASE WHEN c.street_number IS DISTINCT FROM e.street_number
       THEN 'CLI=' || coalesce(c.street_number,'NULL') || ' EXT=' || coalesce(e.street_number,'NULL') END AS street_number,
  CASE WHEN c.street_name IS DISTINCT FROM e.street_name
       THEN 'CLI=' || coalesce(c.street_name,'NULL') || ' EXT=' || coalesce(e.street_name,'NULL') END AS street_name,
  CASE WHEN c.suffix IS DISTINCT FROM e.suffix
       THEN 'CLI=' || coalesce(c.suffix,'NULL') || ' EXT=' || coalesce(e.suffix,'NULL') END AS suffix,
  CASE WHEN c.city IS DISTINCT FROM e.city
       THEN 'CLI=' || coalesce(c.city,'NULL') || ' EXT=' || coalesce(e.city,'NULL') END AS city,
  CASE WHEN c.state IS DISTINCT FROM e.state
       THEN 'CLI=' || coalesce(c.state,'NULL') || ' EXT=' || coalesce(e.state,'NULL') END AS state,
  CASE WHEN c.zip IS DISTINCT FROM e.zip
       THEN 'CLI=' || coalesce(c.zip,'NULL') || ' EXT=' || coalesce(e.zip,'NULL') END AS zip
FROM cli_with_id c
JOIN addr_ext_default e ON c.id = e.id
WHERE c.street_number IS DISTINCT FROM e.street_number
   OR c.street_name IS DISTINCT FROM e.street_name
   OR c.suffix IS DISTINCT FROM e.suffix
   OR c.city IS DISTINCT FROM e.city
   OR c.state IS DISTINCT FROM e.state
   OR c.zip IS DISTINCT FROM e.zip
LIMIT 20;

SQL

echo ""
echo "=== Comparing CLI vs Extension (Custom Config) ==="

$DUCKDB_CLI "$ADDRUST_DB" <<SQL

CREATE TEMP TABLE cli_custom_with_id AS
  SELECT a.id, c.*
  FROM addr_cli_custom c
  JOIN addr a ON a.address = c.address;

SELECT
  count(*) AS total_joined,
  count(*) FILTER (WHERE c.street_number IS DISTINCT FROM e.street_number) AS street_number_diff,
  count(*) FILTER (WHERE c.pre_direction IS DISTINCT FROM e.pre_direction) AS pre_direction_diff,
  count(*) FILTER (WHERE c.street_name IS DISTINCT FROM e.street_name) AS street_name_diff,
  count(*) FILTER (WHERE c.suffix IS DISTINCT FROM e.suffix) AS suffix_diff,
  count(*) FILTER (WHERE c.post_direction IS DISTINCT FROM e.post_direction) AS post_direction_diff,
  count(*) FILTER (WHERE c.unit_type IS DISTINCT FROM e.unit_type) AS unit_type_diff,
  count(*) FILTER (WHERE c.unit IS DISTINCT FROM e.unit) AS unit_diff,
  count(*) FILTER (WHERE c.po_box IS DISTINCT FROM e.po_box) AS po_box_diff,
  count(*) FILTER (WHERE c.city IS DISTINCT FROM e.city) AS city_diff,
  count(*) FILTER (WHERE c.state IS DISTINCT FROM e.state) AS state_diff,
  count(*) FILTER (WHERE c.zip IS DISTINCT FROM e.zip) AS zip_diff
FROM cli_custom_with_id c
JOIN addr_ext_custom e ON c.id = e.id;

-- Sample mismatches
SELECT c.id,
  CASE WHEN c.street_name IS DISTINCT FROM e.street_name
       THEN 'CLI=' || coalesce(c.street_name,'NULL') || ' EXT=' || coalesce(e.street_name,'NULL') END AS street_name,
  CASE WHEN c.suffix IS DISTINCT FROM e.suffix
       THEN 'CLI=' || coalesce(c.suffix,'NULL') || ' EXT=' || coalesce(e.suffix,'NULL') END AS suffix,
  CASE WHEN c.pre_direction IS DISTINCT FROM e.pre_direction
       THEN 'CLI=' || coalesce(c.pre_direction,'NULL') || ' EXT=' || coalesce(e.pre_direction,'NULL') END AS pre_direction,
  CASE WHEN c.state IS DISTINCT FROM e.state
       THEN 'CLI=' || coalesce(c.state,'NULL') || ' EXT=' || coalesce(e.state,'NULL') END AS state
FROM cli_custom_with_id c
JOIN addr_ext_custom e ON c.id = e.id
WHERE c.street_number IS DISTINCT FROM e.street_number
   OR c.street_name IS DISTINCT FROM e.street_name
   OR c.suffix IS DISTINCT FROM e.suffix
   OR c.pre_direction IS DISTINCT FROM e.pre_direction
   OR c.state IS DISTINCT FROM e.state
   OR c.zip IS DISTINCT FROM e.zip
LIMIT 20;

SQL

echo ""
echo "=== Comparison complete ==="
echo "If all diffs are 0: the FFI wrapper produces identical output to the CLI."
echo "If diffs exist: investigate the sample rows above for FFI bugs."
