#!/usr/bin/env bash
# Re-standardize rows that differed between PG and DuckDB through a fresh
# single PG connection, then compare against DuckDB.  This isolates PAGC
# state-leakage diffs from real code differences.
#
# Requires: 06_export_and_compare.sh has already run (produces results_pg.csv
# and results_duckdb.csv in $RESULTS_DIR).
source "$(dirname "$0")/config.sh"

PG_CSV="$RESULTS_DIR/results_pg.csv"
DK_CSV="$RESULTS_DIR/results_duckdb.csv"
DIFF_IDS="$RESULTS_DIR/diff_ids.csv"
PG_RECHECK_CSV="$RESULTS_DIR/results_pg_recheck.csv"

# ─── Verify inputs ──────────────────────────────────────────────
for f in "$PG_CSV" "$DK_CSV"; do
  if [ ! -f "$f" ]; then
    echo "ERROR: $f not found — run 06_export_and_compare.sh first"
    exit 1
  fi
done

# ─── Step 1: Extract diff IDs using DuckDB ──────────────────────
echo "=== Step 1: Extracting differing row IDs ==="

$DUCKDB_CLI <<SQL
CREATE TABLE pg AS SELECT * FROM read_csv('$PG_CSV', all_varchar=true, header=true, null_padding=true);
CREATE TABLE dk AS SELECT * FROM read_csv('$DK_CSV', all_varchar=true, header=true, null_padding=true);

COPY (
  SELECT p.id FROM pg p JOIN dk d ON p.id = d.id
  WHERE p.building  IS DISTINCT FROM d.building
     OR p.house_num IS DISTINCT FROM d.house_num
     OR p.predir    IS DISTINCT FROM d.predir
     OR p.qual      IS DISTINCT FROM d.qual
     OR p.pretype   IS DISTINCT FROM d.pretype
     OR p.name      IS DISTINCT FROM d.name
     OR p.suftype   IS DISTINCT FROM d.suftype
     OR p.sufdir    IS DISTINCT FROM d.sufdir
     OR p.ruralroute IS DISTINCT FROM d.ruralroute
     OR p.extra     IS DISTINCT FROM d.extra
     OR p.city      IS DISTINCT FROM d.city
     OR p.state     IS DISTINCT FROM d.state
     OR p.country   IS DISTINCT FROM d.country
     OR p.postcode  IS DISTINCT FROM d.postcode
     OR p.box       IS DISTINCT FROM d.box
     OR p.unit      IS DISTINCT FROM d.unit
  ORDER BY p.id
) TO '$DIFF_IDS' (HEADER);
SQL

DIFF_COUNT=$(tail -n +2 "$DIFF_IDS" | wc -l | tr -d ' ')
echo "Found $DIFF_COUNT differing rows"

if [ "$DIFF_COUNT" -eq 0 ]; then
  echo "No diffs — nothing to recheck."
  exit 0
fi

# ─── Step 2: Re-standardize diff rows via single PG connection ──
echo ""
echo "=== Step 2: Re-standardizing $DIFF_COUNT rows via single PG connection ==="

"$PSQL" -d "$PG_DB" \
  -c "CREATE TEMP TABLE diff_ids (id text);" \
  -c "\\COPY diff_ids FROM '$DIFF_IDS' WITH (FORMAT csv, HEADER)" \
  -c "
CREATE TEMP TABLE pg_recheck AS
SELECT id, (sa).building, (sa).house_num, (sa).predir, (sa).qual, (sa).pretype,
       (sa).name, (sa).suftype, (sa).sufdir, (sa).ruralroute, (sa).extra,
       (sa).city, (sa).state, (sa).country, (sa).postcode, (sa).box, (sa).unit
FROM (SELECT a.id, standardize_address('us_lex', 'us_gaz', 'us_rules',
  concat_ws(', ', a.addr1, a.addr2), concat_ws(', ', a.city, a.state, a.zip)) AS sa
FROM addr a JOIN diff_ids d ON a.id::text = d.id ORDER BY a.id) t;
" \
  -c "\\COPY pg_recheck TO '$PG_RECHECK_CSV' WITH (FORMAT csv, HEADER, NULL '')"

RECHECK_ROWS=$(tail -n +2 "$PG_RECHECK_CSV" | wc -l | tr -d ' ')
echo "Re-standardized $RECHECK_ROWS rows"

# ─── Step 3: Compare recheck vs DuckDB ──────────────────────────
echo ""
echo "=== Step 3: Comparing PG recheck vs DuckDB ==="

$DUCKDB_CLI <<SQL
CREATE TABLE pg_re AS SELECT * FROM read_csv('$PG_RECHECK_CSV', all_varchar=true, header=true, null_padding=true);
CREATE TABLE dk AS SELECT * FROM read_csv('$DK_CSV', all_varchar=true, header=true, null_padding=true);

-- Summary: how many of the original diffs are now resolved?
SELECT
  count(*) AS total_rechecked,
  count(*) FILTER (
    WHERE p.building  IS NOT DISTINCT FROM d.building
      AND p.house_num IS NOT DISTINCT FROM d.house_num
      AND p.predir    IS NOT DISTINCT FROM d.predir
      AND p.qual      IS NOT DISTINCT FROM d.qual
      AND p.pretype   IS NOT DISTINCT FROM d.pretype
      AND p.name      IS NOT DISTINCT FROM d.name
      AND p.suftype   IS NOT DISTINCT FROM d.suftype
      AND p.sufdir    IS NOT DISTINCT FROM d.sufdir
      AND p.ruralroute IS NOT DISTINCT FROM d.ruralroute
      AND p.extra     IS NOT DISTINCT FROM d.extra
      AND p.city      IS NOT DISTINCT FROM d.city
      AND p.state     IS NOT DISTINCT FROM d.state
      AND p.country   IS NOT DISTINCT FROM d.country
      AND p.postcode  IS NOT DISTINCT FROM d.postcode
      AND p.box       IS NOT DISTINCT FROM d.box
      AND p.unit      IS NOT DISTINCT FROM d.unit
  ) AS now_matching,
  count(*) - count(*) FILTER (
    WHERE p.building  IS NOT DISTINCT FROM d.building
      AND p.house_num IS NOT DISTINCT FROM d.house_num
      AND p.predir    IS NOT DISTINCT FROM d.predir
      AND p.qual      IS NOT DISTINCT FROM d.qual
      AND p.pretype   IS NOT DISTINCT FROM d.pretype
      AND p.name      IS NOT DISTINCT FROM d.name
      AND p.suftype   IS NOT DISTINCT FROM d.suftype
      AND p.sufdir    IS NOT DISTINCT FROM d.sufdir
      AND p.ruralroute IS NOT DISTINCT FROM d.ruralroute
      AND p.extra     IS NOT DISTINCT FROM d.extra
      AND p.city      IS NOT DISTINCT FROM d.city
      AND p.state     IS NOT DISTINCT FROM d.state
      AND p.country   IS NOT DISTINCT FROM d.country
      AND p.postcode  IS NOT DISTINCT FROM d.postcode
      AND p.box       IS NOT DISTINCT FROM d.box
      AND p.unit      IS NOT DISTINCT FROM d.unit
  ) AS still_differing
FROM pg_re p
JOIN dk d ON p.id = d.id;

-- Per-field breakdown of remaining diffs (if any)
SELECT
  count(*) FILTER (WHERE p.building  IS DISTINCT FROM d.building)  AS building_diff,
  count(*) FILTER (WHERE p.house_num IS DISTINCT FROM d.house_num) AS house_num_diff,
  count(*) FILTER (WHERE p.predir    IS DISTINCT FROM d.predir)    AS predir_diff,
  count(*) FILTER (WHERE p.qual      IS DISTINCT FROM d.qual)      AS qual_diff,
  count(*) FILTER (WHERE p.pretype   IS DISTINCT FROM d.pretype)   AS pretype_diff,
  count(*) FILTER (WHERE p.name      IS DISTINCT FROM d.name)      AS name_diff,
  count(*) FILTER (WHERE p.suftype   IS DISTINCT FROM d.suftype)   AS suftype_diff,
  count(*) FILTER (WHERE p.sufdir    IS DISTINCT FROM d.sufdir)    AS sufdir_diff,
  count(*) FILTER (WHERE p.city      IS DISTINCT FROM d.city)      AS city_diff,
  count(*) FILTER (WHERE p.state     IS DISTINCT FROM d.state)     AS state_diff,
  count(*) FILTER (WHERE p.postcode  IS DISTINCT FROM d.postcode)  AS postcode_diff,
  count(*) FILTER (WHERE p.unit      IS DISTINCT FROM d.unit)      AS unit_diff
FROM pg_re p
JOIN dk d ON p.id = d.id;

-- Show remaining diffs (if any)
SELECT p.id,
  CASE WHEN p.house_num IS DISTINCT FROM d.house_num
       THEN 'PG=' || coalesce(p.house_num,'NULL') || ' DK=' || coalesce(d.house_num,'NULL') END AS house_num,
  CASE WHEN p.pretype IS DISTINCT FROM d.pretype
       THEN 'PG=' || coalesce(p.pretype,'NULL') || ' DK=' || coalesce(d.pretype,'NULL') END AS pretype,
  CASE WHEN p.name IS DISTINCT FROM d.name
       THEN 'PG=' || coalesce(p.name,'NULL') || ' DK=' || coalesce(d.name,'NULL') END AS name,
  CASE WHEN p.suftype IS DISTINCT FROM d.suftype
       THEN 'PG=' || coalesce(p.suftype,'NULL') || ' DK=' || coalesce(d.suftype,'NULL') END AS suftype,
  CASE WHEN p.city IS DISTINCT FROM d.city
       THEN 'PG=' || coalesce(p.city,'NULL') || ' DK=' || coalesce(d.city,'NULL') END AS city,
  CASE WHEN p.state IS DISTINCT FROM d.state
       THEN 'PG=' || coalesce(p.state,'NULL') || ' DK=' || coalesce(d.state,'NULL') END AS state
FROM pg_re p
JOIN dk d ON p.id = d.id
WHERE p.building  IS DISTINCT FROM d.building
   OR p.house_num IS DISTINCT FROM d.house_num
   OR p.predir    IS DISTINCT FROM d.predir
   OR p.qual      IS DISTINCT FROM d.qual
   OR p.pretype   IS DISTINCT FROM d.pretype
   OR p.name      IS DISTINCT FROM d.name
   OR p.suftype   IS DISTINCT FROM d.suftype
   OR p.sufdir    IS DISTINCT FROM d.sufdir
   OR p.city      IS DISTINCT FROM d.city
   OR p.state     IS DISTINCT FROM d.state
   OR p.postcode  IS DISTINCT FROM d.postcode
   OR p.unit      IS DISTINCT FROM d.unit
LIMIT 20;
SQL

echo ""
echo "=== Recheck complete ==="
echo "If all diffs resolved: confirms PAGC state leakage (not code bugs)."
echo "If diffs remain: those are real code differences needing investigation."
echo ""
echo "Files:"
echo "  Diff IDs:        $DIFF_IDS"
echo "  PG recheck CSV:  $PG_RECHECK_CSV"
