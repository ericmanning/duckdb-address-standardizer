#!/usr/bin/env bash
# Export full standardization results from both engines and compare per-field.
source "$(dirname "$0")/../config.sh"

PG_CSV="$RESULTS_DIR/results_pg.csv"
DK_CSV="$RESULTS_DIR/results_duckdb.csv"

echo "=== Exporting Results ==="

# ─── PostgreSQL: ensure PARALLEL UNSAFE ──────────────────────
# The PAGC standardizer has internal mutable state in STAND_PARAM that leaks
# between rows. Parallel workers each cache their own STANDARDIZER but the
# non-deterministic row→worker assignment causes corrupted results (e.g.
# suftype from a prior row bleeding into the current row's name field).
echo "Ensuring standardize_address is PARALLEL UNSAFE..."
"$PSQL" -d "$PG_DB" <<'SQL'
ALTER FUNCTION standardize_address(text, text, text, text, text) PARALLEL UNSAFE;
ALTER FUNCTION standardize_address(text, text, text, text) PARALLEL UNSAFE;
SQL

# ─── PostgreSQL export (parallel connections) ─────────────────
echo ""
echo "Exporting PostgreSQL results using $PG_JOBS parallel connections..."

PG_START=$(python3 -c "import time; print(time.time())")

# Launch N parallel psql processes, each handling rows where hashtext(id) % N = i
PG_PIDS=()
for i in $(seq 0 $((PG_JOBS - 1))); do
  PG_PART="$RESULTS_DIR/results_pg_part${i}.csv"
  "$PSQL" -d "$PG_DB" -c "\\COPY (SELECT id, (sa).building, (sa).house_num, (sa).predir, (sa).qual, (sa).pretype, (sa).name, (sa).suftype, (sa).sufdir, (sa).ruralroute, (sa).extra, (sa).city, (sa).state, (sa).country, (sa).postcode, (sa).box, (sa).unit FROM (SELECT id, standardize_address('us_lex', 'us_gaz', 'us_rules', concat_ws(', ', addr1, addr2), concat_ws(', ', city, state, zip)) AS sa FROM addr WHERE abs(hashtext(id)) % $PG_JOBS = $i) t) TO '$PG_PART' WITH (FORMAT csv, NULL '')" &
  PG_PIDS+=($!)
done

# Wait for all workers and check for failures
PG_FAIL=0
for pid in "${PG_PIDS[@]}"; do
  wait "$pid" || PG_FAIL=1
done

PG_END=$(python3 -c "import time; print(time.time())")

if [ "$PG_FAIL" -eq 1 ]; then
  echo "ERROR: One or more PG export workers failed"
  exit 1
fi

# Merge partitions: write header, then cat all parts, sort by id
echo "id,building,house_num,predir,qual,pretype,name,suftype,sufdir,ruralroute,extra,city,state,country,postcode,box,unit" > "$PG_CSV"
cat "$RESULTS_DIR"/results_pg_part*.csv >> "$PG_CSV"
rm -f "$RESULTS_DIR"/results_pg_part*.csv

PG_TIME_S=$(python3 -c "print(f'{$PG_END - $PG_START:.3f}')")
printf "pg\tall\t1\t%s-conn\t%s\n" "$PG_JOBS" "$PG_TIME_S" >> "$RESULTS_DIR/timings.tsv"
echo "PG time: ${PG_TIME_S} s ($PG_JOBS parallel connections)"

PG_ROWS=$(wc -l < "$PG_CSV" | tr -d ' ')
echo "PG export: $PG_CSV ($((PG_ROWS - 1)) rows)"

# ─── DuckDB export ────────────────────────────────────────────
echo ""
echo "Exporting DuckDB results..."
$DUCKDB_CLI "$DUCKDB_DB" <<SQL
LOAD '$DUCKDB_EXT';
COPY (
  SELECT id, sa.*
  FROM (
    SELECT id, standardize_address('us_lex', 'us_gaz', 'us_rules',
      concat_ws(', ', addr1, addr2),
      concat_ws(', ', city, state, zip)) AS sa
    FROM addr ORDER BY id
  )
) TO '$DK_CSV' (HEADER, DELIMITER ',', NULL '');
SQL
DK_ROWS=$(wc -l < "$DK_CSV" | tr -d ' ')
echo "DuckDB export: $DK_CSV ($((DK_ROWS - 1)) rows)"

# ─── Compare ─────────────────────────────────────────────────
echo ""
echo "=== Comparing Results ==="

$DUCKDB_CLI <<SQL
CREATE TABLE pg_res AS SELECT * FROM read_csv('$PG_CSV', all_varchar=true, header=true, null_padding=true);
CREATE TABLE dk_res AS SELECT * FROM read_csv('$DK_CSV', all_varchar=true, header=true, null_padding=true);

SELECT count(*) AS pg_rows FROM pg_res;
SELECT count(*) AS dk_rows FROM dk_res;

-- Per-field mismatch counts
SELECT
  count(*) AS total_joined,
  count(*) FILTER (WHERE p.building IS DISTINCT FROM d.building)   AS building_diff,
  count(*) FILTER (WHERE p.house_num IS DISTINCT FROM d.house_num) AS house_num_diff,
  count(*) FILTER (WHERE p.predir IS DISTINCT FROM d.predir)       AS predir_diff,
  count(*) FILTER (WHERE p.qual IS DISTINCT FROM d.qual)           AS qual_diff,
  count(*) FILTER (WHERE p.pretype IS DISTINCT FROM d.pretype)     AS pretype_diff,
  count(*) FILTER (WHERE p.name IS DISTINCT FROM d.name)           AS name_diff,
  count(*) FILTER (WHERE p.suftype IS DISTINCT FROM d.suftype)     AS suftype_diff,
  count(*) FILTER (WHERE p.sufdir IS DISTINCT FROM d.sufdir)       AS sufdir_diff,
  count(*) FILTER (WHERE p.ruralroute IS DISTINCT FROM d.ruralroute) AS ruralroute_diff,
  count(*) FILTER (WHERE p.extra IS DISTINCT FROM d.extra)         AS extra_diff,
  count(*) FILTER (WHERE p.city IS DISTINCT FROM d.city)           AS city_diff,
  count(*) FILTER (WHERE p.state IS DISTINCT FROM d.state)         AS state_diff,
  count(*) FILTER (WHERE p.country IS DISTINCT FROM d.country)     AS country_diff,
  count(*) FILTER (WHERE p.postcode IS DISTINCT FROM d.postcode)   AS postcode_diff,
  count(*) FILTER (WHERE p.box IS DISTINCT FROM d.box)             AS box_diff,
  count(*) FILTER (WHERE p.unit IS DISTINCT FROM d.unit)           AS unit_diff
FROM pg_res p
JOIN dk_res d ON p.id = d.id;

-- Sample mismatches (first 20)
SELECT p.id,
  CASE WHEN p.house_num IS DISTINCT FROM d.house_num
       THEN 'PG=' || coalesce(p.house_num,'NULL') || ' DK=' || coalesce(d.house_num,'NULL') END AS house_num,
  CASE WHEN p.name IS DISTINCT FROM d.name
       THEN 'PG=' || coalesce(p.name,'NULL') || ' DK=' || coalesce(d.name,'NULL') END AS name,
  CASE WHEN p.city IS DISTINCT FROM d.city
       THEN 'PG=' || coalesce(p.city,'NULL') || ' DK=' || coalesce(d.city,'NULL') END AS city,
  CASE WHEN p.state IS DISTINCT FROM d.state
       THEN 'PG=' || coalesce(p.state,'NULL') || ' DK=' || coalesce(d.state,'NULL') END AS state,
  CASE WHEN p.postcode IS DISTINCT FROM d.postcode
       THEN 'PG=' || coalesce(p.postcode,'NULL') || ' DK=' || coalesce(d.postcode,'NULL') END AS postcode,
  CASE WHEN p.country IS DISTINCT FROM d.country
       THEN 'PG=' || coalesce(p.country,'NULL') || ' DK=' || coalesce(d.country,'NULL') END AS country
FROM pg_res p
JOIN dk_res d ON p.id = d.id
WHERE p.house_num IS DISTINCT FROM d.house_num
   OR p.name IS DISTINCT FROM d.name
   OR p.city IS DISTINCT FROM d.city
   OR p.state IS DISTINCT FROM d.state
   OR p.postcode IS DISTINCT FROM d.postcode
   OR p.country IS DISTINCT FROM d.country
LIMIT 20;
SQL

echo ""
echo "=== Comparison complete ==="
echo "Full results: $PG_CSV and $DK_CSV"
echo "Timings: $RESULTS_DIR/timings.tsv"
