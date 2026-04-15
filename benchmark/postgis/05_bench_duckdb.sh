#!/usr/bin/env bash
# Benchmark standardize_address on DuckDB.
# Runs each (size x thread_count x RUNS), logs wall-clock timings to timings.tsv.
source "$(dirname "$0")/../config.sh"

TIMINGS_FILE="$RESULTS_DIR/timings.tsv"

# Write header if file doesn't exist
if [ ! -f "$TIMINGS_FILE" ]; then
  printf "engine\tsize\trun\tthreads\tseconds\n" > "$TIMINGS_FILE"
fi

echo "=== DuckDB Benchmark ==="
echo "Sizes: ${SIZES[*]}"
echo "Thread counts: ${DUCKDB_THREAD_COUNTS[*]}"
echo "Runs per config: $RUNS"
echo ""

for size in "${SIZES[@]}"; do
  if [ "$size" -eq 0 ]; then
    LIMIT_CLAUSE=""
    LABEL="all"
  else
    LIMIT_CLAUSE="LIMIT $size"
    LABEL="$size"
  fi

  for threads in "${DUCKDB_THREAD_COUNTS[@]}"; do
    if [ "$threads" -eq 0 ]; then
      THREAD_CMD=""
      THREAD_LABEL="default"
    else
      THREAD_CMD="SET threads = $threads;"
      THREAD_LABEL="$threads"
    fi

    echo "--- Size: $LABEL, Threads: $THREAD_LABEL ---"

    for run in $(seq 1 "$RUNS"); do
      # Run query with .timer on, capture output
      OUTPUT=$($DUCKDB_CLI "$DUCKDB_DB" 2>&1 <<SQL
LOAD '$DUCKDB_EXT';
$THREAD_CMD
.timer on
CREATE TEMP TABLE _bench_result AS
SELECT id, sa.*
FROM (
  SELECT id, standardize_address('us_lex', 'us_gaz', 'us_rules',
    concat_ws(', ', addr1, addr2),
    concat_ws(', ', city, state, zip)) AS sa
  FROM addr $LIMIT_CLAUSE
);
DROP TABLE _bench_result;
SQL
      )

      # DuckDB prints "Run Time (s): real X.XXX user X.XXX sys X.XXX"
      # The first "Run Time" after .timer on is the CREATE TABLE (our query)
      TIME_S=$(echo "$OUTPUT" | grep "Run Time" | head -1 | grep -oE 'real [0-9.]+' | grep -oE '[0-9.]+')

      if [ -z "$TIME_S" ]; then
        echo "  Run $run: ERROR — could not parse timing"
        echo "$OUTPUT"
        continue
      fi

      printf "duckdb\t%s\t%d\t%s\t%s\n" "$LABEL" "$run" "$THREAD_LABEL" "$TIME_S" >> "$TIMINGS_FILE"
      echo "  Run $run: ${TIME_S} s"
    done
  done
  echo ""
done

echo "=== DuckDB benchmark complete. Results in $TIMINGS_FILE ==="
