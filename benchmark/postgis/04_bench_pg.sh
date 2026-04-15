#!/usr/bin/env bash
# Benchmark standardize_address on PostgreSQL.
# Runs each size x RUNS times, logs wall-clock timings to timings.tsv.
source "$(dirname "$0")/../config.sh"

TIMINGS_FILE="$RESULTS_DIR/timings.tsv"

# Write header if file doesn't exist
if [ ! -f "$TIMINGS_FILE" ]; then
  printf "engine\tsize\trun\tthreads\tseconds\n" > "$TIMINGS_FILE"
fi

echo "=== PostgreSQL Benchmark ==="
echo "Sizes: ${SIZES[*]}"
echo "Runs per size: $RUNS"
echo ""

for size in "${SIZES[@]}"; do
  if [ "$size" -eq 0 ]; then
    echo "--- Skipping full dataset (use 06_export_and_compare.sh with parallel PG) ---"
    echo ""
    continue
  else
    LIMIT_CLAUSE="WHERE id IN (SELECT id FROM addr LIMIT $size)"
    LABEL="$size"
  fi

  echo "--- Size: $LABEL ---"

  for run in $(seq 1 "$RUNS"); do
    # Run the query, capture timing from psql output
    OUTPUT=$("$PSQL" -d "$PG_DB" 2>&1 <<SQL
\\timing on
CREATE TEMP TABLE IF NOT EXISTS _bench_result AS SELECT 1 WHERE false;
DROP TABLE _bench_result;
CREATE TEMP TABLE _bench_result AS
SELECT id, (sa).*
FROM (
  SELECT id, standardize_address('us_lex', 'us_gaz', 'us_rules',
    concat_ws(', ', addr1, addr2),
    concat_ws(', ', city, state, zip)) AS sa
  FROM addr $LIMIT_CLAUSE
) t;
DROP TABLE _bench_result;
SQL
    )

    # Extract the CREATE TABLE AS SELECT timing (3rd "Time:" line)
    TIME_MS=$(echo "$OUTPUT" | grep "Time:" | sed -n '3p' | grep -oE '[0-9]+\.[0-9]+' | head -1)

    if [ -z "$TIME_MS" ]; then
      echo "  Run $run: ERROR — could not parse timing"
      echo "$OUTPUT"
      continue
    fi

    TIME_S=$(python3 -c "print(f'{$TIME_MS / 1000:.3f}')")
    printf "pg\t%s\t%d\t1\t%s\n" "$LABEL" "$run" "$TIME_S" >> "$TIMINGS_FILE"
    echo "  Run $run: ${TIME_MS} ms (${TIME_S} s)"
  done
  echo ""
done

echo "=== PG benchmark complete. Results in $TIMINGS_FILE ==="
