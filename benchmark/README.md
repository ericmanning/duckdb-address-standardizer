# Address Standardizer Benchmark: DuckDB vs PostgreSQL

Compares performance and output correctness of `standardize_address()` between the DuckDB C extension and the PostgreSQL extension (built from the `address-standardizer` submodule).

## Prerequisites

- **macOS** with Homebrew
- **PostgreSQL 18.x** (`brew install postgresql@18`)
- **PCRE2** (`brew install pcre2`)
- **DuckDB CLI** (`brew install duckdb`)
- A **parquet file** with columns: `id`, `addr1`, `addr2`, `city`, `state`, `zip` (all text)

## Quick Start

```bash
export PARQUET_FILE=/path/to/your/addresses.parquet
cd benchmark

# 1. One-time setup
./00_setup_pg.sh
./01_setup_duckdb.sh

# 2. Load data into both engines
./02_load_data.sh

# 3. Sanity check
./03_verify.sh

# 4. Run benchmarks
./04_bench_pg.sh
./05_bench_duckdb.sh

# 5. Compare outputs
./06_export_and_compare.sh
```

## Scripts

| Script | What it does |
|--------|--------------|
| `config.sh` | Shared configuration — paths, benchmark sizes, run count, thread counts. Sourced by all other scripts. |
| `00_setup_pg.sh` | Installs PG 18 if needed, builds the PG extension from the `address-standardizer` submodule, creates the `address_bench` database with `address_standardizer` and `address_standardizer_data_us` extensions. |
| `01_setup_duckdb.sh` | Builds the DuckDB extension (release). |
| `02_load_data.sh` | Converts parquet → CSV → PostgreSQL via `\COPY`. Loads parquet directly into DuckDB and calls `load_us_address_data()` for reference data. |
| `03_verify.sh` | Runs a known address and the first row from the data file through both engines. Inspect the output to confirm they match. |
| `04_bench_pg.sh` | Benchmarks PG at each configured size, repeated `RUNS` times. Parses `\timing` output and logs to `timings.tsv`. |
| `05_bench_duckdb.sh` | Benchmarks DuckDB at each configured size × thread count (1, 4, 8, default), repeated `RUNS` times. Logs to the same `timings.tsv`. |
| `06_export_and_compare.sh` | Exports full results from both engines to CSV, then runs a per-field mismatch count and prints sample differences. |

## Configuration

Edit `config.sh` to adjust:

```bash
SIZES=(1000 10000 100000 0)   # Row counts to benchmark; 0 = all rows
RUNS=3                         # Repetitions per configuration
DUCKDB_THREAD_COUNTS=(1 4 8 0) # 0 = default (all cores)
PG_CONFIG="/opt/homebrew/opt/postgresql@18/bin/pg_config"
```

## Output

All results go to `/tmp/bench_results/`:

- **`timings.tsv`** — one row per benchmark run:
  ```
  engine	size	run	threads	seconds
  pg	1000	1	1	0.456
  duckdb	1000	1	4	0.198
  ```
- **`results_pg.csv`** / **`results_duckdb.csv`** — full standardization output for every row (produced by `06_export_and_compare.sh`)

## Architecture Notes

- **PostgreSQL** runs `standardize_address()` single-threaded — the function is `IMMUTABLE STRICT` but not `PARALLEL SAFE`.
- **DuckDB** uses a two-pass architecture per chunk:
  1. **Pass 1** (parallel): regex-based macro parsing (`parseaddress()`) runs across all worker threads.
  2. **Pass 2** (serialized): PAGC standardization under a mutex.
- The DuckDB thread sweep (1/4/8/all) shows how much pass-1 parallelism helps before hitting the serialized pass-2 bottleneck (Amdahl's law).
- Both engines cache the PAGC standardizer (built from lex/gaz/rules) after the first call. The first run in a session includes this initialization cost; subsequent runs reuse the cached state. The scripts report all runs so you can see the warm-up effect.

## Troubleshooting

- **`pg_config` not found**: Ensure PG 18 bin is on PATH or edit `PG_CONFIG` in `config.sh`.
- **`PARQUET_FILE` error**: You must `export PARQUET_FILE=...` before running any script.
- **Extension build fails**: Check that `pcre2` is installed (`brew install pcre2`).
- **PG connection refused**: Run `brew services start postgresql@18` or check `pg_isready`.
- **Timing parse errors**: The scripts extract timing from psql `\timing` and DuckDB `.timer on` output. If your locale uses commas for decimals, timings may not parse correctly — set `LC_NUMERIC=C` before running.
