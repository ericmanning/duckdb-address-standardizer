# Address Standardizer Benchmarks

Two benchmark suites for validating the DuckDB address standardizer extension against reference implementations on real-world data.

## Structure

```
benchmark/
├── config.sh              # Shared configuration (paths, sizes, thread counts)
├── README.md              # This file
├── postgis/               # DuckDB standardize_address() vs PostgreSQL
│   ├── 00_setup_pg.sh     # Install PG extension
│   ├── 01_setup_duckdb.sh # Build DuckDB extension
│   ├── 02_load_data.sh    # Load parquet into both engines
│   ├── 03_verify.sh       # Sanity check
│   ├── 04_bench_pg.sh     # Benchmark PostgreSQL
│   ├── 05_bench_duckdb.sh # Benchmark DuckDB
│   ├── 06_export_and_compare.sh  # Full output comparison
│   └── 07_recheck_diffs.sh       # Isolate PAGC state-leakage
└── addrust/               # addrust CLI vs DuckDB addrust_parse()
    ├── config_short_suffix.toml  # Custom config for 2-arg tests
    ├── 01_setup.sh         # Build CLI + extension, create database
    ├── 02_bench_cli.sh     # Parse via addrust CLI
    ├── 03_bench_extension.sh  # Parse via DuckDB extension
    ├── 04_compare.sh       # Per-field diff comparison
    └── README.md           # Addrust benchmark details
```

## Prerequisites

- **macOS** with Homebrew
- **DuckDB CLI** (`brew install duckdb`)
- **Rust** toolchain (for building addrust)
- A **parquet file** with columns: `id`, `addr1`, `addr2`, `city`, `state`, `zip` (all text)
- For PostGIS benchmarks: **PostgreSQL 18.x** (`brew install postgresql@18`), **PCRE2** (`brew install pcre2`)

## Quick Start

```bash
export PARQUET_FILE=/path/to/your/addresses.parquet

# --- Addrust: CLI vs DuckDB extension ---
cd benchmark/addrust
./01_setup.sh
./02_bench_cli.sh
./03_bench_extension.sh
./04_compare.sh

# --- PostGIS: DuckDB vs PostgreSQL ---
cd ../postgis
./00_setup_pg.sh
./01_setup_duckdb.sh
./02_load_data.sh
./03_verify.sh
./04_bench_pg.sh
./05_bench_duckdb.sh
./06_export_and_compare.sh
./07_recheck_diffs.sh
```

## Configuration

Edit `config.sh` to adjust shared settings:

```bash
SIZES=(1000 10000 100000 0)    # Row counts to benchmark; 0 = all rows
RUNS=3                          # Repetitions per configuration
DUCKDB_THREAD_COUNTS=(1 4 8 0) # 0 = default (all cores)
PG_CONFIG="/opt/homebrew/opt/postgresql@18/bin/pg_config"
```

## Output

All results go to `/tmp/bench_results/`.

See each suite's README for details on output tables and expected results.
