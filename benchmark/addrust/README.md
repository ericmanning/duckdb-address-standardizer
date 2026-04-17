# Addrust Benchmark: CLI vs DuckDB Extension

Compares the **addrust CLI tool** against the **DuckDB `addrust_parse()` extension function** to verify that both produce identical output. Since they use the same underlying Rust library, any difference is a bug in the FFI/wrapper layer.

Tests both the **default pipeline** and a **custom TOML config** (exercising the 2-arg `addrust_parse(address, config_path)` overload).

## Quick Start

```bash
export PARQUET_FILE=/path/to/your/addresses.parquet
cd benchmark/addrust

./01_setup.sh           # Build CLI + extension, create DuckDB database
./02_bench_cli.sh       # Parse all rows via addrust CLI (default + custom)
./03_bench_extension.sh # Parse all rows via DuckDB extension (default + custom)
./04_compare.sh         # Per-field diff counts between CLI and extension
```

## Scripts

| Script | What it does |
|--------|--------------|
| `01_setup.sh` | Builds addrust CLI (release), builds DuckDB extension, creates `addrust_bench.duckdb` with a single `address` column concatenated from addr1/addr2/city/state/zip. |
| `02_bench_cli.sh` | Runs `addrust parse --duckdb` on the address table with default and custom configs. Outputs timing. |
| `03_bench_extension.sh` | Runs `addrust_parse()` via the DuckDB extension with default and custom configs. Outputs timing. |
| `04_compare.sh` | Joins CLI and extension results by id and reports per-field mismatch counts + sample diffs. |

## Custom Config (`config_short_suffix.toml`)

The custom TOML exercises config features to ensure the `config_path` overload works correctly at scale:

| Change | Default | Custom | Why |
|--------|---------|--------|-----|
| `suffix` output | long (`STREET`) | short (`ST`) | Most common config change — tests output formatting |
| `direction` output | short (`N`) | long (`NORTH`) | Tests direction expansion |
| `state` output | short (`IL`) | long (`ILLINOIS`) | Tests state expansion across millions of rows |
| Disabled step: `ordinal_to_word` | enabled (`42ND` → `FORTYSECOND`) | disabled (`42ND` stays `42ND`) | Tests step disabling — expect differences in ordinal streets |
| Custom suffix: `PSGE` → `PASSAGE` | not recognized | recognized | Tests dictionary patching — low frequency but verifies the feature |

## Output

Results are written into the shared `/tmp/bench_results/addrust_bench.duckdb` database:

| Table | Source | Config |
|-------|--------|--------|
| `addr` | Raw addresses (id + concatenated address text) | — |
| `addr_cli_default` | addrust CLI, default config | default |
| `addr_cli_custom` | addrust CLI, custom TOML | `config_short_suffix.toml` |
| `addr_ext_default` | DuckDB `addrust_parse(address)` | default |
| `addr_ext_custom` | DuckDB `addrust_parse(address, config_path)` | `config_short_suffix.toml` |

## Expected Results

- **Default config**: 0 diffs across all 15 fields. Any diff is an FFI bug.
- **Custom config**: 0 diffs across all 15 fields. Any diff means the config_path overload doesn't match the CLI's config loading behavior.
