# DuckDB Address Standardizer Extension

A DuckDB extension providing US address parsing and standardization functions:

- **PAGC-based** (`parse_address`, `standardize_address`) — ported from the
  [PostGIS address_standardizer](https://github.com/postgis/address_standardizer)
- **addrust-based** (`addrust_parse`) — powered by the
  [addrust](https://github.com/EvictionLab/addrust) Rust address parser, with
  optional TOML configuration for customizable parsing pipelines

## Building

### Prerequisites

- C compiler (gcc/clang)
- CMake 3.5+
- Python 3 with venv
- [pcre2](https://github.com/PCRE2Project/pcre2) development headers
- [Rust](https://rustup.rs/) toolchain (for building the addrust component)
- Make, Git

#### Install pcre2

```bash
# macOS
brew install pcre2

# Debian/Ubuntu
sudo apt install libpcre2-dev

# RHEL/CentOS
sudo dnf install pcre2-devel
```

### Build

```bash
# Clone with submodules (for DuckDB CI tools)
git submodule update --init --recursive

# Configure (sets up Python venv, downloads DuckDB headers)
make configure

# Build debug
make debug

# Build release
make release
```

### Test

```bash
make test_debug
```

## Functions

### `addrust_parse(address VARCHAR) -> STRUCT`

Configurable Rust-based address parser powered by [addrust](https://github.com/EvictionLab/addrust).
Returns a struct with 15 fields:
`street_number`, `pre_direction`, `street_name`, `suffix`, `post_direction`,
`unit_type`, `unit`, `po_box`, `building`, `building_type`,
`extra_front`, `extra_back`, `city`, `state`, `zip`

### `addrust_parse(address VARCHAR, config_path VARCHAR) -> STRUCT`

Same as above but loads a custom TOML configuration file to control the parsing pipeline
(disable steps, change output formats, add dictionary entries, etc.).
See [addrust documentation](https://github.com/EvictionLab/addrust) for config options.

### `parse_address(address VARCHAR) -> STRUCT`

Simple regex-based address parser (PAGC). Returns a struct with fields:
`num`, `street`, `street2`, `address1`, `city`, `state`, `zip`, `zipplus`, `country`

### `standardize_address(lextab, gaztab, rultab, micro, macro) -> STRUCT`

Full PAGC rule-based standardization (5-argument form). Requires reference tables loaded
via `load_us_address_data()`. Returns a struct with fields:
`building`, `house_num`, `predir`, `qual`, `pretype`, `name`, `suftype`, `sufdir`,
`ruralroute`, `extra`, `city`, `state`, `country`, `postcode`, `box`, `unit`

### `standardize_address(lextab, gaztab, rultab, address) -> STRUCT`

Single-line variant (4-argument form). Parses the address first, then standardizes.

### `load_us_address_data([schema]) -> VARCHAR`

Creates and populates `us_lex`, `us_gaz`, and `us_rules` reference tables from data
embedded in the extension binary. Optional schema argument controls where tables are created
(defaults to the current schema).

### `debug_standardize_address(...) -> VARCHAR`

Same signatures as `standardize_address`. Returns a human-readable debug trace instead of a struct.

## Usage

```sql
LOAD 'address_standardizer';

-- ─── addrust parser (no reference tables needed) ───────────────

-- Parse an address with the default pipeline
SELECT ap.*
FROM (SELECT addrust_parse('123 N Main St Apt 4, Springfield IL 62704') AS ap);

-- Parse with a custom TOML config file
SELECT ap.*
FROM (SELECT addrust_parse('123 N Main St', '/path/to/.addrust.toml') AS ap);

-- ─── PAGC standardizer (requires reference tables) ────────────

-- Load built-in US reference data (one-time per database)
SELECT load_us_address_data();

-- Simple address parsing (regex-based, no reference tables)
SELECT pa.*
FROM (SELECT parse_address('123 Main Street, Kansas City, MO 45678') AS pa);

-- Full standardization (5-arg: micro + macro)
SELECT sa.*
FROM (SELECT standardize_address('us_lex', 'us_gaz', 'us_rules',
    '123 Main Street', 'Kansas City, MO 45678') AS sa);

-- Single-line version (4-arg)
SELECT sa.*
FROM (SELECT standardize_address('us_lex', 'us_gaz', 'us_rules',
    '123 Main Street, Kansas City, MO 45678') AS sa);
```

## Platform Support


| Platform | Status |
|----------|--------|
| Linux (amd64, arm64) | Supported |
| macOS (arm64) | Supported |
| macOS (amd64) | Not yet — CI cross-compilation gap for C API extensions |
| Windows (MinGW) | Supported |
| Windows (MSVC) | Not yet — needs `pthread_mutex` → `CRITICAL_SECTION` abstraction |
| WASM | Not supported — requires threading primitives unavailable in WASM |

## License

Portions of this code belong to their respective contributors. The upstream PostGIS address standardizer on which this extension is built is released under the MIT license. The `addrust` parser is released under the MIT license. See [LICENSE](LICENSE) for attributions.

Modifications in this extension and the forked PostGIS submodule are

Copyright (c) 2026 The Trustees of Princeton University