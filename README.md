# DuckDB Address Standardizer Extension

A DuckDB extension providing `parse_address()`, `standardize_address()`, and
`load_us_address_data()` functions, ported from the
[PostGIS address_standardizer](https://github.com/postgis/address_standardizer).

## Prerequisites

- C compiler (gcc/clang)
- CMake 3.5+
- Python 3 with venv
- [pcre2](https://github.com/PCRE2Project/pcre2) development headers
- Make, Git

### Install pcre2

```bash
# macOS
brew install pcre2

# Debian/Ubuntu
sudo apt install libpcre2-dev

# RHEL/CentOS
sudo dnf install pcre2-devel
```

## Building

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

## Testing

```bash
make test_debug
```

## Usage

```sql
LOAD 'address_standardizer';

-- Simple address parsing (no reference tables needed)
-- Returns a STRUCT; use .* to unpack into columns
SELECT pa.*
FROM (SELECT parse_address('123 Main Street, Kansas City, MO 45678') AS pa);

-- Load built-in US reference data (one-time per database)
SELECT load_us_address_data();

-- Or load into a specific schema:
-- SELECT load_us_address_data('my_schema');

-- Full standardization (5-arg: micro + macro)
SELECT sa.*
FROM (SELECT standardize_address('us_lex', 'us_gaz', 'us_rules',
    '123 Main Street', 'Kansas City, MO 45678') AS sa);

-- Single-line version (4-arg)
SELECT sa.*
FROM (SELECT standardize_address('us_lex', 'us_gaz', 'us_rules',
    '123 Main Street, Kansas City, MO 45678') AS sa);
```

## Functions

### `parse_address(address VARCHAR) -> STRUCT`

Simple regex-based address parser. Returns a struct with fields:
`num`, `street`, `street2`, `address1`, `city`, `state`, `zip`, `zipplus`, `country`

### `standardize_address(lextab, gaztab, rultab, micro, macro) -> STRUCT`

Full PAGC rule-based standardization (5-argument form). Returns a struct with fields:
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
