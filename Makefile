.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Main extension configuration
EXTENSION_NAME=us_address_standardizer

# Set to 1 to enable Unstable API
USE_UNSTABLE_C_API=0

# The stable C Extension API version (what gets stamped into the extension metadata).
# Stable C API has been frozen at v1.2.0 since DuckDB 1.2.0 — pinning here maximizes
# forward-compatibility with future DuckDB releases.
TARGET_DUCKDB_VERSION=v1.2.0

# DuckDB release to download headers from (may be newer than the API version).
DUCKDB_HEADER_VERSION=v1.5.2

all: configure release

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

# Override header download URL to use the latest release headers.
BASE_HEADER_URL=https://raw.githubusercontent.com/duckdb/duckdb/$(DUCKDB_HEADER_VERSION)/src/include
DUCKDB_C_HEADER_URL=$(BASE_HEADER_URL)/duckdb.h
DUCKDB_C_EXTENSION_HEADER_URL=$(BASE_HEADER_URL)/duckdb_extension.h

configure: venv platform extension_version fetch_headers

fetch_headers:
	@mkdir -p duckdb_capi
	$(MAKE) update_duckdb_headers

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean clean_configure
