cmake_minimum_required(VERSION 3.5...3.29)
option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)

###
# Configuration
###
if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()

add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

###
# Build
###
project(${EXTENSION_NAME} LANGUAGES C)

# PAGC core source files (portable C, no PostgreSQL dependencies)
set(PAGC_SOURCES
    address-standardizer/src/analyze.c
    address-standardizer/src/err_param.c
    address-standardizer/src/export.c
    address-standardizer/src/gamma.c
    address-standardizer/src/hash.c
    address-standardizer/src/lexicon.c
    address-standardizer/src/pagc_tools.c
    address-standardizer/src/standard.c
    address-standardizer/src/tokenize.c
    address-standardizer/src/parseaddress-api.c
)

# DuckDB extension source files
set(EXTENSION_SOURCES
    src/address_standardizer_duckdb.c
    src/debug_standardize_address_duckdb.c
    src/parseaddress_duckdb.c
    src/load_us_data_duckdb.c
    src/addrust_parse_duckdb.c
    src/duckdb_entry.c
)

if (DUCKDB_WASM_EXTENSION)
    add_library(${EXTENSION_NAME} STATIC ${PAGC_SOURCES} ${EXTENSION_SOURCES})
else()
    add_library(${EXTENSION_NAME} SHARED ${PAGC_SOURCES} ${EXTENSION_SOURCES})
endif()

# Hide symbols
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")

# Compile flags: use PCRE2, provide a version string
target_compile_definitions(${EXTENSION_NAME} PRIVATE
    DUCKDB_BUILD
    BUILD_API
    PCRE_VERSION=2
    AS_VERSION="4.0.0"
)

# Include paths
target_include_directories(${EXTENSION_NAME} PRIVATE
    src/include                    # DuckDB-specific headers (postgres.h shim, etc.)
    src/generated                  # Auto-generated data headers
    address-standardizer/src       # Original PAGC headers
    duckdb_capi                    # DuckDB C API header
)

# Find and link pcre2: try vcpkg first, then system fallback
find_package(unofficial-pcre2 CONFIG QUIET)
if(unofficial-pcre2_FOUND)
    target_link_libraries(${EXTENSION_NAME} unofficial::pcre2::pcre2-8)
else()
    find_library(PCRE2_LIB pcre2-8)
    find_path(PCRE2_INCLUDE_DIR pcre2.h)

    if(PCRE2_LIB AND PCRE2_INCLUDE_DIR)
        target_include_directories(${EXTENSION_NAME} PRIVATE ${PCRE2_INCLUDE_DIR})
        target_link_libraries(${EXTENSION_NAME} ${PCRE2_LIB})
    elseif(PCRE2_LIB)
        # Library found but not header — try pkg-config for include path
        find_package(PkgConfig)
        if(PkgConfig_FOUND)
            pkg_check_modules(PCRE2 REQUIRED libpcre2-8)
            target_include_directories(${EXTENSION_NAME} PRIVATE ${PCRE2_INCLUDE_DIRS})
        endif()
        target_link_libraries(${EXTENSION_NAME} ${PCRE2_LIB})
    else()
        # Try pkg-config for both
        find_package(PkgConfig)
        if(PkgConfig_FOUND)
            pkg_check_modules(PCRE2 REQUIRED libpcre2-8)
            target_include_directories(${EXTENSION_NAME} PRIVATE ${PCRE2_INCLUDE_DIRS})
            target_link_libraries(${EXTENSION_NAME} ${PCRE2_LIBRARIES})
        else()
            target_link_libraries(${EXTENSION_NAME} pcre2-8)
        endif()
    endif()
endif()

# --- Rust (addrust-ffi) integration ---
set(ADDRUST_FFI_DIR ${CMAKE_CURRENT_SOURCE_DIR}/addrust-ffi)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CARGO_BUILD_TYPE release)
    set(CARGO_BUILD_FLAG --release)
else()
    set(CARGO_BUILD_TYPE debug)
    set(CARGO_BUILD_FLAG "")
endif()

# Static lib name varies by platform/toolchain
set(ADDRUST_LIB_NAME libaddrust_ffi.a)

# Cross-compilation: map DUCKDB_PLATFORM to Cargo --target.
# CI passes DUCKDB_PLATFORM as an env var; read it into a CMake variable.
if(NOT DEFINED DUCKDB_PLATFORM AND DEFINED ENV{DUCKDB_PLATFORM})
    set(DUCKDB_PLATFORM $ENV{DUCKDB_PLATFORM})
endif()

set(CARGO_TARGET_FLAG "")
set(CARGO_TARGET_DIR ${ADDRUST_FFI_DIR}/target)
if(DEFINED DUCKDB_PLATFORM)
    if(DUCKDB_PLATFORM STREQUAL "osx_arm64")
        set(CARGO_TARGET_FLAG --target aarch64-apple-darwin)
        set(CARGO_TARGET_DIR ${ADDRUST_FFI_DIR}/target/aarch64-apple-darwin)
    elseif(DUCKDB_PLATFORM STREQUAL "osx_amd64")
        set(CARGO_TARGET_FLAG --target x86_64-apple-darwin)
        set(CARGO_TARGET_DIR ${ADDRUST_FFI_DIR}/target/x86_64-apple-darwin)
    elseif(DUCKDB_PLATFORM STREQUAL "linux_arm64")
        set(CARGO_TARGET_FLAG --target aarch64-unknown-linux-gnu)
        set(CARGO_TARGET_DIR ${ADDRUST_FFI_DIR}/target/aarch64-unknown-linux-gnu)
    elseif(DUCKDB_PLATFORM MATCHES "windows_amd64_mingw|windows_amd64_rtools")
        set(CARGO_TARGET_FLAG --target x86_64-pc-windows-gnu)
        set(CARGO_TARGET_DIR ${ADDRUST_FFI_DIR}/target/x86_64-pc-windows-gnu)
    endif()
endif()

set(ADDRUST_LIB ${CARGO_TARGET_DIR}/${CARGO_BUILD_TYPE}/${ADDRUST_LIB_NAME})

add_custom_command(
    OUTPUT ${ADDRUST_LIB}
    COMMAND cargo build ${CARGO_BUILD_FLAG} ${CARGO_TARGET_FLAG}
    WORKING_DIRECTORY ${ADDRUST_FFI_DIR}
    COMMENT "Building addrust-ffi Rust static library"
)
add_custom_target(addrust_ffi_build DEPENDS ${ADDRUST_LIB})

add_dependencies(${EXTENSION_NAME} addrust_ffi_build)
target_include_directories(${EXTENSION_NAME} PRIVATE ${ADDRUST_FFI_DIR}/include)
target_link_libraries(${EXTENSION_NAME} ${ADDRUST_LIB})

# Rust staticlibs require system libraries
if(APPLE)
    target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
elseif(WIN32)
    target_link_libraries(${EXTENSION_NAME} ntdll userenv ws2_32 bcrypt)
elseif(UNIX)
    target_link_libraries(${EXTENSION_NAME} pthread dl m)
endif()
