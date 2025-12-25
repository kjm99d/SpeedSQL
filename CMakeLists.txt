cmake_minimum_required(VERSION 3.16)
project(SpeedSQL VERSION 0.1.0 LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build options
option(SPEEDSQL_BUILD_SHARED "Build shared library" ON)
option(SPEEDSQL_BUILD_STATIC "Build static library" ON)
option(SPEEDSQL_BUILD_TESTS "Build tests" ON)
option(SPEEDSQL_BUILD_EXAMPLES "Build examples" ON)
option(SPEEDSQL_BUILD_BENCHMARK "Build benchmarks" OFF)

# Platform detection
if(WIN32)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
    add_compile_definitions(NOMINMAX)
    add_compile_definitions(WIN32_LEAN_AND_MEAN)
endif()

# Compiler flags
if(MSVC)
    add_compile_options(/W4 /WX-)
    add_compile_options(/MP)  # Multi-process compilation
    add_compile_options(/Zc:__cplusplus)  # Correct __cplusplus macro
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
    add_compile_options(-fPIC)
endif()

# Release optimizations
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    if(MSVC)
        add_compile_options(/O2 /Ob2 /DNDEBUG)
    else()
        add_compile_options(-O3 -DNDEBUG)
    endif()
endif()

# Source files
set(SPEEDSQL_SOURCES
    src/core/database.cpp
    src/core/executor.cpp
    src/storage/file_io.cpp
    src/storage/buffer_pool.cpp
    src/storage/wal.cpp
    src/index/btree.cpp
    src/sql/lexer.cpp
    src/sql/parser.cpp
    src/util/hash.cpp
    src/util/value.cpp
    # Crypto module
    src/crypto/crypto_provider.cpp
    src/crypto/cipher_none.cpp
    src/crypto/cipher_aes.cpp
    src/crypto/cipher_aria.cpp
    src/crypto/cipher_seed.cpp
    src/crypto/cipher_chacha20.cpp
)

# Header files
set(SPEEDSQL_HEADERS
    include/speedsql.h
    include/speedsql_types.h
    include/speedsql_internal.h
    include/speedsql_crypto.h
    include/speedsql.hpp
)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Static library
if(SPEEDSQL_BUILD_STATIC)
    add_library(speedsql_static STATIC ${SPEEDSQL_SOURCES} ${SPEEDSQL_HEADERS})
    target_include_directories(speedsql_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_compile_definitions(speedsql_static PRIVATE SPEEDSQL_EXPORTS)
    set_target_properties(speedsql_static PROPERTIES
        OUTPUT_NAME speedsql
        ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
    )

    # Platform-specific libraries
    if(WIN32)
        target_link_libraries(speedsql_static PRIVATE)
    else()
        target_link_libraries(speedsql_static PRIVATE pthread)
    endif()
endif()

# Shared library
if(SPEEDSQL_BUILD_SHARED)
    add_library(speedsql_shared SHARED ${SPEEDSQL_SOURCES} ${SPEEDSQL_HEADERS})
    target_compile_definitions(speedsql_shared PRIVATE SPEEDSQL_EXPORTS)
    target_include_directories(speedsql_shared PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    set_target_properties(speedsql_shared PROPERTIES
        OUTPUT_NAME speedsql
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
    )

    if(WIN32)
        target_link_libraries(speedsql_shared PRIVATE)
    else()
        target_link_libraries(speedsql_shared PRIVATE pthread)
    endif()
endif()

# Examples
if(SPEEDSQL_BUILD_EXAMPLES)
    # Basic usage example
    add_executable(speedsql_example examples/basic_usage.cpp)
    target_link_libraries(speedsql_example PRIVATE speedsql_static)
    target_compile_definitions(speedsql_example PRIVATE SPEEDSQL_EXPORTS)
    set_target_properties(speedsql_example PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Encryption example
    add_executable(speedsql_crypto_example examples/encryption_example.cpp)
    target_link_libraries(speedsql_crypto_example PRIVATE speedsql_static)
    target_compile_definitions(speedsql_crypto_example PRIVATE SPEEDSQL_EXPORTS)
    set_target_properties(speedsql_crypto_example PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Custom cipher example
    add_executable(speedsql_custom_cipher examples/custom_cipher_example.cpp)
    target_link_libraries(speedsql_custom_cipher PRIVATE speedsql_static)
    target_compile_definitions(speedsql_custom_cipher PRIVATE SPEEDSQL_EXPORTS)
    set_target_properties(speedsql_custom_cipher PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # C++ wrapper example
    add_executable(speedsql_cpp_example examples/cpp_wrapper_example.cpp)
    target_link_libraries(speedsql_cpp_example PRIVATE speedsql_static)
    target_compile_definitions(speedsql_cpp_example PRIVATE SPEEDSQL_EXPORTS)
    set_target_properties(speedsql_cpp_example PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Tests
if(SPEEDSQL_BUILD_TESTS)
    enable_testing()

    add_executable(speedsql_test tests/test_main.cpp)
    target_link_libraries(speedsql_test PRIVATE speedsql_static)
    target_compile_definitions(speedsql_test PRIVATE SPEEDSQL_EXPORTS)
    set_target_properties(speedsql_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    add_test(NAME SpeedSQLTest COMMAND speedsql_test)
endif()

# Benchmarks
if(SPEEDSQL_BUILD_BENCHMARK)
    add_executable(speedsql_bench benchmark/bench_main.cpp)
    target_link_libraries(speedsql_bench PRIVATE speedsql_static)
    set_target_properties(speedsql_bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Install
install(TARGETS speedsql_static speedsql_shared
    EXPORT SpeedSQLTargets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

install(FILES ${SPEEDSQL_HEADERS}
    DESTINATION include
)

install(EXPORT SpeedSQLTargets
    FILE SpeedSQLTargets.cmake
    NAMESPACE SpeedSQL::
    DESTINATION lib/cmake/SpeedSQL
)

# Package config
include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/SpeedSQLConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedSQLConfig.cmake
    INSTALL_DESTINATION lib/cmake/SpeedSQL
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedSQLConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedSQLConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedSQLConfigVersion.cmake
    DESTINATION lib/cmake/SpeedSQL
)

# Summary
message(STATUS "")
message(STATUS "SpeedSQL ${PROJECT_VERSION} Configuration:")
message(STATUS "  Build shared library: ${SPEEDSQL_BUILD_SHARED}")
message(STATUS "  Build static library: ${SPEEDSQL_BUILD_STATIC}")
message(STATUS "  Build tests: ${SPEEDSQL_BUILD_TESTS}")
message(STATUS "  Build examples: ${SPEEDSQL_BUILD_EXAMPLES}")
message(STATUS "  Build benchmarks: ${SPEEDSQL_BUILD_BENCHMARK}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "")
