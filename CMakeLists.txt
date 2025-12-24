cmake_minimum_required(VERSION 3.16)
project(SpeedDB VERSION 0.1.0 LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build options
option(SPEEDDB_BUILD_SHARED "Build shared library" ON)
option(SPEEDDB_BUILD_STATIC "Build static library" ON)
option(SPEEDDB_BUILD_TESTS "Build tests" ON)
option(SPEEDDB_BUILD_EXAMPLES "Build examples" ON)
option(SPEEDDB_BUILD_BENCHMARK "Build benchmarks" OFF)

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
set(SPEEDDB_SOURCES
    src/core/database.cpp
    src/storage/file_io.cpp
    src/storage/buffer_pool.cpp
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
set(SPEEDDB_HEADERS
    include/speeddb.h
    include/speeddb_types.h
    include/speeddb_internal.h
    include/speeddb_crypto.h
)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Static library
if(SPEEDDB_BUILD_STATIC)
    add_library(speeddb_static STATIC ${SPEEDDB_SOURCES} ${SPEEDDB_HEADERS})
    target_include_directories(speeddb_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    set_target_properties(speeddb_static PROPERTIES
        OUTPUT_NAME speeddb
        ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
    )

    # Platform-specific libraries
    if(WIN32)
        target_link_libraries(speeddb_static PRIVATE)
    else()
        target_link_libraries(speeddb_static PRIVATE pthread)
    endif()
endif()

# Shared library
if(SPEEDDB_BUILD_SHARED)
    add_library(speeddb_shared SHARED ${SPEEDDB_SOURCES} ${SPEEDDB_HEADERS})
    target_compile_definitions(speeddb_shared PRIVATE SPEEDDB_EXPORTS)
    target_include_directories(speeddb_shared PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    set_target_properties(speeddb_shared PROPERTIES
        OUTPUT_NAME speeddb
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
    )

    if(WIN32)
        target_link_libraries(speeddb_shared PRIVATE)
    else()
        target_link_libraries(speeddb_shared PRIVATE pthread)
    endif()
endif()

# Examples
if(SPEEDDB_BUILD_EXAMPLES)
    # Basic usage example
    add_executable(speeddb_example examples/basic_usage.cpp)
    target_link_libraries(speeddb_example PRIVATE speeddb_static)
    set_target_properties(speeddb_example PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Encryption example
    add_executable(speeddb_crypto_example examples/encryption_example.cpp)
    target_link_libraries(speeddb_crypto_example PRIVATE speeddb_static)
    set_target_properties(speeddb_crypto_example PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Custom cipher example
    add_executable(speeddb_custom_cipher examples/custom_cipher_example.cpp)
    target_link_libraries(speeddb_custom_cipher PRIVATE speeddb_static)
    set_target_properties(speeddb_custom_cipher PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Tests
if(SPEEDDB_BUILD_TESTS)
    enable_testing()

    add_executable(speeddb_test tests/test_main.cpp)
    target_link_libraries(speeddb_test PRIVATE speeddb_static)
    set_target_properties(speeddb_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    add_test(NAME SpeedDBTest COMMAND speeddb_test)
endif()

# Benchmarks
if(SPEEDDB_BUILD_BENCHMARK)
    add_executable(speeddb_bench benchmark/bench_main.cpp)
    target_link_libraries(speeddb_bench PRIVATE speeddb_static)
    set_target_properties(speeddb_bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Install
install(TARGETS speeddb_static speeddb_shared
    EXPORT SpeedDBTargets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

install(FILES ${SPEEDDB_HEADERS}
    DESTINATION include
)

install(EXPORT SpeedDBTargets
    FILE SpeedDBTargets.cmake
    NAMESPACE SpeedDB::
    DESTINATION lib/cmake/SpeedDB
)

# Package config
include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/SpeedDBConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedDBConfig.cmake
    INSTALL_DESTINATION lib/cmake/SpeedDB
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedDBConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedDBConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/SpeedDBConfigVersion.cmake
    DESTINATION lib/cmake/SpeedDB
)

# Summary
message(STATUS "")
message(STATUS "SpeedDB ${PROJECT_VERSION} Configuration:")
message(STATUS "  Build shared library: ${SPEEDDB_BUILD_SHARED}")
message(STATUS "  Build static library: ${SPEEDDB_BUILD_STATIC}")
message(STATUS "  Build tests: ${SPEEDDB_BUILD_TESTS}")
message(STATUS "  Build examples: ${SPEEDDB_BUILD_EXAMPLES}")
message(STATUS "  Build benchmarks: ${SPEEDDB_BUILD_BENCHMARK}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "")
