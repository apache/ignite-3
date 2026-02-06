# Ignite Package Test

This directory contains tests to verify that the installed Ignite package can be found and used correctly via CMake's `find_package()` mechanism.

## Purpose

These tests verify:
1. The `ignite-config.cmake` package configuration works correctly
2. Component-based discovery (client, odbc) functions properly
3. The exported CMake targets link correctly
4. Headers and libraries are accessible

## Building the Tests

Each test configuration can build two executables: `test_client` and `test_odbc`; there are corresponding CMake options:
 * `ENABLE_CLIENT`
 * `ENABLE_ODBC`
These options can be used together. Each CMake configuration should build two tests (if `ENABLE_CLIENT=ON` and `ENABLE_ODBC=ON` are specified).
Successful compilation of these tests means we have found Ignite components, include directories and libraries to link with.

### `cmake_package`

This test implies that there is Ignite source code folder and Ignite was built at least once, but was not installed. 
This test uses `ignite-config.cmake` which will be generated in the binary directory.

Build command:
`cmake -DENABLE_CLIENT=ON -DENABLE_ODBC=ON -S ../tests/package-test/cmake_package/ -B build_package && cmake --build build_package`

### `cmake_package_install`

This test will search for the Ignite installation. For test purposes, the install will be done in a directory inside the build directory.
The Test will search for `ignite-config.cmake` in the installation directory. 

Build command:
`cmake -DENABLE_CLIENT=ON -DENABLE_ODBC=ON -S ../tests/package-test/cmake_package_install/ -B build_package_install && cmake --build build_package_install`

### `cmake_subdirectory`

This test will include Ignite as cmake subdirectory and will build its targets to link to the tests.

Build command:
`cmake -DENABLE_CLIENT=ON -DENABLE_ODBC=ON -S ../tests/package-test/cmake_subdirectory/ -B build_subdirectory && cmake --build build_subdirectory`
