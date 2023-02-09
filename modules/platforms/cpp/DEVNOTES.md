## Build C++

### Prerequisites
* C++ compiler supporting C++17
* Conan C/C++ package manager
* CMake 3.10+

### For Windows Developers
Building in debug mode with tests. In this dir:
```shell
mkdir cmake-build-debug
cd cmake-build-debug
conan install .. --build=missing -s build_type=Debug
cmake .. -DENABLE_TESTS=ON
cmake --build . -j8
```

### For Linux Developers
Building in debug mode with tests. In this dir:
```shell
mkdir cmake-build-debug
cd cmake-build-debug
conan install .. --build=missing -s build_type=Debug -s compiler.libcxx=libstdc++11
cmake .. -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build . -j8
```

### For macOS Developers
Building in debug mode with tests. In this dir:
```shell
mkdir cmake-build-debug
cd cmake-build-debug
conan install .. --build=missing -s build_type=Debug -s compiler.libcxx=libc++
cmake .. -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build . -j8
```

### For Windows users
Building in release mode without tests. In this dir:
```shell
mkdir cmake-build-release
cd cmake-build-release
conan install .. --build=missing -s build_type=Release
cmake .. -DENABLE_TESTS=OFF
cmake --build . -j8
```

### For Linux users
Building in release mode without tests. In this dir:
```shell
mkdir cmake-build-release
cd cmake-build-release
conan install .. --build=missing -s build_type=Release -s compiler.libcxx=libstdc++11
cmake .. -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

### For macOS users
Building in release mode without tests. In this dir:
```shell
mkdir cmake-build-release
cd cmake-build-release
conan install .. --build=missing -s build_type=Release -s compiler.libcxx=libc++
cmake .. -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

## Run Tests

### Prerequisites
* Java 11 SDK
* Gradle

### Starting Java Test Node

Tests require a running Java node. You don't need to start it separately, if there is no running test nodes, tests will
start one internally. So prior to running tests you will obviously need to build a Java part of the product. To do that
the following command can be used from the root of the repo:
`./gradlew assemble compileIntegrationTestJava`

Or a faster variant:
`./gradlew assemble compileIntegrationTestJava -x check -x assembleDist -x distTar -x distZip --parallel`

You can start a Test Node separately in the root repo. Tests will detect that there is a running node and will not start
another one. This can be useful for debugging. To start node from the console you can use the following command prompt:
`./gradlew :ignite-runner:runnerPlatformTest --no-daemon`

You can also run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or
profiler, then run Client tests as usual.

### Starting tests in Windows
In modules/platforms/cpp dir:
`./cmake-build-debug/bin/ignite-client-test.exe`

To run a specific test:
`./cmake-build-debug/bin/ignite-client-test.exe --gtest_filter=Test_Cases1*`

### Starting tests in Linux
In modules/platforms/cpp dir:
`./cmake-build-debug/bin/ignite-client-test`

To run a specific test:
`./cmake-build-debug/bin/ignite-client-test --gtest_filter=Test_Cases1*`

To debug or profile Java side of the tests, run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner`
class in IDEA with a debugger or profiler, then run C++ tests as always or with
debugger.
