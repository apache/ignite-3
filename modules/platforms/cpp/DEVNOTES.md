## Prerequisites
* C++ compiler supporting C++17
* Java 11 SDK
* Maven 3.6.0+ (for building)
* Conan C/C++ package manager
* CMake 3.10+

## Build Java
In repo root: `mvn clean install -DskipTests`

## Build C++

### For Developers
Building in debug mode with tests. In this dir:
```bash
mkdir cmake-build-debug
cd cmake-build-debug
conan install .. -s build_type=Debug
cmake .. -DENABLE_TESTS=ON
cmake --build . -j8 
```

### For users
Building in release mode without tests. In this dir:
```bash
mkdir cmake-build-release
cd cmake-build-release
conan install .. -s build_type=Release
cmake .. -DENABLE_TESTS=OFF
cmake --build . -j8
```

## Run Tests

### Windows
In this dir: `./cmake-build-debug/bin/ignite-client-test.exe`
Specific test: `./cmake-build-debug/bin/ignite-client-test.exe --gtest_filter=Test_Cases1*`

## Start a Test Node
* cd `modules/runner`
* `mvn exec:java@platform-test-node-runner`

To debug or profile Java side of the tests, run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or profiler,
then run C++ tests as always or with debugger.
