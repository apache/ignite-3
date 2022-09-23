## Prerequisites
* C++ compiler supporting C++17
* Java 11 SDK
* Maven 3.6.0+ (for building)
* Conan C/C++ package manager
* CMake 3.10+

## Build Java
In repo root: `mvn clean install -DskipTests`

## Build C++

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
conan install .. -pr:b=default --build=missing -s build_type=Debug -o boost:shared=True
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
conan install .. -pr:b=default --build=missing -s build_type=Release -o boost:shared=True
cmake .. -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release
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
