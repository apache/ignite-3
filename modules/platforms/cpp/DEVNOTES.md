## Prerequisites
* C++ compiler supporting C++17
* Java 11 SDK
* Conan C/C++ package manager
* CMake 3.10+

## Build Java
In repo root: `gradlew clean build -x test`

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

### Windows
In this dir: `./cmake-build-debug/bin/ignite-client-test.exe`
Specific test: `./cmake-build-debug/bin/ignite-client-test.exe --gtest_filter=Test_Cases1*`

## Start a Test Node
* `gradlew :ignite-runner:runnerPlatformTest --no-daemon`

To debug or profile Java side of the tests, run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or profiler,
then run C++ tests as always or with debugger.
