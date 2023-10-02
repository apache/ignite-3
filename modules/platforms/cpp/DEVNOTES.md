## Build C++

### Prerequisites
* C++ compiler supporting C++17
* One of build systems: make, ninja, MS Visual Studio, etc
* Conan C/C++ package manager 1.X (optional)
* CMake 3.10+

### Installing Conan Package Manager

The Conan package manager can be obtained from [its website](https://conan.io).

Currently, we support Conan versions 1.X. The version 2.0+ is **not** supported yet.

One way to install Conan is as follows (need python in your system):

```shell
pip install conan==1.59.0
```

Also, before use, it might be required to configure the default conan profile:

```
conan profile new --detect default
conan profile update settings.compiler.libcxx=libstdc++11 default
```

### Avoiding Conan Package Manager

It is possible to build the project without Conan if all the dependencies are installed on the system manually.
The project dependencies include the following libraries:

	- msgpack-c 4.0.0
	- gtest 1.12.1
	- unixodbc 

When the project is configured with the `-DENABLE_CONAN=OFF` CMake option, the Conan machinery is turned off and
the dependencies are resolved by using the standard means of the build platform. For example, the project can be
configured like this:

```shell
...
cmake .. -DENABLE_CONAN=0 -DCMAKE_BUILD_TYPE=Release
...
```

However, Conan is enabled by default, and so all the build examples below use it.

### Linux Build

#### Building in debug mode with tests

In this dir:

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build . -j8
```

#### Building in release mode without tests

In this dir:

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DENABLE_TESTS=OFF -DENABLE_ODBC=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

### MacOS Build

On macOS, it is typically required to use the C++ standard library from the LLVM project:

```
conan profile update settings.compiler.libcxx=libc++11 default
```

#### Building in debug mode with tests.

In this dir:

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build . -j8
```

#### Building in release mode without tests.

In this dir:

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DENABLE_TESTS=OFF -DENABLE_ODBC=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

### Windows Build

#### Building in debug mode with tests

In this dir (using the ninja build system, other single-config systems can be used too):

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=OFF -DCMAKE_BUILD_TYPE=Debug -GNinja
cmake --build . -j8
```

#### Building in release mode without tests

In this dir (using the ninja build system, other single-config systems can be used too):

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DENABLE_TESTS=OFF -DENABLE_ODBC=OFF -DCMAKE_BUILD_TYPE=Release -GNinja
cmake --build . -j8
```

#### Building with Visual Studio in multi-config mode

Run in this dir from, for example, [VS developer PowerShell](https://learn.microsoft.com/en-us/visualstudio/ide/reference/command-prompt-powershell?view=vs-2022):

```shell
mkdir cmake-build
cd cmake-build
cmake .. -DENABLE_TESTS=ON
cmake --build . --config Debug -j8
cmake --build . --config Release -j8
```

## Run Tests

### Prerequisites
* Java 11 SDK
* Gradle

### Starting Java Test Node

Tests require a running Java node. You don't need to start it separately, if there are no running test nodes, tests will
start one internally. So prior to running tests, you will obviously need to build a Java part of the product. To do
that, the following command can be used from the root of the repo:
`./gradlew assemble compileIntegrationTestJava`

Or a faster variant:
`./gradlew assemble compileIntegrationTestJava -x check -x assembleDist -x distTar -x distZip --parallel`

You can start a Test Node separately in the root repo. Tests will detect that there is a running node and will not start
another one. This can be useful for debugging. To start node from the console, you can use the following command prompt:
`./gradlew :ignite-runner:runnerPlatformTest --no-daemon`

You can also run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or
profiler, then run Client tests as usual.

### Starting tests on Windows
In modules/platforms/cpp dir:
`./cmake-build-debug/bin/ignite-client-test.exe`

To run a specific test:
`./cmake-build-debug/bin/ignite-client-test.exe --gtest_filter=Test_Cases1*`

### Starting tests on Linux
In modules/platforms/cpp dir:
`./cmake-build-debug/bin/ignite-client-test`

To run a specific test:
`./cmake-build-debug/bin/ignite-client-test --gtest_filter=Test_Cases1*`

To debug or profile Java side of the tests, run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner`
class in IDEA with a debugger or profiler, then run C++ tests as always or with
debugger.
