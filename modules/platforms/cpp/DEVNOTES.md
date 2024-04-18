## Build C++

### Prerequisites


* C++ compiler supporting C++ 17
* One of the build systems: make, ninja, MS Visual Studio, etc.
* Conan C/C++ package manager 1.X (optional)
* CMake 3.10+

To build the ODBC driver, it is required to have an ODBC driver manager with headers on your system. On Windows, it 
comes with your OS. On Unix-like systems, you may need to install one; for example, unixODBC.

### Installing Conan Package Manager

The Conan package manager can be obtained from [its website](https://conan.io).

Currently, we support Conan versions 1.X. The 2.0+ versions are **not** supported yet.

One way to install Conan is as follows (need Python on your system):

```shell
pip install conan==1.59.0
```

Before use, you might need to configure the default Conan profile:

```
conan profile new --detect default
conan profile update settings.compiler.libcxx=libstdc++11 default
```

### Avoiding Conan Package Manager

It is possible to build the project without Conan if all the dependencies are installed on the system manually.
The project dependencies include:

* msgpack-c 4.0.0
* gtest 1.12.1

When the project is configured with `-DENABLE_CONAN=OFF`, the Conan machinery is turned off, and the dependencies are resolved using the standard means of the build platform. For example, the project can be configured like this:

```shell
...
cmake .. -DENABLE_CONAN=OFF -DCMAKE_BUILD_TYPE=Release
...
```

Conan is enabled by default. All the build examples below use Conan.

### CMake Options and Typical Build Configurations

The CMake project supports multiple configuration options that can be used to define exactly what and how should be 
built. You can check the list of available CMake options with the `cmake -LAH` command. 
Following are the project-specific options:

* ENABLE_CONAN={ON|OFF} - ON by default. The effect of this option is described in the section above.
* ENABLE_CLIENT={ON|OFF} - ON by default. Indicates whether the C++ client should be built.
* ENABLE_ODBC={ON|OFF} - OFF by default. Indicates whether the ODBC driver should be built.
* ENABLE_TESTS={ON|OFF} - OFF by default. Indicates whether the tests for the selected components should be built.
* WARNINGS_AS_ERRORS={ON|OFF} - OFF by default. If enabled, the compiler treats warnings as errors. It may be a good 
idea to enable this option if you are planning on submitting a PR. If you just want to build a project, keep this 
option disabled.

You should also specify the general (build type) CMake options. There are two types of build available - `Release` and `Debug`. The choice depends on how are you going to use the resulting artifacts. If you are going to use them in production, use the `Release` build type. If you are planning to just submit a patch for the project, use `Debug`.

You must ALWAYS specify a build type.

There are two options for this:

* CMAKE_BUILD_TYPE={Debug|Release} - Used on single-configuration generators, like Unix Makefile or Ninja. This
  is a parameter that you should use with a Unix-like OS.
* CMAKE_CONFIGURATION_TYPES={Debug|Release} - Used on multi-configuration generators like Visual Studio. If you are 
using the Visual Studio generator, you should specify this option, which ignores CMAKE_BUILD_TYPE.

As an Apache Ignite developer, you would want to configure your project like this:

```shell
cmake .. -DENABLE_ODBC=ON -DENABLE_TESTS=ON -DWARNINGS_AS_ERRORS=ON -DCMAKE_BUILD_TYPE=Debug
```

If you want to only compile the client to use in your project, the following configuration is more fitting, considering the default options:

```shell
cmake .. -DCMAKE_BUILD_TYPE=Release
```

If you don't need the client and just want to compile the ODBC driver, add the following options:

```shell
cmake .. -DENABLE_CLIENT=OFF -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Release
```

Don't forget to replace `CMAKE_BUILD_TYPE` with `CMAKE_CONFIGURATION_TYPES` if you are using the Visual Studio
generator:

```shell
cmake .. -DCMAKE_CONFIGURATION_TYPES=Release
```

In the following sections, you can find more detailed line-by-line instructions and configurations for the different platforms and use cases.

### Linux and macOS Builds

When using Conan on macOS, it is typically required to specify the C++ standard library from the LLVM project:

```
conan profile update settings.compiler.libcxx=libc++11 default
```

#### Building in the Debug Mode with Tests and ODBC

In this dir:

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build .
```

#### Building Only the Client in the Release Mode

In this dir:

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

#### Building Only the ODBC Driver in the Release Mode

In this dir:

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DENABLE_CLIENT=OFF -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

### Windows Build

#### Building in the Debug Mode with Tests and ODBC Using a Single-config Generator

In this dir (using Ninja or any other single-config generator):

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
cmake --build .
```

#### Building Only the Client in the Release Mode Using a Single-config Generator

In this dir (using Ninja or any other single-config generator):

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release -GNinja
cmake --build .
```

#### Building with Visual Studio in the Multi-config Mode

Run in this dir from, for example, 
[VS developer PowerShell](https://learn.microsoft.com/en-us/visualstudio/ide/reference/command-prompt-powershell?view=vs-2022).

We are using Visual Studio 17 2022 in this example, but any other multi-config generator can be used.

```shell
mkdir cmake-build
cd cmake-build
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_CONFIGURATION_TYPES="Debug;Release" -G "Visual Studio 17 2022"
cmake --build . --config Debug
cmake --build . --config Release
```

## Running Tests

### Prerequisites

* Java 11 SDK
* Gradle

### Starting a Java Test Node

Tests require a running Java node. You don't need to start it explicitly. If there are no running test nodes, a test 
will start one implicitly. Prior to running tests, you will need to build a Java part of the product. To do that, run 
the following command from the root of the repo:

`./gradlew assemble compileIntegrationTestJava`

Alternatively, run a faster variantwww:

`./gradlew assemble compileIntegrationTestJava -x check -x assembleDist -x distTar -x distZip --parallel`

You can start a test node separately in the root repo. Tests will detect a running node, and will not start
another one. This can be useful for debugging. To start a node from the console, use the following command prompt:
`./gradlew :ignite-runner:runnerPlatformTest --no-daemon`

You can also run the `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or
profiler, then run the client tests as usual.

### Starting Tests on Windows

In modules/platforms/cpp dir:
`./cmake-build-debug/bin/ignite-client-test.exe`

To run a specific test:
`./cmake-build-debug/bin/ignite-client-test.exe --gtest_filter=Test_Cases1*`

### Starting Tests on Linux

In modules/platforms/cpp dir:
`./cmake-build-debug/bin/ignite-client-test`

To run a specific test:
`./cmake-build-debug/bin/ignite-client-test --gtest_filter=Test_Cases1*`

To debug or profile the Java side of the tests, run the `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner`
class in IDEA with a debugger or profiler, then run C++ tests as usual, optionally with debugger.
