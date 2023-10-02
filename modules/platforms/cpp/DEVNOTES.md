## Build C++

### Prerequisites
* C++ compiler supporting C++17;
* One of build systems: make, ninja, MS Visual Studio, etc;
* Conan C/C++ package manager 1.X (optional);
* CMake 3.10+;
* To build the ODBC driver, it is required to have an ODBC driver manager with headers on your system. On Windows, it 
  comes with your OS, but on Unix-like systems you may need to install one, for example, unixODBC;

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

When the project is configured with the `-DENABLE_CONAN=OFF` CMake option, the Conan machinery is turned off and
the dependencies are resolved by using the standard means of the build platform. For example, the project can be
configured like this:

```shell
...
cmake .. -DENABLE_CONAN=OFF -DCMAKE_BUILD_TYPE=Release
...
```

However, Conan is enabled by default, and so all the build examples below use it.

### CMake options and examples of typical build configurations

There are multiple configuration options supported by the CMake project that can be used to choose what exactly and how
should be built. You can check the whole list of available cmake options with the command `cmake -LAH`. Below, you can
see project-specific options only:
- ENABLE_CONAN={ON|OFF}. ON by default. The effect of this exact option is described in a section above;
- ENABLE_CLIENT={ON|OFF}. ON by default. Indicates whether the C++ client should be built;
- ENABLE_ODBC={ON|OFF}. OFF by default. Indicates whether the ODBC driver should be built;
- ENABLE_TESTS={ON|OFF}. OFF by default. Indicates whether the tests for the selected components should be built;
- WARNINGS_AS_ERRORS={ON|OFF}. OFF by default. If enabled, compiler will treat warnings as errors. It may be a good idea
  to enable this option if you are planning on submitting a PR, but if you just want to build a project just keep it 
  disabled;

There are also general CMake options that you should specify. They are build type options. There are two types of build
available - `Release` and `Debug`. The choice here depends on how are you going to use resulting artifacts. If you are
going to use them in production, it is probably a good idea to use `Release` build type. If you are just an Ignite
developer or planning to submit a patch for the project, use Debug.

You should ALWAYS specify a build type.

There are two options for this:
- CMAKE_BUILD_TYPE={Debug|Release}. Used on single-configuration generators, like Unix Makefile generator or Ninja. This
  is a parameter that you should most likely use if you are using Unix-like OS;
- CMAKE_CONFIGURATION_TYPES={Debug|Release}. Used on multi-configuration generators like Visual Studio. If you are using
  Visual Studio generator, you should specify this option, as it ignores CMAKE_BUILD_TYPE.

So, basically, if you are an Apache Ignite developer, you would want to configure your project like this:

```shell
cmake .. -DENABLE_ODBC=ON -DENABLE_TESTS=ON -DWARNINGS_AS_ERRORS=ON -DCMAKE_BUILD_TYPE=Debug
```

But if you just want to compile the client to use in your project, the following configuration is more fitting for you,
considering default options:

```shell
cmake .. -DCMAKE_BUILD_TYPE=Release
```

If you don't need the client and just want to compile the ODBC driver, just add the following options:

```shell
cmake .. -DENABLE_CLIENT=OFF -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Release
```

And do not forget to replace `CMAKE_BUILD_TYPE` with `CMAKE_CONFIGURATION_TYPES` if you are using Visual Studio
generator:

```shell
cmake .. -DCMAKE_CONFIGURATION_TYPES=Release
```

Below, you can find more detailed line-by-line instructions and configurations for different platforms and use-cases.


### Linux and macOS Builds

On macOS, it is typically required to specify the C++ standard library from the LLVM project if you are using Conan:

```
conan profile update settings.compiler.libcxx=libc++11 default
```

#### Building in debug mode with tests and ODBC

In this dir:

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build .
```

#### Building only the client in release mode

In this dir:

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

#### Building only the ODBC driver in release mode

In this dir:

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DENABLE_CLIENT=OFF -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

### Windows Build

#### Building in debug mode with tests and ODBC using single-config generator

In this dir (using Ninja, but any other single-config generator can be used):

```shell
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
cmake --build .
```

#### Building only the client in release mode using single-config generator

In this dir (using Ninja, but any other single-config generator can be used):

```shell
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release -GNinja
cmake --build .
```

#### Building with Visual Studio in multi-config mode

Run in this dir from, for example, [VS developer PowerShell](https://learn.microsoft.com/en-us/visualstudio/ide/reference/command-prompt-powershell?view=vs-2022):
We are using Visual Studio 17 2022 in this example, but any other multi-config generator can be used.

```shell
mkdir cmake-build
cd cmake-build
cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_CONFIGURATION_TYPES="Debug;Release" -G "Visual Studio 17 2022"
cmake --build . --config Debug
cmake --build . --config Release
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
