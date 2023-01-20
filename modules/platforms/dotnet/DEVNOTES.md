## Prerequisites
* .NET 6 SDK
* Java 11 SDK

## Build Java
In repo root:
`./gradlew assemble compileIntegrationTestJava`

Or a faster variant:
`./gradlew assemble compileIntegrationTestJava -x check -x assembleDist -x distTar -x distZip --parallel`

## Build .NET
In this dir: `dotnet build`

## Run Tests
In this dir: `dotnet test --logger "console;verbosity=normal"`

Specific test: `dotnet test --logger "console;verbosity=normal" --filter ClientSocketTests`

## Start a Test Node
`gradlew :ignite-runner:runnerPlatformTest --no-daemon`

To debug or profile Java side of the tests, run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or profiler,
then run .NET tests with `dotnet test` or `dotnet test --filter TEST_NAME`. When a server node is present, .NET tests will use it instead of starting a new one.

The test node will stop after 30 minutes by default.
To change this, set `IGNITE_PLATFORM_TEST_NODE_RUNNER_RUN_TIME_MINUTES` environment variable.

## Static Code Analysis

Static code analysis (Roslyn-based) runs as part of the build and includes code style check. Build fails on any warning.
* Analysis rules are defined in `.editorconfig` files (test projects have relaxed rule set).
* License header is defined in `stylecop.json`
* Warnings As Errors behavior is enabled in `Directory.Build.props` (can be disabled locally for rapid prototyping so that builds are faster and warnings don't distract)

## Release Procedure

### Build Binaries
`dotnet publish Apache.Ignite --configuration Release --output release/bin`

### Pack NuGet
`dotnet pack Apache.Ignite --configuration Release --include-source --output release/nupkg`
