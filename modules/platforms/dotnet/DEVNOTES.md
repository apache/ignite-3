## Prerequisites
* .NET 6 SDK
* Java 11 SDK
* Maven 3.6.0+ (for building)

## Build Java
In repo root: `mvn clean install -DskipTests`

## Build .NET
In this dir: `dotnet build`

## Run Tests
In this dir: `dotnet test --logger "console;verbosity=normal"`
Specific test: `dotnet test --logger "console;verbosity=normal" --filter ClientSocketTests`

## Start a Test Node
* cd `modules/runner`
* `mvn exec:java@platform-test-node-runner`

To debug or profile Java side of the tests, run `org.apache.ignite.internal.runner.app.PlatformTestNodeRunner` class in IDEA with a debugger or profiler,
then run .NET tests with `dotnet test` or `dotnet test --filter TEST_NAME`. When a server node is present, .NET tests will use it instead of starting a new one.

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
