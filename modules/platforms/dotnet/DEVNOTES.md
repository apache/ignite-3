## Prerequisites
* .NET Core 3.1 SDK
* Java 11 SDK
* Maven 3.6.0+ (for building)

## Build Java
In repo root: `mvn clean install -DskipTests`

## Run Tests
In `modules/platforms/dotnet/Apache.Ignite.Tests`: `dotnet test`

## Start a Test Node
`mvn -Dtest=ITThinClientConnectionTest -DfailIfNoTests=false -DIGNITE_TEST_KEEP_NODES_RUNNING=true surefire:test`

## .NET Core 3.1 and .NET Standard 2.1

* `Apache.Ignite` project targets `netstandard2.1`
* Test projects target `netcoreapp3.1`

Ignite 3 is in alpha stage, we don't know the final release date.
Right now (Sep 2021) [the only supported LTS version of .NET SDK is .NET Core 3.1](https://dotnet.microsoft.com/platform/support/policy), which supports `netstandard2.1`.

There is also .NET Framework (Windows-only) which is a part of Windows and follows the same support policy (.NET Framework 4.8 is supposed to be supported for many years to come).
We are **not** going to target this in alpha stages to simplify development. 

When the release time comes:
* Look at supported versions of .NET and decide which ones to target.
* Add multi-targeting to our packages.
* Use conditional compilation, if necessary, to deal with unsupported APIs on older SDKs.

More info: https://docs.microsoft.com/en-us/dotnet/standard/net-standard#net-5-and-net-standard, https://dotnet.microsoft.com/platform/support/policy/dotnet-framework
