## Prerequisites
* .NET 5 SDK
* Java 11 SDK
* Maven 3.6.0+ (for building)

## Build Java
In repo root: `mvn clean install -DskipTests`

## Run Tests
In `modules/platforms/dotnet/Apache.Ignite.Tests`: `dotnet test`

## Start a Test Node
`mvn -Dtest=ITThinClientConnectionTest -DfailIfNoTests=false -DIGNITE_TEST_KEEP_NODES_RUNNING=true surefire:test`

## .NET 5 and .NET Standard 2.1

* `Apache.Ignite` project targets `netstandard2.1`
* Test projects target `net5.0`

Ignite 3 is in alpha stage, we don't know the final release date.
Right now (Sep 2021) [the oldest supported LTS version of .NET SDK is .NET Core 3.1](https://dotnet.microsoft.com/platform/support/policy), which supports `netstandard2.1`.

**There are no supported SDKs that don't support `netstandard2.1`.** 

When the release time comes:
* Look at supported LTS versions of .NET and decide which ones to support.
* Add multi-targeting to our packages.
* Use conditional compilation, if necessary, to deal with unsupported APIs on older SDKs.

More info: https://docs.microsoft.com/en-us/dotnet/standard/net-standard#net-5-and-net-standard
