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

## Why .NET Core 3.1?
Ignite 3 is in alpha stage, we don't know the final release date.
For now we use the oldest supported LTS version of .NET SDK. 

When the release time comes:
* Look at supported LTS versions of .NET and decide which ones to support.
* Add multi-targeting to our packages.
* Use conditional compilation, if necessary, to deal with unsupported APIs on older SDKs.
