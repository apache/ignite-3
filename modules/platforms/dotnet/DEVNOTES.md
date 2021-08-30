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

* Library project target `netstandard2.1`
* Test projects target `netcoreapp3.1`

See [IEP-78 .NET Thin Client](https://cwiki.apache.org/confluence/display/IGNITE/IEP-78+.NET+Thin+Client) for design considerations.


TODO: Assembly signing
