# .NET Platform Compute Executor

Facilitates the execution of .NET compute jobs. 

Java server starts this process on demand and communicates with it using the Ignite client protocol.

More details: https://cwiki.apache.org/confluence/display/IGNITE/IEP-136+Platform+Compute

## Release Build

To build the project, use the following command:

```bash
dotnet publish -c Release -p:UseAppHost=false
```
