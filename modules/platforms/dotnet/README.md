# Apache Ignite 3 .NET Client

.NET client for [Apache Ignite](https://ignite.apache.org/) - a distributed database for high‑performance applications with in‑memory speed.


# Key Features

* Full support of all Ignite APIs: SQL, Transactions, Key/Value, Compute.
* Connects to any number of Ignite nodes at the same time.
* Partition awareness: sends key-based requests to the right node.
* Load-balancing, failover, automatic reconnection and request retries.
* Built-in LINQ provider for strongly-typed SQL queries.
* Integrates with [NodaTime](https://nodatime.org/) to provide precise mapping to Ignite date/time types.
* Logging and metrics.
* High performance and fully asynchronous.


# Getting Started

Below are a few examples of basic usage to get you started: 

```cs
// Connect to the cluster.
var cfg = new IgniteClientConfiguration("127.0.0.1:10800");
IIgniteClient client = await IgniteClient.StartAsync(cfg);

// Start a read-only transaction.
await using var tx = await client.Transactions.BeginAsync(
    new TransactionOptions { ReadOnly = true });

// Get table by name.
ITable? table = await client.Tables.GetTableAsync("Person");

// Get a strongly-typed view of table data using Person record.
IRecordView<Person> view = table!.GetRecordView<Person>();

// Upsert a record with KV (NoSQL) API.
await view.UpsertAsync(tx, new Person(1, "John"));

// Query data with SQL.
await using var resultSet = await client.Sql.ExecuteAsync<Person>(
    tx, "SELECT * FROM Person");
    
List<Person> sqlResults = await resultSet.ToListAsync();

// Query data with LINQ.
List<string> names  = view.AsQueryable(tx)
    .OrderBy(person => person.Name)
    .Select(person => person.Name)
    .ToList();

// Execute a distributed computation.
IList<IClusterNode> nodes = await client.GetClusterNodesAsync();
IJobTarget<IEnumerable<IClusterNode>> jobTarget = JobTarget.AnyNode(nodes);
var jobDesc = new JobDescriptor<string, int>(
    "org.foo.bar.WordCountJob");
IJobExecution<int> jobExecution = await client.Compute.SubmitAsync(
    jobTarget, jobDesc, "Hello, world!");

int wordCount = await jobExecution.GetResultAsync();
```

## DI Integration

Use `IgniteServiceCollectionExtensions.AddIgniteClientGroup` to register Ignite client in the dependency injection container:

```cs
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddIgniteClientGroup(new IgniteClientGroupConfiguration
{
    Size = 1, // Client pool size, 1 is enough for most scenarios.
    ClientConfiguration = new IgniteClientConfiguration("localhost:10942")
});    
```

Inject `IgniteClientGroup` where needed:

```cs
app.MapGet("/tables", async ([FromServices] IgniteClientGroup igniteGrp) =>
{
    IIgnite ignite = await igniteGrp.GetIgniteAsync();

    return await ignite.Tables.GetTablesAsync();
});
```

# API Walkthrough

## Configuration

`IgniteClientConfiguration` is used to configure connections properties (endpoints, SSL), retry policy, logging, and timeouts. 

```cs
var cfg = new IgniteClientConfiguration
{
    // Connect to multiple servers.
    Endpoints = { "server1:10800", "server2:10801" },

    // Enable TLS.
    SslStreamFactory = new SslStreamFactory
    {
        SslClientAuthenticationOptions = new SslClientAuthenticationOptions
        {
            // Allow self-signed certificates.
            RemoteCertificateValidationCallback = 
                (sender, certificate, chain, errors) => true
        }
    },    
        
    // Retry all read operations in case of network issues.
    RetryPolicy = new RetryReadPolicy { RetryLimit = 32 }
};
```

## SQL

SQL is the primary API for data access. It is used to create, drop, and query tables, as well as to insert, update, and delete data. 

```cs
using var client = await IgniteClient.StartAsync(new("localhost"));

await client.Sql.ExecuteAsync(
    null, "CREATE TABLE Person (Id INT PRIMARY KEY, Name VARCHAR)");

await client.Sql.ExecuteAsync(
    null, "INSERT INTO Person (Id, Name) VALUES (1, 'John Doe')");

await using var resultSet = await client.Sql.ExecuteAsync(
    null, "SELECT Name FROM Person");

await foreach (IIgniteTuple row in resultSet)
    Console.WriteLine(row[0]);
```

### Mapping SQL Results to User Types

SQL results can be mapped to user types using `ExecuteAsync<T>` method. This is cleaner and more efficient than `IIgniteTuple` approach above.

```cs
await using var resultSet = await client.Sql.ExecuteAsync<Person>(
    null, "SELECT Name FROM Person");
    
await foreach (Person p in resultSet)
    Console.WriteLine(p.Name);
    
public record Person(int Id, string Name);
```

Column names are matched to record properties by name. To map columns to properties with different names, use `ColumnAttribute`.

## DbDataReader (ADO.NET API)

Another way to work with query results is `System.Data.Common.DbDataReader`, which can be obtained with `ExecuteReaderAsync` method. 

For example, you can bind query results to a `DataGridView` control:

```cs
await using var reader = await client.Sql.ExecuteReaderAsync(
    null, "select * from Person");

var dt = new DataTable();
dt.Load(reader);

dataGridView1.DataSource = dt;
```

## NoSQL

NoSQL API is used to store and retrieve data in a key/value fashion. It can be more efficient than SQL in certain scenarios. 
Existing tables can be accessed, but new tables can only be created with SQL.

First, get a table by name:

```cs
ITable? table = await client.Tables.GetTableAsync("Person");
```

Then, there are two ways to look at the data.

### Record View

Record view represents the entire row as a single object. It can be an `IIgniteTuple` or a user-defined type.

```cs
IRecordView<IIgniteTuple> binaryView = table.RecordBinaryView;
IRecordView<Person> view = table.GetRecordView<Person>();

await view.UpsertAsync(null, new Person(1, "John"));
```

### KeyValue View

Key/Value view splits the row into key and value parts.

```cs
IKeyValueView<IIgniteTuple, IIgniteTuple> kvBinaryView = table.KeyValueBinaryView;
IKeyValueView<PersonKey, Person> kvView = table.GetKeyValueView<PersonKey, Person>();

await kvView.PutAsync(null, new PersonKey(1), new Person("John"));
```


## LINQ

Data can be queried and modified with LINQ using `AsQueryable` method. 
LINQ expressions are translated to SQL queries and executed on the server. 

```cs
ITable? table = await client.Tables.GetTableAsync("Person");
IRecordView<Person> view = table!.GetRecordView<Person>();

IQueryable<string> query = view.AsQueryable()
    .Where(p => p.Id > 100)
    .Select(p => p.Name);

List<string> names = await query.ToListAsync();
```

Generated SQL can be retrieved with `ToQueryString` extension method, or by enabling debug logging.

Bulk update and delete with optional conditions are supported via `ExecuteUpdateAsync` and `ExecuteDeleteAsync` extensions methods on `IQueryable<T>`


## Transactions

All operations on data in Ignite are transactional. If a transaction is not specified, an explicit transaction is started and committed automatically.

To start a transaction, use `ITransactions.BeginAsync` method. Then, pass the transaction object to all operations that should be part of the same transaction.

```cs
await using ITransaction tx = await client.Transactions.BeginAsync();

await view.UpsertAsync(tx, new Person(1, "John"));

await client.Sql.ExecuteAsync(
    tx, "INSERT INTO Person (Id, Name) VALUES (2, 'Jane')");

await view.AsQueryable(tx)
    .Where(p => p.Id > 0)
    .ExecuteUpdateAsync(updater => 
        updater.SetProperty(person => person.Name, person => person.Name + " Doe"));

await tx.CommitAsync();
```


## Compute

Compute API is used to execute distributed computations on the cluster. 

Compute jobs can be implemented in Java or .NET. 
Resulting binaries (jar or dll files) should be [deployed](https://ignite.apache.org/docs/ignite3/latest/developers-guide/code-deployment/code-deployment) to the server nodes and called by the full class name.

### Call a Java Compute Job

You can call [Java computing jobs](https://ignite.apache.org/docs/ignite3/latest/developers-guide/compute/compute) from your .NET code, for example:

```csharp
IList<IClusterNode> nodes = await client.GetClusterNodesAsync();
IJobTarget<IEnumerable<IClusterNode>> jobTarget = JobTarget.AnyNode(nodes);

var jobDesc = new JobDescriptor<string, string>(
    JobClassName: "org.foo.bar.MyJob",
    DeploymentUnits: [new DeploymentUnit("Unit1")]);

IJobExecution<string> jobExecution = await client.Compute.SubmitAsync(
    jobTarget, jobDesc, "Job Arg");

string jobResult = await jobExecution.GetResultAsync();
```

### Implement a .NET Compute Job

1. Prepare a "class library" project for the job implementation (`dotnet new classlib`). In most cases, it is better to use a separate project for compute jobs to reduce deployment size.
2. Add a reference to `Apache.Ignite` package to the class library project (`dotnet add package Apache.Ignite`).
3. Create a class that implements `IComputeJob<TArg, TRes>` interface, for example:
    ```csharp
    public class HelloJob : IComputeJob<string, string>
    {
        public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult("Hello " + arg);
    }
    ```
4. Publish the project (`dotnet publish -c Release`).
5. Copy the resulting dll file (and any extra dependencies, EXCLUDING Ignite dlls) to a separate directory.
   * Note: The directory with the dll must not contain any subdirectories.
6. Use [Ignite CLI](https://ignite.apache.org/docs/ignite3/latest/ignite-cli-tool#cluster-commands) `cluster unit deploy` command to deploy the directory to the cluster as a deployment unit.

### Run a .NET Compute Job

.NET compute jobs can be executed from any client (.NET, Java, C++, etc), 
by specifying the assembly-qualified class name and using the `JobExecutorType.DotNetSidecar` option.

```csharp
var jobTarget = JobTarget.AnyNode(await client.GetClusterNodesAsync());
var jobDesc = new JobDescriptor<string, string>(
    JobClassName: typeof(HelloJob).AssemblyQualifiedName!,
    DeploymentUnits: [new DeploymentUnit("unit1")],
    Options: new JobExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar));

IJobExecution<string> jobExec = await client.Compute.SubmitAsync(jobTarget, jobDesc, "world");
```

Alternatively, use the `JobDescriptor.Of` shortcut method to create a job descriptor from a job instance:

```csharp
JobDescriptor<string, string> jobDesc = JobDescriptor.Of(new HelloJob())
    with { DeploymentUnits = [new DeploymentUnit("unit1")] };
```

#### Notes

* .NET 8 (or later) runtime (not SDK) is required on the server nodes.
* .NET compute jobs are executed in a separate process (sidecar) on the server node. 
* The process is started on the first .NET job call and then reused for subsequent jobs.
* Every deployment unit combination is loaded into a separate [AssemblyLoadContext](https://learn.microsoft.com/en-us/dotnet/core/dependency-loading/understanding-assemblyloadcontext).


## Failover, Retry, Reconnect, Load Balancing

Ignite client implements a number of features to improve reliability and performance:
* When multiple endpoints are configured, the client will maintain connections to all of them, and load balance requests between them.
* If a connection is lost, the client will try to reconnect, assuming it may be a temporary network issue or a node restart.
* Periodic heartbeat messages are used to detect connection issues early.
* If a user request fails due to a connection issue, the client will retry it automatically according to the configured `IgniteClientConfiguration.RetryPolicy`.


## Logging

To enable logging, set `IgniteClientConfiguration.LoggerFactory` property. It uses the standard [Microsoft.Extensions.Logging](https://docs.microsoft.com/en-us/dotnet/core/extensions/logging) API.

For example, to log to console (requires `Microsoft.Extensions.Logging.Console` package):

```cs
var cfg = new IgniteClientConfiguration
{
    LoggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug))
};
```

Or with Serilog (requires `Serilog.Extensions.Logging` and `Serilog.Sinks.Console` packages):

```cs
var cfg = new IgniteClientConfiguration
{
    LoggerFactory = LoggerFactory.Create(builder =>
        builder.AddSerilog(new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger()))
};
```


## Metrics

Ignite client exposes a number of metrics with `Apache.Ignite` meter name through the [System.Diagnostics.Metrics API](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation) 
that can be used to monitor system health and performance.

For example, [dotnet-counters](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/dotnet-counters) tool can be used like this:

```sh
dotnet-counters monitor --counters Apache.Ignite,System.Runtime --process-id PID
```

# Documentation

Full documentation is available at https://ignite.apache.org/docs.

# Feedback

Use any of the following channels to provide feedback:

* [user@ignite.apache.org](mailto:user@ignite.apache.org)
* https://stackoverflow.com/questions/tagged/ignite
* https://github.com/apache/ignite-3/issues
