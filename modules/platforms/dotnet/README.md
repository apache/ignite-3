# Apache Ignite 3 .NET Client

.NET client for Apache Ignite - a distributed database for high‑performance applications with in‑memory speed.


# Key Features

* Full support of all IGNITE APIs: SQL, Transactions, Key/Value, Compute.
* Connects to any number of Ignite nodes at the same time.
* Partition awareness: sends key-based requests to the right node.
* Load-balancing, failover, automatic reconnection and request retries.
* Built-in LINQ provider for strongly-typed SQL queries.
* Integrates with [NodaTime](https://nodatime.org/) to provide precise mapping to Ignite date/time types.
* Logging and metrics.
* High performance and fully async network layer.


# Getting Started

Below are a few examples of basic usage to get you started: 

```cs
// Connect to the cluster.
var cfg = new IgniteClientConfiguration("127.0.0.1:10800");
IIgniteClient client = await IgniteClient.StartAsync(cfg);

// Start a read-only transaction.
await using var tx = await client.Transactions.BeginAsync(new TransactionOptions { ReadOnly = true });

// Get table by name.
ITable? table = await client.Tables.GetTableAsync("Person");

// Get a strongly-typed view of table data using Person record.
IRecordView<Person> view = table!.GetRecordView<Person>();

// Upsert a record with KV (NoSQL) API.
await view.UpsertAsync(tx, new Person(1, "John"));

// Query data with SQL.
await using var resultSet = await client.Sql.ExecuteAsync<Person>(tx, "SELECT * FROM Person");
List<Person> sqlResults = await resultSet.ToListAsync();

// Query data with LINQ.
IQueryable<Person> query = view.AsQueryable(tx)
    .Where(person => person.Id > 0)
    .OrderBy(person => person.Name);

List<Person> linqResults = await query.ToListAsync();

string generatedSql = query.ToQueryString();

// Execute a distributed computation.
IList<IClusterNode> nodes = await client.GetClusterNodesAsync();
int wordCount = await client.Compute.ExecuteAsync<int>(nodes, "org.foo.bar.WordCountTask", "Hello, world!");
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
            RemoteCertificateValidationCallback = (sender, certificate, chain, errors) => true
        }
    },    
        
    // Retry all read operations in case of network issues.
    RetryPolicy = new RetryReadPolicy { RetryLimit = 32 },

    // Log to console.
    Logger = new ConsoleLogger { MinLevel = LogLevel.Debug }
};
```

## SQL

SQL is the primary API for data access. It is used to create, drop, and query tables, as well as to insert, update, and delete data. 

```cs
using var client = await IgniteClient.StartAsync(new("localhost"));

await client.Sql.ExecuteAsync(null, "CREATE TABLE Person (Id INT PRIMARY KEY, Name VARCHAR)");
await client.Sql.ExecuteAsync(null, "INSERT INTO Person (Id, Name) VALUES (1, 'John Doe')");

await using var resultSet = await client.Sql.ExecuteAsync(null, "SELECT Name FROM Person");
await foreach (IIgniteTuple row in resultSet)
    Console.WriteLine(row[0]);
```

### Mapping SQL Results to User Types

By default, SQL results are returned as `IIgniteTuple` instances. This interface provides access to raw data by column name or index (see example above).

To map SQL results to user types and access data in a strongly-typed manner, use generic `ExecuteAsync<T>` overload: 

```cs
await using var resultSet = await client.Sql.ExecuteAsync<Person>(null, "SELECT Name FROM Person");
await foreach (Person p in resultSet)
    Console.WriteLine(p.Name);
    
public record Person(int Id, string Name);
```

Column names are matched to record properties by name. If a column name does not match any property, it is ignored.

To map columns to properties with different names, use `ColumnAttribute`:

```cs
[Column("FULL_NAME")]
public string Name { get; set; }
```

Mapping is performed using runtime code generation (IL emit). Emitted delegates are cached. User type mapping is more performant than `IIgniteTuple` approach and allocates less memory while reading query results.

## LINQ

## Tables

## RecordView

## KeyValueView

## User Type Mapping

## Transactions

TODO RW, RO

## Compute

## Logging

## Metrics

## Failover, Retry, Reconnect, Load Balancing
