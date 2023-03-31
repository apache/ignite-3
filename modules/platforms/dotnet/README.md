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

## Tables

## RecordView

## KeyValueView

## User Type Mapping

## Transactions

TODO RW, RO

## SQL

## LINQ

## Compute

## Logging

## Metrics

## Failover, Retry, Reconnect, Load Balancing
