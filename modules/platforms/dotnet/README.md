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

SQL results can be mapped to user types using `ExecuteAsync<T>` method. This is cleaner and more efficient than `IIgniteTuple` approach above

```cs
await using var resultSet = await client.Sql.ExecuteAsync<Person>(null, "SELECT Name FROM Person");
await foreach (Person p in resultSet)
    Console.WriteLine(p.Name);
    
public record Person(int Id, string Name);
```

Column names are matched to record properties by name. To map columns to properties with different names, use `ColumnAttribute`.
## DbDataReader (ADO.NET API)

Another way to work with query results is `System.Data.Common.DbDataReader`, which can be obtained with `ExecuteReaderAsync` method. 

For example, you can bind query results to a `DataGridView` control:

```cs
await using var reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);

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

```
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

TODO RW, RO

## Compute

## Logging

## Metrics

## Failover, Retry, Reconnect, Load Balancing
