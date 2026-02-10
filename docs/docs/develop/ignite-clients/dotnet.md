---
id: dotnet-client
title: .NET Client
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ignite 3 clients connect to the cluster via a standard socket connection. Unlike Ignite 2.x, there are no separate Thin and Thick clients in Ignite 3. All clients are 'thin'.

Clients do not become a part of the cluster topology, never hold any data, and are not used as a destination for compute calculations.

## Getting Started

### Prerequisites

To use C# thin client, .NET 8.0 or newer is required.

### Installation

C# client is available via NuGet. To add it, use the `add package` command:

```bash
dotnet add package Apache.Ignite --version 3.0.0
```

## Connecting to Cluster

To initialize a client, use the `IgniteClient` class, and provide it with the configuration:

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var clientCfg = new IgniteClientConfiguration
{
  Endpoints = { "127.0.0.1" }
};
using var client = await IgniteClient.StartAsync(clientCfg);
```

</TabItem>
</Tabs>

## Authentication

To pass authentication information, pass it to `IgniteClient` builder:

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var cfg = new IgniteClientConfiguration("127.0.0.1:10800")
{
	Authenticator = new BasicAuthenticator
	{
		Username = "myUser",
		Password = "myPassword"
	}
};
IIgniteClient client = await IgniteClient.StartAsync(cfg);
```

</TabItem>
</Tabs>

### Limitations

There are limitations to user types that can be used for such a mapping. Some limitations are common, and others are platform-specific due to the programming language used.

- Only flat field structure is supported, meaning no nesting user objects. This is because Ignite tables, and therefore tuples have flat structure themselves.
- Fields should be mapped to Ignite types.
- All fields in user type should either be mapped to Table column or explicitly excluded.
- All columns from Table should be mapped to some field in the user type.
- *.NET only*: Any type (class, struct, record) is supported as long as all fields can be mapped to Ignite types.

### Usage Examples

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
public class Account
{
  public long Id { get; set; }
  public long Balance { get; set; }

  [NotMapped]
  public Guid UnmappedId { get; set; }
}
```

</TabItem>
</Tabs>

## Using Dependency Injection

Ignite client provides support for using [Dependency Injection](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection) when initializing a client instance.

This approach can be used to simplify initializing the client in DI containers:

- Register the `IgniteClientGroup` in your DI container:

```cpp
builder.Services.AddSingleton<IgniteClientGroup>(_ => new IgniteClientGroup(
    new IgniteClientGroupConfiguration
    {
        Size = 3,
        ClientConfiguration = new("localhost"),
    }));
```

- Use an instance of the group you created in your methods:

```cpp
public async Task<IActionResult> Index([FromServices] IgniteClientGroup igniteGroup)
{
    IIgnite ignite = await igniteGroup.GetIgniteAsync();
    var tables = await ignite.Tables.GetTablesAsync();
    return Ok(tables);
}
```

## SQL API

Ignite 3 is focused on SQL, and SQL API is the primary way to work with the data. You can read more about supported SQL statements in the [SQL Reference](/3.1.0/sql/reference/language-definition/ddl) section. Here is how you can send SQL requests:

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
IResultSet<IIgniteTuple> resultSet = await client.Sql.ExecuteAsync(transaction: null, "select name from tbl where id = ?", 42);
List<IIgniteTuple> rows = await resultSet.ToListAsync();
IIgniteTuple row = rows.Single();
Debug.Assert(row["name"] as string == "John Doe");
```

</TabItem>
</Tabs>

### Batch SQL Execution

You can execute the specified DML statement once for each set of arguments and return the number of affected rows for each execution.

:::note
Only `INSERT`, `UPDATE`, `DELETE` statements are supported.
:::

To run a batch execution, you need to implement `ExecuteBatchAsync()` method with the following parameters:

```csharp
Task<long[]> ExecuteBatchAsync(
    ITransaction? transaction,
    SqlStatement statement,
    IEnumerable<IEnumerable<object?>> args,
    CancellationToken cancellationToken = default
);
```

#### Parameters

- `transaction` - The optional transaction in which to execute the batch.
- `statement` - The SQL statement to execute for each entry in `args`.
- `args` - A collection of argument lists. The statement will be executed once per inner collection. Must not be empty or contain empty rows.
- `cancellationToken` - Token for cancelling the operation.

#### Example

In this example we return an array of update counts. Each element corresponds to the number of rows affected by the statement execution for the matching entry in `args`. The length of the returned array equals the number of argument sets.

```csharp
long[] res = await sql.ExecuteBatchAsync(
    transaction: null,
    statement: "INSERT INTO Person (Id, Name) VALUES (?, ?)",
    args:
    [
        [1, "Alice"],
        [2, "Bob" ],
        [3, "Charlie"]
    ]
);
// res => [1, 1, 1]
```

### SQL Scripts

The default API executes SQL statements one at a time. If you want to execute large SQL statements, pass them to the `executeScript()` method. These statements will be executed in order.

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
string script =
    "CREATE TABLE IF NOT EXISTS Person (id int primary key, city_id int, name varchar, age int, company varchar);" +
    "INSERT INTO Person (1,3, 'John', 43, 'Sample')";

await Client.Sql.ExecuteScriptAsync(script);
```

</TabItem>
</Tabs>

:::note
Execution of each statement is considered complete when the first page is ready to be returned. As a result, when working with large data sets, SELECT statement may be affected by later statements in the same script.
:::

## Transactions

All table operations in Ignite 3 are transactional. You can provide an explicit transaction as a first argument of any Table and SQL API call. If you do not provide an explicit transaction, an implicit one will be created for every call.

Here is how you can provide a transaction explicitly:

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var accounts = table.GetKeyValueView<long, Account>();
await accounts.PutAsync(transaction: null, 42, new Account(16_000));

await using ITransaction tx = await client.Transactions.BeginAsync();

(Account account, bool hasValue) = await accounts.GetAsync(tx, 42);
account = account with { Balance = account.Balance + 500 };

await accounts.PutAsync(tx, 42, account);

Debug.Assert((await accounts.GetAsync(tx, 42)).Value.Balance == 16_500);

await tx.RollbackAsync();

Debug.Assert((await accounts.GetAsync(null, 42)).Value.Balance == 16_000);

public record Account(decimal Balance);
```

</TabItem>
</Tabs>

## Table API

To execute table operations on a specific table, you need to get a specific view of the table and use one of its methods. You can only create new tables by using SQL API.

When working with tables, you can use built-in Tuple type, which is a set of key-value pairs underneath, or map the data to your own types for a strongly-typed access. Here is how you can work with tables:

### Getting a Table Instance

To obtain an instance of a table, use the `ITables.GetTableAsync(string name)` You can also use `ITables.GetTablesAsync` method to list all existing tables.

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var existingTables = await Client.Tables.GetTablesAsync();
var firstTable = existingTables[0];

var myTable = await Client.Tables.GetTableAsync("MY_TABLE");
```

</TabItem>
</Tabs>

### Basic Table Operations

Once you've got a table you need to get a specific view to choose how you want to operate table records.

#### Tuple Record View

A tuple record view. It can be used to operate table tuples directly.

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
IRecordView<IIgniteTuple> view = table.RecordBinaryView;

IIgniteTuple fullRecord = new IgniteTuple
{
  ["id"] = 42,
  ["name"] = "John Doe"
};

await view.UpsertAsync(transaction: null, fullRecord);

IIgniteTuple keyRecord = new IgniteTuple { ["id"] = 42 };
(IIgniteTuple value, bool hasValue) = await view.GetAsync(transaction: null, keyRecord);

Debug.Assert(hasValue);
Debug.Assert(value.FieldCount == 2);
Debug.Assert(value["id"] as int? == 42);
Debug.Assert(value["name"] as string == "John Doe");
```

</TabItem>
</Tabs>

#### Record View

A record view mapped to a user type. It can be used to operate table using user objects which are mapped to table tuples.

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var pocoView = table.GetRecordView<Poco>();

await pocoView.UpsertAsync(transaction: null, new Poco(42, "John Doe"));
var (value, hasValue) = await pocoView.GetAsync(transaction: null, new Poco(42));

Debug.Assert(hasValue);
Debug.Assert(value.Name == "John Doe");

public record Poco(long Id, string? Name = null);
```

</TabItem>
</Tabs>

#### Key-Value Tuple View

A tuple key-value view. It can be used to operate table using key and value tuples separately.

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
IKeyValueView<IIgniteTuple, IIgniteTuple> kvView = table.KeyValueBinaryView;

IIgniteTuple key = new IgniteTuple { ["id"] = 42 };
IIgniteTuple val = new IgniteTuple { ["name"] = "John Doe" };

await kvView.PutAsync(transaction: null, key, val);
(IIgniteTuple? value, bool hasValue) = await kvView.GetAsync(transaction: null, key);

Debug.Assert(hasValue);
Debug.Assert(value.FieldCount == 1);
Debug.Assert(value["name"] as string == "John Doe");
```

</TabItem>
</Tabs>

#### Key-Value View

A key-value view with user objects. It can be used to operate table using key and value user objects mapped to table tuples.

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
IKeyValueView<long, Poco> kvView = table.GetKeyValueView<long, Poco>();

await kvView.PutAsync(transaction: null, 42, new Poco(Id: 0, Name: "John Doe"));
(Poco? value, bool hasValue) = await kvView.GetAsync(transaction: null, 42);

Debug.Assert(hasValue);
Debug.Assert(value.Name == "John Doe");

public record Poco(long Id, string? Name = null);
```

</TabItem>
</Tabs>

## Streaming Data

To stream a large amount of data, use the data streamer. Data streaming provides a quicker and more efficient way to load, organize and optimally distribute your data. Data streamer accepts a stream of data and distributes data entries across the cluster, where the processing takes place. Data streaming is available in all table views.

![Data Streaming](/img/data_streaming.png)

Data streaming provides at-least-once delivery guarantee.

### Using Data Streamer API

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var options = DataStreamerOptions.Default with { PageSize = 10 };
var data = Enumerable.Range(0, Count).Select(x => new IgniteTuple { ["id"] = 1L, ["name"] = "foo" }).ToList();

await TupleView.StreamDataAsync(data.ToAsyncEnumerable(), options);
```

</TabItem>
</Tabs>

## Client Metrics

Metrics are exposed by the .NET client through the `System.Diagnostics.Metrics` API with the `Apache.Ignite` meter name. For example, here is how you can access Ignite metrics by using the [dotnet-counters](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/dotnet-counters) tool:

```bash
dotnet-counters monitor --counters Apache.Ignite,System.Runtime --process-id PID
```

You can also get metrics in your code by creating a listener:

```csharp
var listener = new MeterListener();
listener.InstrumentPublished = (instrument, meterListener) =>
{
    if (instrument.Meter.Name == "Apache.Ignite")
    {
        meterListener.EnableMeasurementEvents(instrument);
    }
};
listener.SetMeasurementEventCallback<int>(
    (instrument, measurement, tags, state) => Console.WriteLine($"{instrument.Name}: {measurement}"));

listener.Start();
```

### Available .NET Metrics

| Metric name | Description |
|-------------|-------------|
| connections-active | The number of currently active connections. |
| connections-established | The number of established connections. |
| connections-lost | The number of connections lost. |
| connections-lost-timeout | The number of connections lost due to a timeout. |
| handshakes-failed | The number of failed handshakes. |
| handshakes-failed-timeout | The number of handshakes that failed due to a timeout. |
| requests-active | The number of currently active requests. |
| requests-sent | The number of requests sent. |
| requests-completed | The number of completed requests. Requests are completed once a response is received. |
| requests-retried | The number of request retries. |
| requests-failed | The number of failed requests. |
| bytes-sent | The amount of bytes sent. |
| bytes-received | The amount of bytes received. |
| streamer-batches-sent | The number of data streamer batches sent. |
| streamer-items-sent | The number of data streamer items sent. |
| streamer-batches-active | The number of existing data streamer batches. |
| streamer-items-queued | The number of queued data streamer items. |

## Logging

To enable logging, set the `IgniteClientConfiguration.LoggerFactory` property to an instance of the `Microsoft.Extensions.Logging.ILoggerFactory` standard API. See [Standard logging in .NET](https://docs.microsoft.com/en-us/dotnet/core/extensions/logging) to learn more.

### Examples

The example below shows how you can configure logging to console with the `Microsoft.Extensions.Logging.Console` package:

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var cfg = new IgniteClientConfiguration
{
    LoggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug))
};
```

</TabItem>
</Tabs>

Alternatively, here is how to configure logging with [Serilog](https://serilog.net/) by using `Serilog.Extensions.Logging` and `Serilog.Sinks.Console` packages:

<Tabs groupId="languages">
<TabItem value="dotnet" label=".NET">

```csharp
var cfg = new IgniteClientConfiguration
{
    LoggerFactory = LoggerFactory.Create(builder =>
        builder.AddSerilog(new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger()))
};
```

</TabItem>
</Tabs>
