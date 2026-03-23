---
title: ADO.NET Integration
sidebar_label: ADO.NET
---

{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}

Apache Ignite implements [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) classes, such as `DbConnection`, `DbCommand`, `DbDataReader`, etc.,
allowing you to use standard ADO.NET components to interact with Ignite SQL.

## Getting Started

### Prerequisites

To use C# thin client, .NET 8.0 or newer is required.

### Installation

C# client is available via NuGet. To add it, use the `add package` command:

```bash
dotnet add package Apache.Ignite --version 3.0.0
```


## Connecting to Cluster

To connect to an Apache Ignite cluster, create a new connection with the connection string:

```csharp
var connStr = "Endpoints=localhost:10800";
```

The connection string has the following parameters:

| Parameter | Description |
|-----------|-------------|
| Endpoints | Required. Comma-separated list of server addresses with ports. |
| SocketTimeout | Time span for socket operations timeout in `hh:mm:ss` format. 30 seconds by default. |
| OperationTimeout | Time span for operation timeout in `hh:mm:ss` format. No timeout by default. |
| HeartbeatInterval | Time span between heartbeat messages to keep connection alive in `hh:mm:ss.f` format. 30 seconds by default. |
| ReconnectInterval | Time span between reconnection attempts in `hh:mm:ss` format. 30 seconds by default. |
| SslEnabled | Boolean value to enable/disable SSL encryption. `False` by default. |
| Username | Username for authentication. |
| Password | Password for authentication. |

The example below shows a complete connection string with all parameters

```text
Endpoints=localhost:10800,localhost:10801;SocketTimeout=00:00:10;OperationTimeout=00:03:30;
HeartbeatInterval=00:00:05.5;ReconnectInterval=00:01:00;SslEnabled=True;Username=user;Password=pass
```

Using the connection string, you can establish a connection to an Ignite cluster with the `IgniteDbConnection` class:

```csharp
var connStr = "Endpoints=localhost:10800";
await using var conn = new IgniteDbConnection(connStr);
await conn.OpenAsync();
```

## Executing SQL Commands

You can use the `IgniteDbConnection.CreateCommand` method to create a command and then execute it with one of the [execution commands](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/executing-a-command).

The example below does not expect the command to return any rows, and uses the `ExecuteNonQueryAsync` command.

```csharp
DbCommand cmd = conn.CreateCommand();
cmd.CommandText = "DROP TABLE IF EXISTS Person";
await cmd.ExecuteNonQueryAsync();
```

## Reading Data From Cluster

You can retrieve data from the cluster in a similar way to using [Data Readers](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/retrieving-data-using-a-datareader).

The example below shows how you can get data from your cluster:

```csharp
DbCommand cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM Person";
await using var reader = await cmd.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine($"Person [ID={reader.GetInt32(0)}, Name={reader.GetString(1)}]");
}
```

## Using Parameters

:::note
Apache Ignite only supports input parameters. Parameter types are automatically inferred from the SQL query context, so you do not need to specify the parameter type explicitly.
:::

Apache Ignite supports parameterized queries using positional parameters. You can use the `IgniteDbConnection.CreateParameter()` method to create parameters that will replace the `?` placeholders in your query text.

The example below shows how you can parametrize your query:

```csharp
DbCommand cmd = conn.CreateCommand();
cmd.CommandText = "INSERT INTO Person (ID, Name) VALUES (?, ?)";

DbParameter idParam = cmd.CreateParameter();
idParam.Value = 1;
cmd.Parameters.Add(idParam);

DbParameter nameParam = cmd.CreateParameter();
nameParam.Value = "John Doe";
cmd.Parameters.Add(nameParam);

await cmd.ExecuteNonQueryAsync();
```

Parameters must be added in the exact order they appear in the query. The first `?` corresponds to the first parameter added, the second `?` to the second parameter, etc.

To pass null values, set the parameter value to `null`:

```csharp
DbParameter param = cmd.CreateParameter();
param.Value = null;
cmd.Parameters.Add(param);
```



## Transactions

:::note
Apache Ignite does not support custom isolation levels. All transactions are effectively `Serializable`.
:::

You can use the `DbConnection.BeginTransaction` method to start a transaction.

No data will be committed to the database until the transaction is committed. You can discard all changes with a rollback method:

```csharp
await using DbTransaction tx = await conn.BeginTransactionAsync();
cmd.Transaction = tx;
// ...
// Commit the transaction.
await tx.CommitAsync();
// Roll back the transaction if needed.
// await tx.RollbackAsync();
```


## Full Example

The example below shows how you can work with an Apache Ignite cluster via ADO.NET:

```csharp
var connStr = $"Endpoints=localhost:10800";
await using var conn = new IgniteDbConnection(connStr);
await conn.OpenAsync();

DbCommand createTableCmd = conn.CreateCommand();
createTableCmd.CommandText = "CREATE TABLE IF NOT EXISTS Person (ID INT PRIMARY KEY, Name VARCHAR)";
await createTableCmd.ExecuteNonQueryAsync();

DbCommand insertCmd = conn.CreateCommand();
insertCmd.CommandText = "INSERT INTO Person (ID, Name) VALUES (?, ?)";

await using DbTransaction tx = await conn.BeginTransactionAsync();
insertCmd.Transaction = tx;

DbParameter idParam = insertCmd.CreateParameter();
insertCmd.Parameters.Add(idParam);

DbParameter nameParam = insertCmd.CreateParameter();
insertCmd.Parameters.Add(nameParam);

for (var i = 1; i <= 3; i++)
{
    idParam.Value = i;
    nameParam.Value = "Person " + i;
    await insertCmd.ExecuteNonQueryAsync();
}

await tx.CommitAsync();

DbCommand selectCmd = conn.CreateCommand();
selectCmd.CommandText = "SELECT * FROM Person WHERE ID > ?";

DbParameter selectParam = selectCmd.CreateParameter();
selectParam.Value = 1;
selectCmd.Parameters.Add(selectParam);

await using var reader = await selectCmd.ExecuteReaderAsync();

for (var i = 0; i < reader.FieldCount; i++)
{
    Console.WriteLine($"{reader.GetName(i)}: {reader.GetFieldType(i)}");
}

while (await reader.ReadAsync())
{
    int id = reader.GetInt32(0);
    string name = reader.GetString(1);

    Console.WriteLine($"Person [ID={id}, Name={name}]");
}
```
