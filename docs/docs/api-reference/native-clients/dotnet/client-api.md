---
title: Client API
id: client-api
sidebar_position: 1
---

# Client API

The Client API provides connection management and access to all Ignite 3 functionality. IIgniteClient serves as the entry point for interacting with an Ignite cluster from .NET applications.

## Key Concepts

IIgniteClient manages connections to cluster nodes and provides access to tables, SQL, transactions, compute, and other Ignite APIs. The client automatically maintains connections to multiple nodes for high availability and load balancing.

### Connection Management

The client maintains a pool of connections to cluster nodes. When you specify multiple endpoints in the configuration, the client connects to all available nodes and distributes requests across them. If a node fails, the client automatically reconnects using the configured reconnect interval.

### Thread Safety

All IIgniteClient operations are thread-safe. You can share a single client instance across your application and call methods from multiple threads without additional synchronization.

## Usage Examples

### Basic Connection

```csharp
using Apache.Ignite;

var cfg = new IgniteClientConfiguration("localhost:10800");
using var client = await IgniteClient.StartAsync(cfg);
```

### Multiple Endpoints

```csharp
var cfg = new IgniteClientConfiguration
{
    Endpoints = { "node1:10800", "node2:10800", "node3:10800" }
};
using var client = await IgniteClient.StartAsync(cfg);
```

### Connection Lifecycle

```csharp
var cfg = new IgniteClientConfiguration("localhost:10800")
{
    SocketTimeout = TimeSpan.FromSeconds(60),
    HeartbeatInterval = TimeSpan.FromSeconds(30),
    ReconnectInterval = TimeSpan.FromSeconds(10)
};

using var client = await IgniteClient.StartAsync(cfg);

// Check active connections
var connections = client.GetConnections();
foreach (var conn in connections)
{
    Console.WriteLine($"Connected to {conn.Node.Name}");
}
```

### Logging Configuration

```csharp
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

var cfg = new IgniteClientConfiguration("localhost:10800")
{
    LoggerFactory = loggerFactory
};

using var client = await IgniteClient.StartAsync(cfg);
```

### Operation Timeout

```csharp
var cfg = new IgniteClientConfiguration("localhost:10800")
{
    OperationTimeout = TimeSpan.FromSeconds(30)
};

using var client = await IgniteClient.StartAsync(cfg);
```

This timeout applies to individual operations. Long-running queries or transactions are not affected unless they involve multiple round-trips to the cluster.

### Retry Policy

```csharp
using Apache.Ignite;

var cfg = new IgniteClientConfiguration("localhost:10800")
{
    RetryPolicy = new RetryReadPolicy()
};

using var client = await IgniteClient.StartAsync(cfg);
```

The retry policy controls automatic retry behavior for failed requests. RetryReadPolicy retries only read operations, while RetryNonePolicy disables retries.

### Accessing Core APIs

```csharp
using var client = await IgniteClient.StartAsync(cfg);

// Tables API
var tables = client.Tables;
var table = await tables.GetTableAsync("my_table");

// SQL API
var sql = client.Sql;
var resultSet = await sql.ExecuteAsync(null, "SELECT * FROM my_table");

// Transactions API
var transactions = client.Transactions;
var tx = await transactions.BeginAsync();

// Compute API
var compute = client.Compute;
```

## Reference

### IIgniteClient Interface

The main client interface provides:

- **Configuration** - Access to the client configuration used at startup
- **GetConnections()** - Returns active connections to cluster nodes
- **Tables** - Access to table management and data operations
- **Sql** - SQL query execution
- **Transactions** - Transaction management
- **Compute** - Distributed computing

### IgniteClientConfiguration

Configuration properties:

- **Endpoints** - List of cluster node addresses to connect to
- **LoggerFactory** - Microsoft.Extensions.Logging.ILoggerFactory for diagnostics
- **SocketTimeout** - Timeout for socket operations including handshake and heartbeats (default: 30 seconds)
- **OperationTimeout** - Timeout for individual operations (default: infinite)
- **HeartbeatInterval** - Interval between heartbeat messages (default: 30 seconds)
- **ReconnectInterval** - Interval for background reconnection attempts (default: 30 seconds)
- **RetryPolicy** - Policy for retrying failed requests (default: RetryReadPolicy)
- **SslStreamFactory** - Factory for creating SSL streams when SSL is required
- **Authenticator** - Authentication handler for cluster access

### Constants

- **DefaultPort** - 10800
- **DefaultSocketTimeout** - 30 seconds
- **DefaultHeartbeatInterval** - 30 seconds
- **DefaultReconnectInterval** - 30 seconds
