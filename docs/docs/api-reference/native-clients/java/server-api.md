---
title: Server API
id: server-api
sidebar_position: 2
---

# Server API

The Ignite interface provides access to an embedded cluster node. Applications using the embedded approach run as full cluster participants that store data, process queries, and participate in cluster operations. This contrasts with thin clients that connect remotely without storing data.

## Key Concepts

An embedded node initializes as part of the application process. The node joins the cluster during startup and provides direct access to all cluster subsystems. Since the node stores data locally, operations execute without network hops when accessing colocated data.

The Ignite interface serves as the primary entry point for all operations. Applications obtain references to tables, SQL engines, transaction managers, compute capabilities, and catalog management through this interface.

## Node Initialization

Start an embedded node using the IgniteServer start method:

```java
IgniteServer server = IgniteServer.start(
    "myNode",
    Path.of("/config/ignite-config.conf"),
    Path.of("/work/dir")
);

server.initCluster(
    InitParameters.builder()
        .metaStorageNodes(server)
        .cmgNodes(server)
        .build()
);

Ignite ignite = server.api();
String nodeName = ignite.name();
System.out.println("Node started: " + nodeName);

// Use node for operations
```

The start method requires a node name, configuration file path, and work directory. The node name must be unique within the cluster. After starting, initialize the cluster and obtain the Ignite API through the api() method.

## Resource Access

Access cluster resources through the Ignite interface:

```java
IgniteTables tables = ignite.tables();
IgniteSql sql = ignite.sql();
IgniteTransactions transactions = ignite.transactions();
IgniteCompute compute = ignite.compute();
IgniteCatalog catalog = ignite.catalog();
IgniteCluster cluster = ignite.cluster();
```

Each accessor returns a facade for the corresponding subsystem. These facades remain valid for the node lifetime.

## Table Operations

Obtain table references for data operations:

```java
Table users = ignite.tables().table("users");
if (users != null) {
    RecordView<Tuple> view = users.recordView();
    // Perform operations
}
```

The table method returns null if the table does not exist.

## SQL Execution

Execute SQL queries using the SQL facade:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    "SELECT * FROM users WHERE age > ?",
    25
)) {
    while (rs.hasNext()) {
        SqlRow row = rs.next();
        System.out.println(row.stringValue("name"));
    }
}
```

Pass null as the transaction parameter for auto-commit execution.

## Transaction Management

Manage transactions through the transaction facade:

```java
ignite.transactions().runInTransaction(tx -> {
    Table table = ignite.tables().table("accounts");
    RecordView<Tuple> view = table.recordView();

    Tuple key = Tuple.create().set("id", 1);
    Tuple record = view.get(tx, key);

    record.set("balance", record.intValue("balance") + 100);
    view.put(tx, record);
});
```

The runInTransaction method automatically commits on normal completion and rolls back on exceptions.

## Compute Operations

Submit compute jobs for distributed execution:

```java
JobDescriptor<String, Integer> descriptor =
    JobDescriptor.of("com.example.WordCountJob");

CompletableFuture<JobExecution<Integer>> execution =
    ignite.compute().submitAsync(
        JobTarget.anyNode(ignite.cluster().nodes()),
        descriptor,
        "input text"
    );

Integer result = execution
    .thenCompose(JobExecution::resultAsync)
    .join();
```

The compute facade submits jobs to cluster nodes based on the specified targeting strategy.

## Catalog Management

Manage schema definitions through the catalog facade:

```java
TableDefinition definition = TableDefinition.builder("products")
    .columns(
        ColumnDefinition.column("id", ColumnType.INT32),
        ColumnDefinition.column("name", ColumnType.STRING)
    )
    .primaryKey("id")
    .build();

ignite.catalog().createTableAsync(definition).join();
```

The catalog facade provides programmatic schema management without executing DDL statements.

## Cluster Information

Access cluster topology information:

```java
Collection<ClusterNode> nodes = ignite.cluster().nodes();
for (ClusterNode node : nodes) {
    NetworkAddress address = node.address();
    System.out.println("Node: " + node.name() + " at " + address.host() + ":" + address.port());
}

ClusterNode local = ignite.cluster().localNode();
System.out.println("Local node: " + local.name());
```

The cluster().nodes() method returns all active cluster members. The cluster facade provides access to local node information and cluster-wide operations.

## Lifecycle Management

Properly shut down the node to release resources:

```java
IgniteServer server = IgniteServer.start("node1", configPath, workDir);
server.initCluster(InitParameters.builder()
    .metaStorageNodes(server)
    .cmgNodes(server)
    .build());

Ignite ignite = server.api();
// Use node

server.shutdown();
```

Shutting down the node stops local subsystems and removes the node from the cluster. Note that IgniteServer provides shutdown() methods, while the Ignite interface does not implement AutoCloseable.

## Reference

- Server interface: `org.apache.ignite.IgniteServer`
- API interface: `org.apache.ignite.Ignite`
- Network types: `org.apache.ignite.network.NetworkAddress`, `org.apache.ignite.network.ClusterNode`

### IgniteServer Methods

- `static IgniteServer start(String nodeName, Path configPath, Path workDir)` - Start an embedded node
- `void initCluster(InitParameters parameters)` - Initialize the cluster
- `Ignite api()` - Get the Ignite API facade
- `void shutdown()` - Stop the node
- `String name()` - Returns the node name

### Ignite API Methods

- `String name()` - Returns the node name
- `IgniteTables tables()` - Access table management
- `IgniteTransactions transactions()` - Access transaction management
- `IgniteSql sql()` - Access SQL query engine
- `IgniteCompute compute()` - Access compute job execution
- `IgniteCatalog catalog()` - Access catalog management
- `IgniteCluster cluster()` - Access cluster information

### Subsystem Facades

- `IgniteTables` - Table discovery and access
- `IgniteTransactions` - Transaction lifecycle management
- `IgniteSql` - SQL query execution
- `IgniteCompute` - Distributed job execution
- `IgniteCatalog` - Schema definition management
- `IgniteCluster` - Cluster topology and node information

### Network Types

- `NetworkAddress` - Represents a network address with host and port
  - `String host()` - Returns the host name
  - `int port()` - Returns the port number
- `ClusterNode` - Represents a node in the cluster
  - `String name()` - Returns the node name
  - `NetworkAddress address()` - Returns the network address
  - `UUID id()` - Returns the node ID
