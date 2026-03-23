---
title: Client API
id: client-api
sidebar_position: 1
---

# Client API

The IgniteClient provides a lightweight connection to an Ignite cluster. Applications use the client to access data and execute operations without running a full cluster node. The client maintains connection pools, handles automatic reconnection, and supports authentication and TLS encryption.

## Key Concepts

The client implements the Ignite interface and adds connection management capabilities. Unlike embedded nodes, clients do not store data or participate in cluster consensus. Clients connect to server nodes via a binary protocol over TCP.

The client builder pattern configures connection parameters before establishing cluster connectivity. Once created, the client provides access to tables, SQL, transactions, compute, and catalog APIs.

## Connection Configuration

Configure the client using the builder:

```java
try (IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800", "server2:10800")
    .connectTimeout(5000)
    .operationTimeout(30000)
    .heartbeatInterval(3000)
    .heartbeatTimeout(5000)
    .retryPolicy(new RetryReadPolicy())
    .build()) {

    String nodeName = client.name();
    System.out.println("Connected to: " + nodeName);
}
```

The `addresses()` method accepts multiple server endpoints. The client attempts connections in order and maintains active connections to all reachable servers for load distribution.

## Connection Resilience

The client automatically reconnects when connections fail. Configure reconnection behavior with timeout and interval settings:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("server1:10800", "server2:10800")
    .backgroundReconnectInterval(1000)
    .build();
```

The `backgroundReconnectInterval()` parameter controls how frequently the client attempts to restore lost connections.

## Retry Policies

Retry policies determine which failed operations should be retried. Use RetryReadPolicy to retry read-only operations while preventing duplicate writes:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .retryPolicy(new RetryReadPolicy())
    .build();
```

Create custom retry policies by implementing the RetryPolicy interface. The policy receives context about the failed operation and returns whether to retry.

## Authentication

Configure authentication using the authenticator builder parameter:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .authenticator(BasicAuthenticator.builder()
        .username("username")
        .password("password")
        .build())
    .build();
```

BasicAuthenticator provides username and password authentication. The client sends credentials during connection establishment.

## TLS Configuration

Enable TLS for encrypted client-server communication:

```java
SslConfiguration ssl = SslConfiguration.builder()
    .enabled(true)
    .trustStorePath("/path/to/truststore.jks")
    .trustStorePassword("password")
    .build();

IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .ssl(ssl)
    .build();
```

Configure keystore settings when using client certificate authentication.

## Asynchronous Connection

Build the client asynchronously to avoid blocking during connection establishment:

```java
CompletableFuture<IgniteClient> clientFuture = IgniteClient.builder()
    .addresses("localhost:10800")
    .buildAsync();

clientFuture.thenAccept(client -> {
    // Use client
}).exceptionally(ex -> {
    // Handle connection failure
    return null;
});
```

Asynchronous building returns immediately while the client establishes connections in the background.

## Active Connections

Retrieve information about active server connections:

```java
List<ClusterNode> connections = client.connections();
for (ClusterNode node : connections) {
    System.out.println("Connected to: " + node.name());
}
```

The connections list reflects currently active client-server connections.

## Resource Management

Close the client to release network connections and resources:

```java
try (IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .build()) {

    // Use client
} // Automatically closed
```

Use try-with-resources to ensure proper cleanup. Closing the client terminates all active operations and connections.

## Configuration Access

Access the current client configuration:

```java
IgniteClientConfiguration config = client.configuration();
long timeout = config.operationTimeout();
```

The configuration object provides read-only access to connection settings.

## Reference

- Key interface: `org.apache.ignite.client.IgniteClient`
- Builder: `IgniteClient.Builder`
- Configuration: `org.apache.ignite.client.IgniteClientConfiguration`
- Authentication: `org.apache.ignite.client.IgniteClientAuthenticator`, `org.apache.ignite.client.BasicAuthenticator`
- TLS: `org.apache.ignite.client.SslConfiguration`
- Retry: `org.apache.ignite.client.RetryPolicy`, `org.apache.ignite.client.RetryReadPolicy`

### Builder Configuration Methods

- `addresses(String...)` - Server endpoints (host:port format)
- `connectTimeout(long)` - Socket connection timeout in milliseconds
- `operationTimeout(long)` - Default operation timeout in milliseconds
- `heartbeatInterval(long)` - Heartbeat message interval in milliseconds
- `heartbeatTimeout(long)` - Heartbeat timeout in milliseconds
- `backgroundReconnectInterval(long)` - Reconnection attempt interval in milliseconds
- `retryPolicy(RetryPolicy)` - Retry policy for failed operations
- `authenticator(IgniteClientAuthenticator)` - Authentication configuration
- `ssl(SslConfiguration)` - TLS configuration
- `metricsEnabled(boolean)` - Enable JMX metrics collection
- `loggerFactory(LoggerFactory)` - Custom logger factory
- `build()` - Create client synchronously
- `buildAsync()` - Create client asynchronously
