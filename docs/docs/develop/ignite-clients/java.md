---
id: java-client
title: Java Client
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ignite 3 clients connect to the cluster via a standard socket connection. Unlike Ignite 2.x, there are no separate Thin and Thick clients in Ignite 3. All clients are 'thin'.

Clients do not become a part of the cluster topology, never hold any data, and are not used as a destination for compute calculations.

## Getting Started

### Prerequisites

To use Java thin client, Java 11 or newer is required.

### Installation

Java client can be added to your project by using maven:

```xml
<dependency>
    <groupId>org.apache.ignite</groupId>
    <artifactId>ignite-client</artifactId>
    <version>3.0.0</version>
</dependency>
```

## Connecting to Cluster

To initialize a client, use the `IgniteClient` class, and provide it with the configuration:

<Tabs groupId="languages">
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder()
  .addresses("127.0.0.1:10800")
  .build()
) {
  // Your code goes here
}
```

</TabItem>
</Tabs>

## Authentication

To pass [authentication](/3.1.0/configure-and-operate/configuration/config-authentication) information, use the `IgniteClientAuthenticator` class and pass it to `IgniteClient` builder:

<Tabs groupId="languages">
<TabItem value="java" label="Java">

```java
IgniteClientAuthenticator auth = BasicAuthenticator.builder().username("myUser").password("myPassword").build();
IgniteClient.builder()
    .addresses("127.0.0.1:10800")
    .authenticator(auth)
    .build();
```

</TabItem>
</Tabs>

## Logging

To configure client logging, add `loggerFactory`:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("127.0.0.1")
    .loggerFactory(System::getLogger)  // Optional: this is the default
    .build();
```

The client logs connection errors, reconnects, and retries. By default, logging routes to `java.util.logging` (JUL) at INFO level.

For detailed configuration with Logback, Log4j2, or JUL, see [Java Client Logging](../work-with-data/java-client-logging).

## Client Metrics

### Java

When running Java client, you need to enable metrics in the client builder:

```java
IgniteClient client = IgniteClient.builder()
  .addresses("127.0.0.1:10800")
  .metricsEnabled(true)
  .build();
```

After that, client metrics will be available to any Java monitoring tool, for example [JDK Mission Control](https://www.oracle.com/java/technologies/jdk-mission-control.html).

#### Available Java Metrics

| Metric name | Description |
|-------------|-------------|
| ConnectionsActive | The number of currently active connections. |
| ConnectionsEstablished | The number of established connections. |
| ConnectionsLost | The number of connections lost. |
| ConnectionsLostTimeout | The number of connections lost due to a timeout. |
| HandshakesFailed | The number of failed handshakes. |
| HandshakesFailedTimeout | The number of handshakes that failed due to a timeout. |
| RequestsActive | The number of currently active requests. |
| RequestsSent | The number of requests sent. |
| RequestsCompleted | The number of completed requests. Requests are completed once a response is received. |
| RequestsRetried | The number of request retries. |
| RequestsFailed | The number of failed requests. |
| BytesSent | The amount of bytes sent. |
| BytesReceived | The amount of bytes received. |
| StreamerBatchesSent | The number of data streamer batches sent. |
| StreamerItemsSent | The number of data streamer items sent. |
| StreamerBatchesActive | The number of in-flight data streamer batches. |
| StreamerItemsQueued | The number of queued data streamer items. |

## Client Connection Configuration

There is a number of configuration properties managing the connection between the client and Ignite cluster:

```java
IgniteClient client = IgniteClient.builder()
  .addresses("127.0.0.1:10800")
  .connectTimeout(5000)
  .heartbeatInterval(30000)
  .heartbeatTimeout(5000)
  .operationTimeout(3000)
  .backgroundReconnectInterval(30000)
  .retryPolicy(new RetryLimitPolicy().retryLimit(8))
  .build();
```

| Configuration name | Description |
|--------------------|-------------|
| connectTimeout | Client connection timeout, in milliseconds. |
| heartbeatInterval | Heartbeat message interval, in milliseconds. |
| heartbeatTimeout | Heartbeat message timeout, in milliseconds. |
| operationTimeout | Operation timeout, in milliseconds. |
| backgroundReconnectInterval | Background reconnect interval, in milliseconds. |
| retryPolicy | Retry policy. By default, all read operations are retried up to 16 times, and write operations are not retried. |
