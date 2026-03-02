---
title: Network API
id: network-api
sidebar_position: 10
---

# Network API

The Network API provides access to cluster topology information. Applications use this API to discover nodes, inspect network addresses, and access node metadata. This information supports compute job targeting, monitoring, and cluster awareness.

## Key Concepts

ClusterNode represents individual nodes in the cluster. Each node has a unique identifier, consistent name, network address, and metadata. The IgniteCluster facade provides access to topology information including all cluster members and the local node.

Network addresses identify node endpoints using host and port combinations. Applications parse addresses from strings or construct them programmatically.

## Cluster Access

Access cluster topology through the cluster facade:

```java
IgniteCluster cluster = ignite.cluster();

Collection<ClusterNode> nodes = cluster.nodes();
System.out.println("Cluster has " + nodes.size() + " nodes");

for (ClusterNode node : nodes) {
    System.out.println("Node: " + node.name() + " at " + node.address());
}
```

## Asynchronous Node Discovery

Retrieve nodes asynchronously:

```java
CompletableFuture<Collection<ClusterNode>> nodesFuture = cluster.nodesAsync();

nodesFuture.thenAccept(nodes -> {
    for (ClusterNode node : nodes) {
        System.out.println("Found node: " + node.name());
    }
});
```

Asynchronous access avoids blocking when topology information requires network calls.

## Local Node Information

Access the local node:

```java
ClusterNode local = ignite.cluster().localNode();

System.out.println("Local node ID: " + local.id());
System.out.println("Local node name: " + local.name());
System.out.println("Local address: " + local.address());
```

The local node represents the current Ignite instance within the cluster.

## Node Identification

Access node identifiers:

```java
ClusterNode node = cluster.localNode();

UUID nodeId = node.id();
String nodeName = node.name();

System.out.println("Node ID: " + nodeId);
System.out.println("Node name: " + nodeName);
```

The node ID uniquely identifies the node. The node name provides a human-readable consistent identifier.

## Network Addresses

Access node network endpoints:

```java
ClusterNode node = cluster.localNode();
NetworkAddress address = node.address();

String host = address.host();
int port = address.port();

System.out.println("Host: " + host);
System.out.println("Port: " + port);
```

NetworkAddress identifies the node endpoint for client connections.

## Address Construction

Create network addresses programmatically:

```java
NetworkAddress address1 = new NetworkAddress("localhost", 10800);

NetworkAddress address2 = NetworkAddress.from("192.168.1.100:10800");

InetSocketAddress socketAddress = new InetSocketAddress("server.example.com", 10800);
NetworkAddress address3 = NetworkAddress.from(socketAddress);
```

The from method parses addresses from strings or socket addresses.

## Node Metadata

Access node metadata:

```java
ClusterNode node = cluster.localNode();
NodeMetadata metadata = node.nodeMetadata();

// Access metadata properties
// (specific metadata content depends on configuration)
```

Node metadata contains additional node-specific information configured during cluster setup.

## Client Connections

Thin clients can access active connections:

```java
List<ClusterNode> connections = client.connections();

System.out.println("Connected to " + connections.size() + " servers");

for (ClusterNode node : connections) {
    System.out.println("Connected to: " + node.name() +
        " at " + node.address());
}
```

The connections list shows servers with active client connections.

## Node Selection

Select specific nodes for operations:

```java
Collection<ClusterNode> allNodes = ignite.clusterNodes();

// Find node by name
ClusterNode targetNode = allNodes.stream()
    .filter(node -> node.name().equals("node-1"))
    .findFirst()
    .orElse(null);

if (targetNode != null) {
    System.out.println("Found node at " + targetNode.address());
}
```

## Multiple Node Selection

Filter nodes by criteria:

```java
Collection<ClusterNode> allNodes = ignite.clusterNodes();

// Select nodes by port
List<ClusterNode> portFiltered = allNodes.stream()
    .filter(node -> node.address().port() == 10800)
    .collect(Collectors.toList());

// Select nodes by hostname pattern
List<ClusterNode> hostFiltered = allNodes.stream()
    .filter(node -> node.address().host().contains("prod"))
    .collect(Collectors.toList());
```

## Compute Job Targeting

Use node information for compute operations:

```java
Collection<ClusterNode> nodes = ignite.clusterNodes();

JobDescriptor<String, Integer> descriptor =
    JobDescriptor.<String, Integer>builder("com.example.DataProcessor").build();

// Execute on all nodes
for (ClusterNode node : nodes) {
    CompletableFuture<JobExecution<Integer>> executionFuture =
        ignite.compute().submitAsync(
            JobTarget.node(node),
            descriptor,
            "input"
        );

    executionFuture.thenCompose(JobExecution::resultAsync)
        .thenAccept(result -> {
            System.out.println("Job result from " + node.name() + ": " + result);
        });
}
```

## Address Parsing

Parse addresses from configuration strings:

```java
String[] serverAddresses = {
    "server1.example.com:10800",
    "server2.example.com:10800",
    "192.168.1.100:10800"
};

List<NetworkAddress> addresses = Arrays.stream(serverAddresses)
    .map(NetworkAddress::from)
    .collect(Collectors.toList());

for (NetworkAddress addr : addresses) {
    System.out.println("Server: " + addr.host() + ":" + addr.port());
}
```

## Address Formatting

Format addresses for display:

```java
NetworkAddress address = node.address();

String formatted = address.host() + ":" + address.port();
System.out.println("Node endpoint: " + formatted);

// Address toString provides formatted output
String automatic = address.toString();
```

## Deprecated API

The clusterNodes method on Ignite is deprecated:

```java
// Deprecated
Collection<ClusterNode> nodes1 = ignite.clusterNodes();

// Preferred
Collection<ClusterNode> nodes2 = ignite.cluster().nodes();
```

Use the cluster facade for topology access.

## Node Comparison

Compare nodes by identity:

```java
ClusterNode node1 = cluster.localNode();
ClusterNode node2 = nodes.iterator().next();

boolean same = node1.id().equals(node2.id());
if (same) {
    System.out.println("Same node");
}
```

Compare nodes using their UUID identifiers.

## Reference

- Cluster facade: `org.apache.ignite.network.IgniteCluster`
- Node representation: `org.apache.ignite.network.ClusterNode`
- Network address: `org.apache.ignite.network.NetworkAddress`
- Node metadata: `org.apache.ignite.network.NodeMetadata`

### IgniteCluster Methods

- `Collection<ClusterNode> nodes()` - Get all cluster nodes
- `CompletableFuture<Collection<ClusterNode>> nodesAsync()` - Async get nodes
- `ClusterNode localNode()` - Get local node

### ClusterNode Methods

- `UUID id()` - Get node unique identifier
- `String name()` - Get node name (consistent ID)
- `NetworkAddress address()` - Get network address
- `NodeMetadata nodeMetadata()` - Get node metadata

### NetworkAddress Methods

- `String host()` - Get hostname
- `int port()` - Get port number

### NetworkAddress Construction

- `NetworkAddress(String host, int port)` - Create from components
- `static NetworkAddress from(String)` - Parse from "host:port" string
- `static NetworkAddress from(InetSocketAddress)` - Convert from socket address

### IgniteClient Connection Methods

- `List<ClusterNode> connections()` - Get active server connections

### Ignite Node Methods

- `Collection<ClusterNode> clusterNodes()` - Get all nodes (deprecated, use cluster().nodes())

### Topology Use Cases

Node information supports several common patterns:
- Compute job targeting based on node location or capabilities
- Monitoring and diagnostics of cluster health
- Connection management for thin clients
- Custom load balancing and data locality optimization
