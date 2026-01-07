# Client Internals

This document describes the internal architecture and design decisions of the Apache Ignite 3 client.

## Glossary

- **Node** / **Server** / **Server Node**: A single cluster member (see `IgniteImpl`)
  - Multiple nodes can run in a single JVM
- **Cluster**: A group of interconnected nodes
- **Client** (formerly "Thin Client"): A single instance of `IgniteClient` (see `TcpIgniteClient`)
  - A client connects to one or more servers
- **Channel** / **Connection**: A single TCP connection between a client and a server (see `TcpClientChannel`)

---

## Connection Management

### Overview

Connection management involves three primary concerns:

1. **Address Resolution**: Get server addresses
2. **Connection Establishment**: Establish connections using resolved addresses
3. **Connection Selection**: Choose an appropriate connection for each operation

These concerns are handled concurrently:

- Background executor refreshes server addresses (periodically and/or on specific events)
- Background executor establishes new connections and maintains existing ones (reconnects if necessary)
- Operation threads select connections for operations

### Connection Lifecycle

#### Initial Connection

When `IgniteClient.builder().addresses("foo:10800", "192.168.0.1").build()` is called:

1. **Build endpoint list** - For each provided address:
   - Extract port if present, otherwise use default port
   - Resolve DNS if necessary (a hostname can resolve to multiple IP addresses)

2. **Establish first connection** - Iterate over endpoints and attempt to connect to each one until successful

Once the first connection is established, the client is fully functional and the `IgniteClient` instance is returned to the user.

#### Additional Connections

Additional connections are established in the background after the initial connection is made. This provides redundancy and load distribution.

#### Connection Maintenance

Each connection exchanges periodic heartbeats with the server:

- If a heartbeat fails, the connection is considered broken and closed
- A background task attempts to re-establish the connection

#### Server Discovery

New server addresses can be discovered through two mechanisms:

1. The configured `IgniteClientConfiguration.addressesFinder`
2. DNS resolution of provided addresses

When new addresses are discovered:

- They are added to the list of known server endpoints
- New connections are established to these endpoints in the background

### Connection Selection Strategy

For each operation, the client selects a connection using the following algorithm:

1. **Check operation type**: Is it a single-key operation (e.g., `get`, `put`, `remove`)?
   - **Yes**:
     - Use partition awareness logic to find the target node name
     - If an active connection to that node exists, use it
     - Otherwise, proceed to step 2
   - **No**: Proceed to step 2

2. **Round-robin selection**: Pick an active connection using round-robin strategy

3. **Fallback**: If no active connections exist, attempt to establish a new connection to one of the known server endpoints

---

## Design Considerations

### Connection Ordering

**Endpoints are iterated in the order they were provided.** This design allows users to prioritize certain addresses (e.g., prefer local servers over remote ones).

#### Why Not Randomize the Endpoint Order?

In scenarios where many client instances initialize simultaneously (e.g., application startup), the first server in the list could be flooded with connection attempts. Despite this concern, endpoint randomization was rejected for the following reasons:

- **User control**: Users should be able to prioritize certain addresses
- **Performance**: Connections are cheap and quick to establish; secondary connections are created shortly after the initial connection

### Startup Optimization

**The client is returned as soon as one connection is established.** This minimizes startup time and allows applications to begin operations immediately.

### Connection Reuse

**The most suitable connection is chosen for each operation among available active connections.** The client avoids establishing new connections when existing ones can serve the operation.

