# Client Internals

## Glossary

- **Node** or **Server** or **Server Node**: a single cluster member (see `IgniteImpl`)
  - Multiple nodes can run in a single JVM
- **Cluster**: a group of interconnected **nodes**
- **Client** (former "Thin Client"): a single instance of `IgniteClient` (see `TcpIgniteClient`)
  - A **client** connects to one or more **servers**
- **Channel** or **Connection**: a single TCP connection between a **client** and a **server** (see `TcpClientChannel`)

## Connection Management

### Initial Connection

When we call `IgniteClient.builder().addresses("foo:10800", "192.168.0.1").build()`, the following happens:
1. Build a list of **endpoints**. For each provided address:
   - Extract port if present, use default otherwise
   - Resolve DNS if necessary. A host name can resolve to one or more IP addresses.
2. Iterate over the list of **endpoints** and try to connect to each one until a connection is successfully established.

Once we have one connection, the client is fully functional, and we return the `IgniteClient` instance to the user.

### Additional Connections

