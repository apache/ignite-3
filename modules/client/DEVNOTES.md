# Client Internals

## Glossary

- **Node** or **Server** or **Server Node**: a single cluster member (see `IgniteImpl`)
  - Multiple nodes can run in a single JVM
- **Cluster**: a group of interconnected **nodes**
- **Client** (former "Thin Client"): a single instance of `IgniteClient` (see `TcpIgniteClient`)
  - A **client** connects to one or more **servers**
- **Channel** or **Connection**: a single TCP connection between a **client** and a **server** (see `TcpClientChannel`)

## Connection Management

### Overview

Three distinct and independent concerns exist around connections:
1. Get server addresses
2. Establish connections
3. Choose a connection for an operation

TBD continue.

### Initial Connection

When we call `IgniteClient.builder().addresses("foo:10800", "192.168.0.1").build()`, the following happens:
1. Build a list of **endpoints**. For each provided address:
   - Extract port if present, use default otherwise
   - Resolve DNS if necessary. A host name can resolve to one or more IP addresses.
2. Iterate over the list of **endpoints** and try to connect to each one until a connection is successfully established.

Once we have one connection, the client is fully functional, and we return the `IgniteClient` instance to the user.

### Additional Connections

Additional connections are established in the background after the initial connection is made.

### Design Considerations
* Iterate over the endpoints in the order they were provided. This allows users to prioritize certain addresses.
* Return the client as soon as one connection is established. This minimizes startup time.

##### Why Not Randomize the Endpoint Order?
We can imagine a situation where the user app comes up and many client instances initialize at the same time, 
flooding the first server in the list with connection attempts. We discussed and rejected the idea to randomize the endpoint order:

* We want to allow users to prioritize certain addresses.
* Connections are cheap and quick to establish. Secondary connections will be created shortly after the initial connection.

