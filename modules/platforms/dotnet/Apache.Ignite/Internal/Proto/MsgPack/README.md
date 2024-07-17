# Ignite-specific MessagePack Implementation

[MessagePack](https://msgpack.org/) is used in Ignite client protocol to serialize non-user data (as opposed to user data, which uses [BinaryTuple](https://cwiki.apache.org/confluence/display/IGNITE/IEP-92%3A+Binary+Tuple+Format)),
such as request/response headers, table schemas, metadata, and so on.

Initially, we used [MessagePack library from neuecc](https://github.com/neuecc/MessagePack-CSharp). 
However, there were two reasons to get rid of the dependency and create our own implementation:
* Ignite uses a basic subset of MessagePack protocol, and only through a low-level reader/writer API.
* Library API works in a way that requires extra allocations and copying, affecting performance (`IBufferWriter` interface causes allocation, reading raw bytes requires a copy).

![ResultSet.ReadRow profiling](https://issues.apache.org/jira/secure/attachment/13053836/ReadBytesAsMemory.png)
