# Metastorage

The module for storing and accessing metadata. This module is linked to another one:

- metastorage-api - The module contains classes that other components can use to access the metastorage service.

To avoid data loss, the storage is distributed using the RAFT consensus algorithm. Every cluster node has access to such distributed
storage, but the majority of members are learners, and only a small number are voters in terms of RAFT algorithm. Typically, only a small
number of the cluster's voting nodes make up the raft-group (number of nodes must be odd, either 3 or 5.). The remaining nodes listen to
metadata updates but do not vote (they called learners in terms of RAFT).

## Threading model

There is only one thread specifically for notifying watchers. The thread is created in KeyValueStorage from the executor:

```java
private ExecutorService watchExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(NamedThreadFactory
        .threadPrefix(nodeName, "metastorage-watch-executor"),LOG));
```

Additionally, the storage internally has two thread executors for creating a snapshot:

```java
private ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2,new NamedThreadFactory(NamedThreadFactory
        .threadPrefix(nodeName, "metastorage-snapshot-executor"),LOG));
```

To distinguish to which node the specific thread belongs, each executor has a node name in its prefix.

### Interface methods

Futures are returned by a number of Metastorage Manager operations, like get(), getAll(), invoke(), etc. Those futures are completed when
the corresponding RAFT command is completed in the Metastorage group. As the entire replication process takes place in RAFT threads, this
result appears in the RAFT client executor with prefix <NODE_NAME>%Raft-Group-Client. See RAFT module for more information about its
threading model.

Although some methods return futures, they are often run synchronously. Futures are dependent on asynchronous Metastorage initialization,
since starting an RAFT group requires time. In other words, we have a chance to complete those features in the RAFT client executor thread (
prefix NODE NAME>%Raft-Group-Client).

### Using common pool

The component uses common ForkJoinPool on start (in fact, it is not necessary, because all components start asynchronously in the same
ForkJoinPool). The using of the common pool is dangerous, because the pool can be occupied by another threads that hosted on the same JVM 
