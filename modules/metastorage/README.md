# Metastorage

The module for store and access to metadata. There is module is connected with this one:

- metastorage-api - the module contains classes to access to metastorage service from another components.

The storage is distributed to prevent losing data and based of RAFT. That distributed storage persists on top of every cluster node, however
most of such participants are learners and only few of them are voting ones. In typical case, the group consists of only several voted nodes
of the cluster (amount of nodes have to be odd 3 or 5). The rest of nodes listens metadata update but does not participant in voting (RAFT 
learners).

## Threading model

There is only one dedicate thread to notify watches. This is a thread with prefix metastorage-watch-executor.
The thread is created from executor in KeyValueStorage:

```java
private ExecutorService watchExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(NamedThreadFactory
        .threadPrefix(nodeName, "metastorage-watch-executor"),LOG));
```

Also, the storage internally contains a two threads executor to create a snapshot:

```java
private ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2,new NamedThreadFactory(NamedThreadFactory
        .threadPrefix(nodeName, "metastorage-snapshot-executor"),LOG));
```

Both of the executors have a node name in their prefixes to distinguish to which node the particular thread belongs.

### Interface methods

Various operations (*get()*, *getAll()*, *invoke()*) in Metastorage manager return futures. Those futures are completed when the matched
RAFT command completes to the Metastorage group. This result appears in RAFT client executor (prefix <NODE_NAME>%Raft-Group-Client), but the
entire replication procedure is happened tin the RAFT threads (description of that treading model available in 
[Raft module](../raft/README.md)).

Although another methods are returned futures, but in most cases they are executed synchronously. The futures are conditioned by
asynchronous Metastorage initialization, that happens because required a time to start a RAFT group. In other words, we have a chance of
complete those features in thread of RAFT client executor (prefix <NODE_NAME>%Raft-Group-Client).

### Using common pool

The component is used common ForkJoinPool on start (in fact, it is not necessary, because all components started in asynchronously in the
same ForkJoinPool). The using of the common pool is dangerous, because the pool can be busy by another threads that hosted on the same JVM 
(TODO: IGNITE-18505 Avoid using common pool on start components).
