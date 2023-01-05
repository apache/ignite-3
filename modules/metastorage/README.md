<h2>Metastorage</h2>
The module for store and access to metadata.
The storage is distributed to prevent losing data and based of RAFT. In typical case, the group consists of only several nodes of the cluster (amount of nodes have to be odd 3 or 5). The rest of nodes listens metadata update but does not participant in voting (RAFT listeners).

<h2>Threading model</h2>

There is only one dedicate thread to notify watches. This is a thread with prefix metastorage-watch-executor.
The thread is created from executor in KeyValueStorage:
```java
private final ExecutorService watchExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("metastorage-watch-executor", LOG));
```
Also, the storage internally contains a two threads executor to create a snapshot:
```java
private final ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("metastorage-snapshot-executor", LOG));
```
Both of the executors have no a node name in their names. The names are required to identify threads belonged to different Ignite nodes on the shared java machine (IGNITE-18XXX Add nodes name in prefix to Metastorage executors).

<h3>Interface methods</h3>
Various operations (*get()*, *getAll()*, *invoke()*) in Metastorage manager return futures. Those futures are completed when the matched RAFT command completes to the Metastorage group. This result appears in  RAFT client executor (prefix <NODE_NAME>%Raft-Group-Client).
Although another methods are returned futures, but in most cases they are executed synchronously. The futures are conditioned by asynchronous Metastorage initialization, that happens because required a time to start a RAFT group. In other words, we have a chance of complete those features in thread of RAFT client executor (prefix <NODE_NAME>%Raft-Group-Client).

<h3>Using common pool</h3>
The component is used common ForkJoinPool on start (in fact, it is not necessary, because all components started in asynchronously in the same ForkJoinPool). The using of the common pool is dangerous, because the pool can be busy by another threads that hosted on the same JVM (IGNITE-18XXX Avoid using common pool on start components).
