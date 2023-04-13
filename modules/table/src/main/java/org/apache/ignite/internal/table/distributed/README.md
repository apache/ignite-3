# Table manager

Table manager is responsible for creating distributed tables. It reacts to changes in distributed configuration changes related to tables and assignments, and fires TableEvent processed by consumers in other components.

Table manager creates and destroys internal representation of tables, calls ReplicaManager to start and stop replicas, creates and destroys table storages.

## Threading model

There are following thread pools that are maintained by TableManager:

**rebalanceScheduler**

It is passed to RebalanceRaftGroupEventsListener and is used to do rebalance related operations.
Size is calculated as
```java
private static final int REBALANCE_SCHEDULER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors() * 3, 20);
```
Thread prefix: `rebalance-scheduler`

**scanRequestExecutor**

It is single thread executor that is passed to replica listeners and is used to process scan requests, provides effective tail recursion execution without deep recursive calls.

Thread prefix: `scan-query-executor-`.

**txStateStorageScheduledPool**

It is single thread executor that is used for flushing purposes for transaction state storages. 
Thread prefix: `tx-state-storage-scheduled-pool
`
**txStateStoragePool**

It is thread executor that is used for flushing purposes for transaction state storages. Size is calculated as available processors count. 
Thread prefix: `tx-state-storage-scheduled-pool

**ioExecutor**

It is used for asynchronous start Raft nodes and clients. Size is calculated as `Math.min(cpus * 3, 25)`.
Thread prefix: `tableManager-io`

### Activities of TableManager are processed in following threads:

**Distributed configuration thread pool**

Most of operations related to configuration listeners happen here. Should not be blocked, asynchronous operations are preferred.

**ioExecutor**

It is used for asynchronous start Raft nodes and clients.

**Raft disruptor pool**
**Raft group client pool**

It depends on the role of the node in meta storage group. Some parts of updater of tables' versioned value (VV) in the update assignments procedure happen within this pool, as the previous updater future is completed from this pool's thread:
- tables's VV is updated from `createTableLocally`
- this updater waits for completion of schemas' VV
- schemas' VV is updated from `SchemaManager#onSchemaChange`, schema is registered in meta storage
- schemas' VV is ready to be completed after meta storage future is complete.
So, meta storage invoke future completes schemas' updater, which completes tables' updater, which allows the next updater to start, and this updater executes the code of assignments update in TableManager.

This thread should not be blocked.
