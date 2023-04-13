# Table manager

Table manager is responsible for creating distributed tables. It reacts to changes in distributed configuration changes related to tables and assignments, and fires TableEvent processed by consumers in other components.

Table manager creates and destroys internal representation of tables, calls ReplicaManager to start and stop replicas, creates and destroys table storages.

## Threading model

There are following thread pools that are maintained by TableManager:
- rebalanceScheduler: is passed to replica listeners;
- scanRequestExecutor: is passed to replica listeners;
- txStateStorageScheduledPool: used for creation of transaction state storages as a pool for flushing;
- txStateStoragePool: used for creation of transaction state storages as a pool for flushing;
- ioExecutor: is used for asynchronous start Raft nodes and clients.

Activities of TableManager are processed in following threads:

### Distributed configuration thread pool
Most of operations related to configuration listeners happen here. Should not be blocked, asynchronous operations are preferred.

### ioExecutor
It is used for asynchronous start Raft nodes and clients.

### Raft disruptor pool
Some parts of updater of tables' versioned value in the update assignments procedure happen within this pool, as the previous updater future is completed from this pool's thread (see creation of schema registries in SchemaManager). Should not be blocked.
