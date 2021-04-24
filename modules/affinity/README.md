# Partitioning approach

## Hash-based partitioning

## Range-based partitioning

# Data migration (rebalance)
There is a significant difference between the rebalance approach in Ignite 2.x and rebalance approach in Ignite 3.x.

Ignite 2.x implemented rebalance process with updates being applied to the storage concurrently with data migration
process. This results in a complex interaction between the rebalance process and data update protocol (the necessity
to compare key-value versions during data migration, different entry processor application paths for cases when 
rebalance is active and not active, uncertain partition state during recovery, etc).

Ignite 3.x relies on common replication infrastructure for data replication between nodes, thus the rebalance should
be handled by means of the replication protocols.

## Raft
Raft consensus protocol does not have a concept of rebalance. Instead, it relies on two underlying mechanisms in order
to have an ability to catch offline nodes up-to-speed and bootstrap new Raft group members: Raft log and Snapshots.
These mechanisms handle both delta (when a local node has relevant enough local state so it can be brought up to speed
by sending only recent Raft log commands) and full (when a Raft group does not have sufficient Raft log to catch up the
node, so the full state machine snapshot should be sent to the local node) rebalance scenarios. The choice between
snapshot and log-based catch-up is based on Raft log availability, however, this logic can be adjusted to a more 
sophisticated heuristic. The underlying state machine should only provide the snapshot functionality. This functionality
differs for in-memory and persistent tables.

### In-memory tables
In-memory tables do not save partitions in isolated memory regions. Instead, the partition data is written to a shared
memory pool in order to provide efficient memory utilization (otherwise, an assigned memory chunk would remain assigned
to a partition and would not be eligible for other partitions for reuse). This makes it impossible to create partition 
memory snapshots on phycial level, so we need to maintain a snapshot on tuple basis.

At any moment in time at most one in-memory partition snapshot can be maintained.

#### Alternative 1
To create an in-memory snapshot, we use an MVCC-like approach with copy-on-write technique. The partition tree is 
extended to support keeping two versions of a tuple for the same key: one is the most relevant version, and another one 
is snapshot version. The snapshot tuple contains snapshot ID additionally to the regular tuple data. Snapshot tuples are 
only available to the snapshot iterator and must be filtered out from regular data access paths.  

When a snapshot for an in-memory partition is requested, the partition state machine checks that there is no another 
active snapshot and assigns a new snapshot ID which will be used for copy-on-write tuples. When the snapshot iterator
is traversing a tree, it attempts to read both up-to-date and snapshot version of the key. If the snapshot version of 
the key with the current snapshot ID exists, it must be used in the iterator. If the snapshot version of the key with 
the current snapshot ID does not exist, the up-to-date version of the tuple must be used in the iterator.

Each partition state machine update checks if there is a snapshot that is being maintained. If there is no active 
snapshot, the update operation should clean an old snapshot tuple version, if any, and do the regular tuple update. If 
there is an active snapshot, the update operation must first clean an old snapshot tuple version, if any. Then, if a 
snapshot tuple version with the current snapshot ID does not exist, the update operation copies the current tuple value
to the snapshot version, and then completes the update (it does not copy the current value if a relevant snapshot 
version already exists).

When snapshot is no longer needed, an asynchronous process can clean up the snapshot versions from the partition.

This approach does not induce any memory overhead when no snapshot is maintained, but may require up to 2x of partition
size under heavy load (because the whole partition may be copied to the snapshot versions in the worst case scenario).

#### Alternative 2
To create an in-memory snapshot, the snapshot data is written to a separate in-memory buffer. The buffer is populated 
from the state machine update thread either by the update operations or by a snapshot advance mini-task which is 
submitted to the state machine update thread as needed.

To maintain a snapshot, the state machine needs to keep an snapshot iterator boundary key. If a key being updated is 
smaller or equal than the boundary key, there is no need in any additional action because the snapshot iterator has 
already processed this key. If a key being updated is larger than the boundary key, the old version of the key is 
eagerly put to the snapshot buffer and the key is marked with snapshot ID (so that the key is skipped during further
iteration). Snapshot advance mini-task iterates over a next batch of the keys starting from the boundary key and puts
to the snapshot buffer only keys that are not yet marked by the snapshot ID. 

This approach has similar memory requirements to the first alternative, but does not require to modify the storage tree
so that it can store multiple versions of the same key. This approach, however, allows for transparent snapshot buffer
offloading to disk which can reduce memory requirements. It is also simpler in implementation because the code is 
essentially single-threaded and only requires synchronization for the in-memory buffer. The downside is that snapshot 
advance tasks will increase tail latency of state machine update operations.

### Persistent tables
For persistent tables, we exploit the existing ability of Ignite native persistence to take partition snapshots and use
the partition snapshot file as a Raft snapshot.

## Rebalance scheduling 
Snapshot transfer between nodes should be run through a separate scheduling layer so that nodes can dynamically adjust
to memory, CPU, IO, and network consumption. A certain quota of each resource should be allocated to the rebalance 
process, and the scheduling layer should suspend and probably cancel the rebalance session when the quota is exceeded
and resume or reschedule the rebalance session when resources get freed.