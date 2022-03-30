## Local state recovery

### Definitions

**Local state** is a state of components, started tables, schemas, persistent and in-memory storages, partitions, RAFT groups, etc. on a given node. It also includes a projection of distributed cluster configuration.

**Local state recovery** is changing local state in a way that every component on the node has its state configured and updated accordingly to cluster-wide configuration, topology, stored data, etc. and all the events that change this state and had already happened in the cluster to the moment in which the node tried to join the cluster.

**Vault** is a local storage of node configuration and a projection of cluster configuration. Vault has no log of historical changes. See [Vault](../../../../../../../../../vault/README.md).

**Metastorage** is a distributed storage of cluster-wide meta information, including cluster [configuration](../../../../../../../../../configuration/README.md). Metastorage is located on a group of nodes, forming a RAFT group. It stores a history of changes.

**CMG, or Cluster Management group**, is responsible for coordinating node join, node left, logical topology, etc.

**Distributed phase of recovery** is a phase when the node retrieves updates from Metastorage and applies them to make its state up-to-date with the cluster. It is required when the node is trying to join the cluster, and is not, when it starts in standalone mode, for example, in maintenance mode.

**Physical topology** - a set of nodes that are currently online and discovered via SWIM protocol, see [network-api](../../../../../../../../../network-api/README.md) and [network](../../../../../../../../../network/README.md) for details.

**Logical topology** - a set of nodes that are present in physical topology and finished recovery process, they are considered to be able to serve user load.

### Prerequisites

1. Vault is either empty or contains data that is available for reading (no corruption).
   If any action is required to recover data in Vault, it should be executed before.
2. Data in Vault is consistent to the moment when the node was stopped (last applied revision appropriates to the rest of the data).
3. Vault contains local node configuration, and a projection of cluster configuration. The latter can be written only after the cluster is initialized. No conflicts are possible between the node local configuration and the cluster configuration that is stored in Metastorage because the properties’ sets don’t intersect with each other.
4. The prerequisite for the recovery from Metastorage is receiving a successful _nodeJoinResponse_ [Node join protocol](TODO add readme).

### The start of the components

The node should start components and recover their state as was said above. If the node isn't starting in standalone mode (for example, maintenance mode), it should join the cluster and update the projection of cluster configuration by retrieving actual data from Metastorage (see description of the next steps for details), so certain steps of recovery must be synchronized with the stages of [node join process](TODO add readme), as described below. We should be able not to wait for these stages if the node is not going to join a cluster, for example, if it starts in maintenance mode.

The components start in such order that dependent components start after the components that they depend on, see [runner](../../../../../../../../../runner/README.md) for details. While starting, they register listeners on configuration updates from ConfigurationManager. These listeners receive notifications during the further lifetime of the node, and also during the recovery process while retrieving notifications from Metastorage, if it’s needed (see the description of the recovery process below). Note that the listeners can also process data from Vault and snapshot of data from Metastorage.

While handling the notifications, listeners should rely on the fact that the components that they depend on, won’t return stale or inconsistent data. It should be guaranteed by [causality tokens mechanism](../causality/README.md).

### Pre-recovery actions

At first, the node starts Vault and reads all data from the persistent storage.

Then the Network manager (ClusterService) starts. If the node is going to join a cluster, it should have a list of seed nodes, passed via command line or any kind of NodeFinder implementation.

The node joins physical topology using the list of seed nodes via SWIM protocol. It becomes available through the network, but is not operational yet.

The node should gain permission to join the cluster to be able to retrieve updates from Metastorage and make its state up-to-date with the cluster.

If _CMG peers_ property is present in Vault it means that cluster is already initialized and the node should wait for _ClusterStateMessage_ from CMG leader, which listens to physical topology events. Otherwise the node registers a handler of cluster init message (see [Node join protocol](TODO add readme) for details). The location of Metastorage is received within _ClusterStateMessage_.

When ClusterStateMessage is received by the node, it sends the _Join request_ which contains properties needed for node validation (cluster tag, version, permissions).

CMG tries to validate the node. If the join request is rejected by CMG, the node stops, otherwise it is considered as validated.

From here, the actual recovery process can start. This distributed communication is skipped when the node starts in standalone mode.

### Recovery process

The node begins to start other components. They register their listeners on `component.start()` using `any()` listener option. Notification of the listeners is allowed when all components have started and notifications from metastorage are allowed.

Then, the Metastorage manager starts. The node checks _metastorage peers_ property in Vault to find out if it should be a member of Metastorage group, and creates a RAFT client for Metastorage group. When Metastorage group and Metastorage manager are ready, the node gets the minimal revision and the latest revision that are available in the cluster to receive updates through the Metastorage watch.

Metastorage watch deploying is one via `metaStorageMgr#deployWatches`.

Here and below:
- **LV** – latest applied revision in Vault;
- **LM** – latest revision in metastorage group;
- **MM** – minimal revision in metastorage group.

Minimal revision in Metastorage advances when the log of Metastorage is compacted, thus the updates having revisions less than minimal cannot be applied sequentially.

Consider following cases:

_a._ **LM = LV**, this means the Vault is up-to-date with the cluster. The node can recover components’ state using data from Vault.

_b._ **LM > LV and MM <= LV + 1**: this is the case when Vault is behind the cluster but the node is able to get all sequential updates from Metastorage and apply them to components. The node can recover components’ state using data from Vault and then, when all components have started, process further updates from Metastorage log as Metastorage notifications, in historical sequence, using listeners. This works as if each component listener received each configuration update from cluster one by one and handled it as a single change. The listeners start handling notifications after all components have started.
  
During this process new updates can be written to Metastorage. After applying updates, the node should check the current value of LM and if it is significantly greater than LV, it should continue receiving updates from Metastorage, trying to catch current LM. This process ends when the difference between LV and LM is less or equal to some **acceptable difference value**, meaning that now the difference between LM and LV is small enough to let the node join logical topology.
  
It’s also possible that Metastorage can be compacted right during this process and MM becomes greater than LV. Another case is when LM is to much greater than LV and sequential logic will be extremely inefficient here. In any of these two cases, we can fallback to the next case.

_c._ **LM > LV and MM > LV + 1**: this means that the node can't apply updates sequentially because a part of updates that are not present in Vault were compacted in metastorage. The node must get all up-to-date data from Metastorage as a snapshot. Then it should perform smart eviction of local data, for example, preserve partitions that are still needed and evict those that are not needed according to cluster configuration.
  If smart eviction takes significant time, the node should check the current value of LM, and if it is now greater than some **acceptable difference value** that was mentioned for previous case, try to catch LM by applying updates for Metastorage, as described above for case b.

_d._ **LM < LV**, this means Metastorage group is behind the Vault. This case is impossible due to guarantees of the RAFT protocol that requires a majority to get updates.

Only case _a_ is possible when the node starts in standalone mode.
 
### Finishing recovery

After the recovery procedure is finished for all components, the node sends _RecoveryFinished_ message to CMG. After finishing recovery, the node should be able to serve user load.
