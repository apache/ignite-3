---
id: data-partitioning
title: Data Partitioning
sidebar_position: 2
---

# Data Partitioning

Data partitioning is a method of subdividing large sets of data into smaller chunks and distributing them between all server nodes in a balanced manner.

## How Data is Partitioned

When the table is created, it is always assigned to a [distribution zone](/docs/3.1.0/sql/reference/distribution-zones). Based on the distribution zone parameters, the table is separated into `PARTITIONS` parts, called *partitions*, stored `REPLICAS` times across the cluster. Each partition is identified by a number from a limited set (0 to 24 for the default zone). Each individual copy of a partition is called a *replica*, and is stored on separate nodes, if possible. Partitions with the same number for all tables in the zone are always stored on the same node.

Apache Ignite uses the *Fair* partition distribution algorithm. It means that it stores information on partition distribution and uses this information for assigning new partitions. This information is preserved in cluster metastorage, and is recalculated only when necessary.

Once partitions and all replicas are created, they are distributed across the available cluster nodes that are included in the distribution zone following the `DATA_NODES_FILTER` parameter and according to the *partition distribution algorithm*. Thus, each key is mapped to a list of nodes owning the corresponding partition and is stored on those nodes. When data is added, it is distributed evenly between partitions.

![Apache Ignite Partitions](/img/partitioning.png)

You can configure the way node stores relevant information in the [node configuration](/docs/3.1.0/configure-and-operate/reference/node-configuration):

- `ignite.system.partitionsBasePath` defines the folder partitions are stored in. By default, partitions are stored in the `work/partitions` folder.
- `ignite.system.partitionsLogPath` defines the folder where partition-specific RAFT logs are stored. These logs contain information on RAFT elections and consensus.
- `ignite.system.metastoragePath` defines the folder where cluster metadata is stored. It is recommended to store metadata on a separate device from partitions.

### Partition Number

When creating a distribution zone, you have an option to manually set the number of partitions with the `PARTITIONS` parameter, for example:

```sql
CREATE ZONE IF NOT EXISTS exampleZone (PARTITIONS 10) STORAGE PROFILES ['default'];
```

As partitions will be spread across the cluster, we recommend to set the number of partitions depending on its size and the number of available cores.

In most cases, we recommend using 2, 3 or 4 times the number of total available cores, divided by the number of replicas as the number of partitions. For example:

- For a cluster with 3 nodes, 8 cores on each node, and 3 data replicas, we recommend using 16, 24 or 32 partitions.
- For a cluster with 7 nodes, 16 cores on each node, and 3 data replicas, we recommend using 75, 112 or 150 partitions.

It is not recommended to set a significantly larger number of partitions or replicas, as maintaining partitions and their distribution can cause a performance drain on the cluster.

Otherwise, Apache Ignite will automatically calculate the recommended number of partitions:

```
dataNodesCount * coresOnNode * 2 / replicas
```

In this case, the `dataNodesCount` is the estimated number of nodes that will be in the distribution zone when it is created, according to its [filter](/docs/3.1.0/sql/reference/distribution-zones) and [storage profiles](/docs/3.1.0/understand/architecture/storage-architecture). At least 1 partition is always created.

### Replica Number

When creating a distribution zone, you can configure the number of *replicas* (individual copies of data on the cluster) by setting the `REPLICAS` parameter. By default, no additional replicas of data are created. As more replicas are added, additional copies of data will be stored on the cluster, and automatically spread to ensure data availability in case of a node leaving the cluster.

Replicas of each partition form a RAFT group, and a [quorum](/docs/3.1.0/sql/reference/distribution-zones) in that group is required to perform updates to the partition. The default quorum size depends on the number of replicas in the distribution zone: 3 replicas are required for quorum if the distribution zone has 5 or more replicas, 2 if there are between 2 and 4 replicas, or 1 if only one data replica exists.

Some replicas will be selected as part of a consensus group. These nodes will be voting members, confirming all data changes in the replication group, while other replicas will be *learners*, only passively receiving data from the group leader and not participating in elections.

Losing the majority of the consensus group leads the partition to enter the `Read-only` state. In this state, no data can be written and only explicit read-only transactions can be used to retrieve data. If the distribution zone [scales](/docs/3.1.0/sql/reference/distribution-zones) up or down (typically, due to a node entering or leaving the cluster), new replicas will be selected as the consensus group.

The size of the consensus group is automatically calculated based on quorum size:

```
quorumSize * 2 - 1
```

For example, with 5 replicas and quorum size of 2, 3 replicas will be part of consensus group, and 2 replicas will be learners. In this scenario, losing 2 nodes will lead to some partitions losing the majority of the consensus group and becoming unavailable. For this reason, it is recommended to have a quorum size of 3 for a 5-node cluster.

It is recommended to always have an odd number of replicas and at least 3 replicas of your data on the cluster. When only 2 data replicas exist, losing one will always lead to losing majority, while having 3 or 5 data replicas will allow the cluster to stay functional in [network segmentation](/docs/3.1.0/configure-and-operate/operations/lifecycle) scenarios.

You can also store data replicas on every node in cluster by creating a zone with `REPLICAS ALL` parameter to ensure data is always available to the cluster.

## Primary Replicas and Leases

Once the partitions are distributed on the nodes, Apache Ignite forms *replication groups* for all partitions of the table, and each group elects its leader. To linearize writes to partitions, Apache Ignite designates one replica of each partition as a *primary replica*, and other replicas as backups.

To designate a primary replica, Apache Ignite uses a process of granting a *lease*. Leases are granted by the *lease placement driver*, and signify the node that houses the primary replica, called a *lease holder*. Once the lease is granted, information about it is written to the [metastorage](/docs/3.1.0/configure-and-operate/operations/lifecycle#cluster-metastorage-group), and provided to all nodes in the cluster. Usually, the primary replica will be the same as replication group leader.

Granted leases are valid for a short period of time and are extended every couple of seconds, preserving the continuity of each lease. A lease cannot be revoked until it expires. In exceptional situations (for example, when the primary replica is unable to serve as primary anymore, the leaseholder node goes offline, the replication group is inoperable, etc.) the placement driver waits for the lease to expire and then initiates the negotiation of the new one.

Only the primary replica can handle operations of read-write transactions. Other replicas of the partition can be read from by using read-only transactions.

If a new replica is chosen to receive the lease, it first makes sure it is up-to-date with its replication group based on the stored data. In scenarios where replication group is no longer operable (for example, a node unexpectedly leaves the cluster and the group loses majority), it follows the [disaster recovery](/docs/3.1.0/configure-and-operate/operations/disaster-recovery-partitions) procedure, and you may need to reset the partitions manually.

### Reading Data From Replicas

Reading data as part of a read-write [transaction](/docs/3.1.0/develop/work-with-data/transactions) is always handled by the primary data replica.

Read-only transactions can be handled by either backup or primary replicas, depending on the specifics of the transaction.

## Version Storage

As new data is written to the partition, Apache Ignite does not immediately delete old one. Instead, Apache Ignite stores old keys in a *version chain* within the same partition.

Older key versions can only be accessed by [read-only transactions](/docs/3.1.0/develop/work-with-data/transactions#read-only-transactions), while up-to-date version can be accessed by any transactions.

Older key versions are kept until the *low watermark* point is reached. By default, low watermark is 600000 ms, and it can be changed in [cluster configuration](/docs/3.1.0/configure-and-operate/configuration/config-cluster-and-nodes). Increasing data availability time will mean that old key versions are stored and available for longer, however storing them may require extra storage, depending on cluster load.

In a similar manner, [dropped tables](/docs/3.1.0/sql/reference/ddl#drop-table) are also not removed from disk until the low watermark point, however you can no longer write to these tables. Read-only transactions that try to get data from these tables will succeed if they read data at timestamp before the table was dropped, and will delay the low watermark point if it is necessary to complete the transaction.

Once the low watermark is reached, old versions of data are considered garbage and will be cleaned up by garbage collector during the next cleanup. This data may or may not be available, as garbage collection is not an immediate process. If a transaction was already started before the low watermark was reached, the required data will be kept available until the end of transaction even if the garbage collection happens. Additionally, Apache Ignite checks that old data is not required anywhere on the cluster before cleaning up the data.

## Distribution Reset

The SQL query performance can deteriorate in a cluster where tables had been created over a long period, alongside topology changes, due to sub-optimum data colocation. To resolve this issue, you can reset (recalculate) partition distribution using [CLI](/docs/3.1.0/tools/cli-commands) or [REST API](/docs/3.1.0/tools/rest-api).

:::note
Reset is likely to result in Partition Rebalance, which may take a long time.
:::

## Partition Rebalance

When the [cluster size changes](/docs/3.1.0/sql/reference/distribution-zones), Apache Ignite waits for the timeout specified in the `AUTO SCALE UP` or `AUTO SCALE DOWN` distribution zone properties, and then redistributes partitions according to partition distribution algorithm and transfers data to make it up-to-date with the replication group. This process is called *data rebalance*.
