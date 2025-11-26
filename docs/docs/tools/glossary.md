---
id: glossary
title: Glossary
sidebar_position: 3
---

# Glossary

## C

**Cache**
: Caches are in-memory temporary storage designed for rapid response and reducing latency when working with databases.

**Cluster management group**
: A subset of Ignite nodes in a RAFT group. Cluster group leader is responsible for managing nodes that enter or leave an Ignite cluster.

**Cluster node**
: A cluster node is the base computational and data storage unit in Ignite.

**Columnar storage**
: A mechanism that is optimized for quick access to columns of data. It can be used to drastically improve performance when reading a specific column's values from a large number of rows.

**Coordinator node**
: The node that received the distributed computing job, manages its execution, and reports the results to the client.

## D

**Data colocation**
: When data is colocated, it means that data items that are relevant to each other are placed on the same node in the cluster.

**Data region**
: Used to control the amount of memory available for storage. Depending on the type of storage a data region is assigned to, the data may be loaded into RAM or stored on disk.

**Data rebalance**
: The process of redistributing partitions equally across all nodes in a cluster.

**Distribution zone**
: Distribution zone controls how data is placed into partitions, and how partitions are distributed on nodes on the cluster. Distribution zones are part of cluster configuration, and can be modified with [SQL commands](/docs/3.1.0/sql/reference/distribution-zones).

## L

**Logical Topology**
: A set of nodes connected into a Raft group is called a logical topology. These nodes follow the Raft leader and form an Ignite cluster.

## M

**Metastore**
: Metastore holds additional information about Ignite cluster that is required for its operation, for example the number and type of data regions configured.

## P

**Persistent Storage**
: Persistent storage is the type of memory storage that is preserved regardless of cluster state. Some portion of data will be loaded into RAM to improve performance.

**Physical topology**
: When nodes are started, they find each other and form a cluster on a physical topology. All nodes on a physical topology can form a Ignite cluster, but are not necessarily part of it.

**Primary Storage**
: Primary storage is the database to which data is written and from which it is usually read.

## R

**RAFT**
: Raft is a consensus algorithm that is used by Ignite to manage Ignite cluster. It provides a high degree of stability and data consistency by using elections to guarantee that there is always only one cluster leader that had an authoritative log of all transactions performed on the cluster.

**RAFT Log**
: Raft log is an append only collection of all operations performed on the cluster. Leader log is the sole authority in the cluster, and overwrites any contradicting logs on follower nodes.

**Rebalance**
: The process of relocation partitions between nodes to guarantee consistent data distribution after cluster topology changes.

**Replica storage**
: Provides a dynamically expanded copy of the primary storage, which can be set up to use a different storage type for better performance.

**Rolling upgrade**
: An update of the Ignite version without cluster-wide downtime.

## S

**Snapshot**
: A backup of data in an Ignite cluster. A snapshot taken on one cluster can be applied to another cluster.

## U

**Update buffer**
: Buffer that stores transactions to the primary storage before committing them to a replica storage. This reduces the number of transactions added to the latter.

## V

**Volatile storage**
: Memory storage that is only preserved while the cluster is active. Loss of power or unexpected cluster shutdown will lead to loss of data.
