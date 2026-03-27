---
title: Migrating from Ignite 2
---

This section describes how to configure an Apache Ignite 3 cluster into which you will migrate all the components of your Apache Ignite 2 cluster.

## Configuration Migration

You need to configure the cluster you have created to match the Apache Ignite 2 cluster you are migrating from.

While cluster configurations in Apache Ignite 2 are XML beans, in Apache Ignite 3 they are in HOCON format. Moreover, many configuration structures in version 3 are different from those in version 2.

In Apache Ignite 3, the configuration file has a single root "node," called `ignite`. All configuration sections are children, grandchildren, etc., of that node.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

For example:

```json
{
    "ignite" : {
        "network" : {
            "nodeFinder" : {
                "netClusterNodes" : ["localhost:3344"]
            },
            "port" : 3344
        },
        "storage" : {
            "profiles" : [
                {
                    "name" : "persistent",
                    "engine" : "aipersist"
                }
            ]
        },
        "nodeAttributes.nodeAttributes" : {
            "region" : "US",
            "storage" : "SSD"
        }
    }
}
```

When migrating your environment Apache Ignite 3 configuration is split between Cluster, Node and distribution zone configurations.

### Node Configuration

Node configuration stores information about the locally running node.

#### Storage Configuration

Apache Ignite 3 storage is configured in a completely different manner from Apache Ignite 2.

* First, you configure **storage engine** properties, which may include properties like page size or checkpoint frequency.
* Then, you create a **storage profile**, which defines a specific storage that will be used.
* Then, you create a **distribution zone** using the storage profile, which can be further used to fine-tune the storage by defining where and how to store data across the cluster.
* Finally, each **table** can be assigned to the distribution zone, or directly to a storage profile.

Note:

* Only tables and distribution zones can be configured from code. Storage profiles and engines must be configured by updating node configuration and restarting node.
* Custom affinity functions are replaced by distribution zones.
* External storage is supported via cache storage that must be configured by using SQL.

#### Client Configuration

All clients in Apache Ignite 3 are "thin", and use a similar `clientConnector` configuration. See [Apache Ignite Clients](/3.1.0/develop/ignite-clients/) section for more information on configuring client connector.

#### Network Configuration

Node network configuration is now performed in the `network` section of the [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).

#### REST API Configuration

REST API is a significant part of Apache Ignite 3. It can be used for multiple purposes, including cluster and node configuration and running SQL requests.

You can configure REST properties in [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).

### Cluster Configuration

Cluster configuration applies to all nodes in the cluster. It is automatically propagated across the cluster from the node you apply in at.

#### Handling Events

Events configuration is simplified in Apache Ignite 3. It is separated in 2 configurations:

* Event **channels** define what is collected.
* Event **sinks** define where the data is sent.

In the current release, only `log` sink are supported. You can configure events as described in the [Events](/3.1.0/develop/work-with-data/events) section.

#### Metrics Collection

Apache Ignite 3 has metrics disabled by default.

All metrics are grouped according to their metric sources, and are enabled in cluster configuration per metric source.

Then, these metrics will be available in Apache Ignite JMX beans.

For instructions on configuring metrics, see [Metrics Configuration](/3.1.0/configure-and-operate/configuration/metrics-configuration).

## Code Migration

Code written for Apache Ignite 2 cannot be directly reused, however as most concepts remain similar, code migration should not take too much time.

### Collocated Compute and Partition-Local Queries

In Apache Ignite 2, you could pin a compute job to a specific partition using `ComputeTask` with `setPartition` on the job context. Ignite 3 provides two approaches to achieve the same result, both based on running a job on the node that owns a partition and then querying only that partition's rows using the `__PARTITION_ID` virtual SQL column.

#### Option 1: Broadcast to Table Partitions (Recommended)

Use `BroadcastJobTarget.table()` with `JobExecutionContext.partition()`. This is the preferred approach because Ignite routes each job instance to the node currently holding its partition, and `context.partition()` is always non-null, so execution is guaranteed to be local.

```java
JobDescriptor<Void, Long> job = JobDescriptor.builder(PartitionQueryJob.class)
        .units(deploymentUnit)
        .build();

Collection<Long> partitionCounts = client.compute()
        .execute(BroadcastJobTarget.table("Person"), job, null);

long total = partitionCounts.stream().mapToLong(Long::longValue).sum();
```

Inside the job, read `context.partition()` to get the partition assigned to this instance, then filter rows with `__PARTITION_ID`:

```java
public class PartitionQueryJob implements ComputeJob<Void, Long> {
    @Override
    public CompletableFuture<Long> executeAsync(JobExecutionContext context, Void arg) {
        Partition partition = context.partition(); // non-null with BroadcastJobTarget.table()

        long count = 0;
        try (ResultSet<SqlRow> rs = context.ignite().sql().execute(
                null,
                "SELECT COUNT(*) FROM Person WHERE __PARTITION_ID = ?",
                partition.id()
        )) {
            if (rs.hasNext()) {
                count = rs.next().longValue(0);
            }
        }
        return CompletableFuture.completedFuture(count);
    }
}
```

See `ComputeBroadcastExample` in the examples module for a complete runnable version.

#### Option 2: MapReduce over Partition Distribution

Use `PartitionDistribution` to enumerate partitions in the split phase of a `MapReduceTask`, then dispatch one job per partition to its primary replica node:

```java
public class PersonCountByPartitionTask implements MapReduceTask<Void, Long, Long, Long> {
    @Override
    public CompletableFuture<List<MapReduceJob<Long, Long>>> splitAsync(
            TaskExecutionContext context, Void input) {
        JobDescriptor<Long, Long> jobDescriptor = JobDescriptor.builder(PartitionPersonCountJob.class)
                .build();

        Map<Partition, ClusterNode> primaryReplicas = context.ignite().tables()
                .table("Person")
                .partitionDistribution()
                .primaryReplicas();

        List<MapReduceJob<Long, Long>> jobs = new ArrayList<>();
        for (Map.Entry<Partition, ClusterNode> entry : primaryReplicas.entrySet()) {
            jobs.add(MapReduceJob.<Long, Long>builder()
                    .jobDescriptor(jobDescriptor)
                    .nodes(Set.of(entry.getValue()))
                    .args(entry.getKey().id())
                    .build());
        }
        return CompletableFuture.completedFuture(jobs);
    }

    @Override
    public CompletableFuture<Long> reduceAsync(TaskExecutionContext context, Map<UUID, Long> results) {
        return CompletableFuture.completedFuture(
                results.values().stream().mapToLong(Long::longValue).sum());
    }
}
```

Each job receives the partition ID as its argument and queries only that partition:

```java
public class PartitionPersonCountJob implements ComputeJob<Long, Long> {
    @Override
    public CompletableFuture<Long> executeAsync(JobExecutionContext context, Long partitionId) {
        long count = 0;
        try (ResultSet<SqlRow> rs = context.ignite().sql().execute(
                null,
                "SELECT COUNT(*) FROM Person WHERE __PARTITION_ID = ?",
                partitionId
        )) {
            if (rs.hasNext()) {
                count = rs.next().longValue(0);
            }
        }
        return CompletableFuture.completedFuture(count);
    }
}
```

See `ComputePartitionQueryMapReduceExample` in the examples module for a complete runnable version.

:::note
`PartitionDistribution.primaryReplicas()` captures partition locations at a point in time. If a partition is reassigned between the split phase and job execution, the job may run on a non-primary node and the SQL query will not be local. Use `BroadcastJobTarget.table()` (Option 1) when local execution must be guaranteed.
:::

#### How to Run a Local Query

Both approaches use the `__PARTITION_ID` virtual column (type `BIGINT`) to restrict a query to a single partition's rows. This is how you achieve the equivalent of Ignite 2's collocated queries without cross-node data movement:

```sql
SELECT * FROM Person WHERE __PARTITION_ID = ?
```

Pass the partition ID returned by `partition.id()` (Option 1) or the `long` ID passed as the job argument (Option 2) as the query parameter.
