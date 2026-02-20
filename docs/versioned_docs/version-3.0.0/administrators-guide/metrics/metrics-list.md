---
title: Available Metrics
sidebar_label: Available Metrics
---

{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}

# Available Metrics

This topic lists all metrics available in Ignite 3.

## client.handler

The metrics provided by the client handler and related to active clients.

| Metric name | Description |
|-------------|-------------|
| BytesReceived | The total number of bytes received. |
| BytesSent | The total number of bytes sent. |
| ConnectionsInitiated | The total number of initiated connections. |
| CursorsActive | The number of active cursors. |
| RequestsActive | The number of requests in progress. |
| RequestsProcessed | The total number of processed requests. |
| RequestsFailed | The total number of failed requests. |
| SessionsAccepted | The total number of accepted sessions. |
| SessionsActive | The number of currently active sessions. |
| SessionsRejected | The total number of sessions rejected due to handshake errors. |
| SessionsRejectedTls | The total number of sessions rejected due to TLS handshake errors. |
| SessionsRejectedTimeout | The total number of sessions rejected due to a timeout. |
| TransactionsActive | The number of active transactions. |

## clock.service

| Metric name | Description |
|-------------|-------------|
| ClockSkewExceedingMaxClockSkew | The observed clock skew that exceeded the maximum clock skew. |

## jvm

The metrics for Ignite Java Virtual Machine resource use.

| Metric name | Description |
|-------------|-------------|
| UpTime | The uptime of the Java virtual machine in milliseconds. |
| gc.CollectionTime | The approximate total time spent on garbage collection in milliseconds, summed across all collectors. |
| memory.heap.Committed | The committed amount of heap memory. |
| memory.heap.Init | The initial amount of heap memory. |
| memory.heap.Max | The maximum amount of heap memory. |
| memory.heap.Used | The currently used amount of heap memory. |
| memory.non-heap.Committed | The committed amount of non-heap memory. |
| memory.non-heap.Init | The initial amount of non-heap memory. |
| memory.non-heap.Max | The maximum amount of non-heap memory. |
| memory.non-heap.Used | The used amount of non-heap memory. |

## metastorage

| Metric name | Description |
|-------------|-------------|
| IdempotentCacheSize | The current size of the cache of idempotent commands' results. |
| SafeTimeLag | The number of milliseconds the local MetaStorage SafeTime lags behind the local logical clock. |

## os

| Metric name | Description |
|-------------|-------------|
| CpuLoad | The CPU load. The value is between 0.0 and 1.0, where 0.0 means no CPU load and 1.0 means 100% CPU load. If the CPU load information is not available, a negative value is returned. |
| LoadAverage | The system load average for the last minute. The system load average is the sum of the number of runnable entities queued to the available processors and the number of runnable entities running on the available processors, averaged over a period of time. The way in which the load average is calculated depends on the operating system. If the load average is not available, a negative value is returned. |

## placement-driver

| Metric name | Description |
|-------------|-------------|
| ActiveLeasesCount | The number of currently active leases. |
| CurrentPendingAssignmentsSize | The current size of pending assignments over all partitions. |
| CurrentStableAssignmentsSize | The current size of stable assignments over all partitions. |
| LeasesCreated | The total number of created leases. |
| LeasesProlonged | The total number of prolonged leases. |
| LeasesPublished | The total number of published leases. |
| LeasesWithoutCandidates | The total number of leases without candidates currently existing. |

## raft

| Metric name | Description |
|-------------|-------------|
| raft.fsmcaller.disruptor.Stripes | The histogram of distribution data by stripes in the state machine for partitions. |
| raft.fsmcaller.disruptor.Batch | The histogram of the batch size to handle in the state machine for partitions. |
| raft.logmanager.disruptor.Batch | The histogram of the batch size to handle in the log for partitions. |
| raft.logmanager.disruptor.Stripes | The histogram of distribution data by stripes in the log for partitions. |
| raft.nodeimpl.disruptor.Batch | The histogram of the batch size to handle node operations for partitions. |
| raft.nodeimpl.disruptor.Stripes | The histogram of distribution data by stripes for node operations for partitions. |
| raft.readonlyservice.disruptor.Stripes | The histogram of distribution data by stripes for read-only operations for partitions. |
| raft.readonlyservice.disruptor.Batch | The histogram of the batch size to handle read-only operations for partitions. |

## resource.vacuum

| Metric name | Description |
|-------------|-------------|
| MarkedForVacuumTransactionMetaCount | The count of transaction metas that have been marked for vacuum. |
| SkippedForFurtherProcessingUnfinishedTransactionCount | The current number of unfinished transactions that are skipped by the vacuumizer for further processing. |
| VacuumizedPersistentTransactionMetaCount | The count of persistent transaction metas that have been vacuumized. |
| VacuumizedVolatileTxnMetaCount | The count of volatile transaction metas that have been vacuumized. |

## storage.aipersist.`{profile}`

:::note
Each [storage profile](../storage/index.md) with `aipersist` storage engine has an individual metrics exporter.
:::

| Metric name | Description |
|-------------|-------------|
| CpTotalPages | The number of pages in the current checkpoint. |
| CpEvictedPages | The number of evicted pages in the current checkpoint. |
| CpWrittenPages | The number of written pages in the current checkpoint. |
| CpSyncedPages | The number of fsynced pages in the current checkpoint. |
| CpWriteSpeed | The checkpoint write speed, in pages per second. The value is averaged over the last 3 checkpoints plus the current one. |
| CurrDirtyRatio | The current ratio of dirty pages (dirty vs total), expressed as a fraction. The fraction is computed for each segment in the current region, and the highest value becomes "current." |
| LastEstimatedSpeedForMarkAll | The last estimated speed of marking all clean pages dirty to the end of a checkpoint, in pages per second. |
| MaxSize | The maximum in-memory region size in bytes. |
| MarkDirtySpeed | The speed of marking pages dirty, in pages per second. The value is averaged over the last 3 fragments, 0.25 sec each, plus the current fragment, 0–0.25 sec (0.75–1.0 sec total). |
| SpeedBasedThrottlingPercentage | The fraction of throttling time within average marking time (e.g., "quarter" = 0.25). |
| TargetDirtyRatio | The ratio of dirty pages (dirty vs total), expressed as a fraction. Throttling starts when this ratio is reached. |
| ThrottleParkTime | The park (sleep) time for the write operation, in nanoseconds. The value is averaged over the last 3 fragments, 0.25 sec each, plus the current fragment, 0–0.25 sec (0.75–1.0 sec total). It defines park periods for either the checkpoint buffer protection or the clean page pool protection. |
| TotalAllocatedSize | The total size of allocated pages on disk in bytes. |
| TotalUsedSize | The total size of non-empty allocated pages on disk in bytes. |

## sql.client

SQL client metrics.

| Metric name | Description |
|-------------|-------------|
| OpenCursors | The number of currently open cursors. |

## sql.memory

| Metric name | Description |
|-------------|-------------|
| Limit | The SQL memory limit (bytes). |
| MaxReserved | The maximum memory usage by SQL so far (bytes). |
| Reserved | The current memory usage by SQL (bytes). |
| StatementLimit | The memory limit per SQL statement (bytes). |

## sql.plan.cache

Metrics for SQL cache planning.

| Metric name | Description |
|-------------|-------------|
| Hits | The total number of cache plan hits. |
| Misses | The total number of cache plan misses. |

## sql.queries

| Metric name | Description |
|-------------|-------------|
| Canceled | The total number of canceled queries. |
| Failed | The total number of failed queries. This metric includes all unsuccessful queries, regardless of reason. |
| Succeeded | The total number of successful queries. |
| TimedOut | The total number of queries that failed due to a time-out. |

## tables.`{table_name}`

Table metrics.

| Metric name | Description |
|-------------|-------------|
| RwReads | The total number of reads performed within read-write transactions. |
| RoReads | The total number of reads performed within read-only transactions. |
| Writes | The total number of write operations for this table. |

## thread.pools.`{thread-pool-executor-name}`

| Metric name | Description |
|-------------|-------------|
| ActiveCount | The approximate number of threads that are actively executing tasks. |
| CompletedTaskCount | The approximate total number of tasks that have completed execution. |
| CorePoolSize | The core number of threads. |
| KeepAliveTime | The thread keep-alive time, which is the amount of time threads in excess of the core pool size may remain idle before being terminated. |
| LargestPoolSize | The largest number of threads that have ever simultaneously been in the pool. |
| MaximumPoolSize | The maximum allowed number of threads. |
| PoolSize | The current number of threads in the pool. |
| TaskCount | The approximate total number of tasks that have been scheduled for execution. |
| QueueSize | The current size of the execution queue. |

## topology.cluster

Metrics for the cluster topology.

| Metric name | Description |
|-------------|-------------|
| ClusterId | The unique identifier of the cluster. |
| ClusterName | The unique name of the cluster. |
| TotalNodes | The total number of nodes in the logical topology. |

## topology.local

Metrics with node information.

| Metric name | Description |
|-------------|-------------|
| NodeName | The unique name of the node. |
| NodeId | The unique identifier of the node. |
| NodeVersion | The Ignite version on the node. |

## transactions

Transaction metrics.

| Metric name | Description |
|-------------|-------------|
| RwCommits | The total number of read-write transaction commits. |
| RoCommits | The total number of read-only transaction commits. |
| RwRollbacks | The total number of read-write transaction rollbacks. |
| RoRollbacks | The total number of read-only transaction rollbacks. |
| RwDuration | The histogram representation of read-write transaction latency. |
| RoDuration | The histogram representation of read-only transaction latency. |
| TotalRollbacks | The total number of transaction rollbacks. |
| TotalCommits | The total number of transaction commits. |

## zones

| Metric name | Description |
|-------------|-------------|
| LocalUnrebalancedPartitionsCount | The number of partitions that should be moved to this node. |
| TotalUnrebalancedPartitionsCount | The total number of partitions that should be moved to a new owner. |
