---
id: metrics-system-views
title: System Views
sidebar_label: System Views
---

Ignite provides a number of built-in SQL views that provide information on the cluster's state and provide real-time insight into the status of its components. These views are available in the SYSTEM schema.

## Getting Data

You access system views in Ignite by using SQL and selecting data from the system view like you would from any other table. For example, you can get a list of all available system views in the following way:

```sql
SELECT * FROM system.system_views
```

You can also use joins to combine data from multiple views. The example below returns all columns of a view that was found in the `SYSTEM_VIEWS` view:

```sql
SELECT svc.*
  FROM system.system_view_columns svc
  JOIN system.system_views sv ON svc.view_id = sv.id
 WHERE sv.name = 'SYSTEM_VIEWS'
```

## Available Views

### COMPUTE_JOBS

| Column | Data Type | Description |
|---|---|---|
| ID | STRING | The compute job ID. |
| COORDINATOR_NODE_ID | STRING | The job's coordinator node ID. |
| STATUS | STRING | The job status. |
| CREATE_TIME | TIMESTAMP WITH LOCAL TIME ZONE | The job creation timestamp. |
| START_TIME | TIMESTAMP WITH LOCAL TIME ZONE | The job start timestamp. |
| FINISH_TIME | TIMESTAMP WITH LOCAL TIME ZONE | The job finish timestamp. |

### GLOBAL_PARTITION_STATES

| Column | Data Type | Description |
|---|---|---|
| ZONE_NAME | STRING | The name of the distribution zone the partition belongs to. |
| TABLE_NAME | STRING | The name of the table stored in the partition. |
| TABLE_ID | INT32 | The ID of the table stored in the partition. |
| SCHEMA_NAME | STRING | The name of the schema the table belongs to. |
| PARTITION_ID | INT32 | The unique identifier of the partition. |
| STATE | STRING | Partition status. Possible values: `AVAILABLE`, `DEGRADED`, `READ_ONLY`, `UNAVAILABLE`. See [Disaster Recovery](/docs/3.1.0/configure-and-operate/operations/disaster-recovery-partitions) documentation for more information. |

### INDEXES

| Column | Data Type | Description |
|---|---|---|
| INDEX_ID | INT32 | Unique index identifier. |
| INDEX_NAME | STRING | The name of the index. |
| TABLE_ID | INT32 | Unique table identifier. |
| TABLE_NAME | STRING | The name of the table. |
| SCHEMA_ID | INT32 | Unique schema identifier. |
| SCHEMA_NAME | STRING | The name of the schema. |
| TYPE | STRING | The type of the index. Possible values: `HASH`, `SORTED`. |
| IS_UNIQUE | BOOLEAN | If the index is unique. |
| COLUMNS | STRING | The list of indexed columns. |
| STATUS | STRING | Current status of the index. Possible values: `REGISTERED` (Index has been registered and is awaiting the start of building), `BUILDING` (Index is being built), `AVAILABLE` (Index is built and is ready to use), `STOPPING` (DROP INDEX command has been executed, index is waiting for running transactions to finish). |

### LOCKS

A node system view that lists the currently active locks.

| Column | Data Type | Description |
|---|---|---|
| OWNING_NODE_ID | STRING | The ID of the node that owns the lock. |
| TX_ID | STRING | The ID of the transaction that created the lock. |
| OBJECT_ID | STRING | The ID of the locked object. |
| MODE | STRING | The [lock mode](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=211885498#IEP91:Transactionprotocol-Lockingmodel). Possible values are: IS (intention shared lock), S (shared lock), IX (intention exclusive lock), SIX (shared intention exclusive lock), X (exclusive lock). |

### LOCAL_PARTITION_STATES

| Column | Data Type | Description |
|---|---|---|
| ZONE_NAME | STRING | The name of the distribution zone the partition belongs to. |
| TABLE_NAME | STRING | The name of the table stored in the partition. |
| TABLE_ID | INT32 | The ID of the table stored in the partition. |
| SCHEMA | STRING | The name of the schema the table belongs to. |
| PARTITION_ID | INT32 | The unique identifier of the partition. |
| STATE | STRING | Partition status. Possible values: `AVAILABLE`, `DEGRADED`, `READ_ONLY`, `UNAVAILABLE`. See [Disaster Recovery](/docs/3.1.0/configure-and-operate/operations/disaster-recovery-partitions) documentation for more information. |
| ESTIMATED_ROWS | INT64 | The estimated number of rows in a partition. |

### SQL_QUERIES

| Column | Data Type | Description |
|---|---|---|
| INITIATOR_NODE | STRING | The name of the node that initiated the query. |
| PHASE | STRING | The query phase: INITIALIZATION (query registration and parsing), OPTIMIZATION (query validation and plan optimization), EXECUTION (query plan execution). |
| TYPE | STRING | The query type: DDL, DML, QUERY, or SCRIPT. |
| ID | STRING | The query ID. |
| USERNAME | STRING | The name of the user who started the query. |
| PARENT_ID | STRING | ID of the script that initiated the query (NULL if the query was not initiated by a script). |
| SQL | STRING | The SQL query's expression. |
| START_TIME | TIMESTAMP | The date/time the query started. |
| SCHEMA | STRING | The name of the default schema that was used to execute the query. |
| TRANSACTION_ID | STRING | The ID of the transaction in which the query was executed. |

### SYSTEM_VIEWS

Describes available system views.

| Column | Data Type | Description |
|---|---|---|
| ID | INT32 | System view ID. |
| SCHEMA | STRING | Name of the schema used. Default is `SYSTEM`. |
| NAME | STRING | System view name. |
| TYPE | STRING | System view type. Possible values: NODE (The view provides node-specific information. Data will be collected from all nodes, and represented in the view.), CLUSTER (The view provides cluster-wide information. Data will be collected from one node, chosen to represent the cluster.). |

### SYSTEM_VIEW_COLUMNS

Describes available system view columns.

| Column | Data Type | Description |
|---|---|---|
| VIEW_ID | INT32 | System view ID. |
| NAME | STRING | Column name. |
| TYPE | STRING | Column type. Can by any of the [supported types](/docs/3.1.0/sql/reference/data-types-and-functions/data-types). |
| NULLABLE | BOOLEAN | Defines if the column can be empty. |
| PRECISION | INT32 | Maximum number of digits. |
| SCALE | INT32 | Maximum number of decimal places. |
| LENGTH | INT32 | Maximum length of the value. Symbols for string values or bytes for binary values. |

### TRANSACTIONS

:::note
This view shows only the currently active transactions.
:::

| Column | Data Type | Description |
|---|---|---|
| COORDINATOR_NODE | STRING | The name of the transaction's coordinator node. |
| STATE | STRING | The transaction state. For read-only transactions, the value is always null (empty). For read-write transactions, the possible values are PENDING (the transaction is in progress) and FINISHING (the transaction is in the process of being finished). |
| ID | STRING | The transaction ID. |
| START_TIME | TIMESTAMP | The transaction's start time. |
| TYPE | STRING | The transaction type: READ_ONLY or READ_WRITE. |
| PRIORITY | STRING | The transaction priority, which is used to resolve conflicts between transactions. Currently, this value cannot be explicitly set by the user. Possible values are LOW and NORMAL (default). |

### ZONES

| Column | Data Type | Description |
|---|---|---|
| NAME | STRING | The name of the distribution zone. |
| PARTITIONS | INT32 | The number of partitions in the distribution zone. |
| REPLICAS | STRING | The number of copies of each partition in the distribution zone. |
| DATA_NODES_AUTO_ADJUST_SCALE_UP | INT32 | The delay in seconds between the new node joining and the start of data zone adjustment. |
| DATA_NODES_AUTO_ADJUST_SCALE_DOWN | INT32 | The delay in seconds between the node leaving the cluster and the start of data zone adjustment. |
| DATA_NODES_FILTER | STRING | The filter that specifies what nodes will be used by the distribution zone. |
| IS_DEFAULT_ZONE | BOOLEAN | Defines if the data zone is used by default. |
