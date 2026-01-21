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

### COMPUTE_TASKS

| Column | Data Type | Description |
|---|---|---|
| COORDINATOR_NODE_ID | STRING | The task's coordinator node ID. |
| COMPUTE_TASK_ID | STRING | The compute task ID. |
| COMPUTE_TASK_STATUS | STRING | The task status. |
| COMPUTE_TASK_CREATE_TIME | TIMESTAMP WITH LOCAL TIME ZONE | The task creation timestamp. |
| COMPUTE_TASK_START_TIME | TIMESTAMP WITH LOCAL TIME ZONE | The task start timestamp. |
| COMPUTE_TASK_FINISH_TIME | TIMESTAMP WITH LOCAL TIME ZONE | The task finish timestamp. |
| ID | STRING | *Deprecated*. The compute task ID. |
| STATUS | STRING | *Deprecated*. The task status. |
| CREATE_TIME | TIMESTAMP WITH LOCAL TIME ZONE | *Deprecated*. The task creation timestamp. |
| START_TIME | TIMESTAMP WITH LOCAL TIME ZONE | *Deprecated*. The task start timestamp. |
| FINISH_TIME | TIMESTAMP WITH LOCAL TIME ZONE | *Deprecated*. The task finish timestamp. |

### GLOBAL_PARTITION_STATES

| Column | Data Type | Description |
|---|---|---|
| ZONE_NAME | STRING | The name of the distribution zone the partition belongs to. |
| TABLE_ID | INT32 | The ID of the table stored in the partition. |
| SCHEMA_NAME | STRING | The name of the schema the table belongs to. |
| TABLE_NAME | STRING | The name of the table stored in the partition. |
| PARTITION_ID | INT32 | The unique identifier of the partition. |
| PARTITION_STATE | STRING | Partition status. Possible values: `AVAILABLE`, `DEGRADED`, `READ_ONLY`, `UNAVAILABLE`. See [Disaster Recovery](/3.1.0/configure-and-operate/operations/disaster-recovery-partitions) documentation for more information. |
| ZONE_ID | INT32 | Unique zone identifier. |
| SCHEMA_ID | INT32 | Unique schema identifier. |
| STATE | STRING | *Deprecated*. Partition status. Possible values: `AVAILABLE`, `DEGRADED`, `READ_ONLY`, `UNAVAILABLE`. |

### GLOBAL_ZONE_PARTITION_STATES

| Column | Data Type | Description |
|---|---|---|
| ZONE_NAME | STRING | Name of the zone. |
| ZONE_ID | INT32 | Internal identifier of the zone. |
| PARTITION_ID | INT32 | Identifier of the partition. |
| PARTITION_STATE | STRING | Current state of the partition. Possible values: `AVAILABLE` (All replicas are healthy), `DEGRADED` (There are healthy replicas, and they form a majority), `READ_ONLY` (There are healthy replicas, but they don't form a majority), `UNAVAILABLE` (There are no healthy replicas). |

### INDEXES

| Column | Data Type | Description |
|---|---|---|
| INDEX_ID | INT32 | Unique index identifier. |
| INDEX_NAME | STRING | The name of the index. |
| TABLE_ID | INT32 | Unique table identifier. |
| TABLE_NAME | STRING | The name of the table. |
| SCHEMA_ID | INT32 | Unique schema identifier. |
| SCHEMA_NAME | STRING | The name of the schema. |
| INDEX_TYPE | STRING | The type of the index. Possible values: `HASH`, `SORTED`. |
| IS_UNIQUE_INDEX | BOOLEAN | If the index is unique. |
| INDEX_COLUMNS | STRING | The list of indexed columns. |
| INDEX_STATE | STRING | Current status of the index. Possible values: `REGISTERED` (Index has been registered and is awaiting the start of building), `BUILDING` (Index is being built), `AVAILABLE` (Index is built and is ready to use), `STOPPING` (DROP INDEX command has been executed, index is waiting for running transactions to finish). |
| TYPE | STRING | *Deprecated*. The type of the index. Possible values: `HASH`, `SORTED`. |
| IS_UNIQUE | BOOLEAN | *Deprecated*. If the index is unique. |
| COLUMNS | STRING | *Deprecated*. The list of indexed columns. |
| STATUS | STRING | *Deprecated*. Current status of the index. |

### INDEX_COLUMNS

| Column | Data Type | Description |
|---|---|---|
| SCHEMA_ID | INT32 | Unique schema identifier. |
| SCHEMA_NAME | STRING | The name of the schema. |
| TABLE_ID | INT32 | Unique table identifier. |
| TABLE_NAME | STRING | The name of the table. |
| INDEX_ID | INT32 | Unique index identifier. |
| INDEX_NAME | STRING | The name of the index. |
| COLUMN_NAME | STRING | Column name. |
| COLUMN_ORDINAL | INT32 | The ordinal number of the column in the index definition. |
| COLUMN_COLLATION | STRING | Collation rules for the column. |

### LOCAL_ZONE_PARTITION_STATES

| Column | Data Type | Description |
|---|---|---|
| NODE_NAME | STRING | Name of the node reporting the partition state. |
| ZONE_NAME | STRING | Name of the zone. |
| ZONE_ID | INT32 | Internal identifier of the zone. |
| ESTIMATED_ROWS | INT64 | Approximate number of rows stored in this partition on the local node. |
| PARTITION_ID | INT32 | Identifier of the partition. |
| PARTITION_STATE | STRING | Current state of the local partition. Possible values: `UNAVAILABLE` (Partition is not yet started or is stopping), `HEALTHY` (Alive partition with a healthy state machine), `INITIALIZING` (Partition is starting right now), `INSTALLING_SNAPSHOT` (Partition is installing a Raft snapshot from the leader), `CATCHING_UP` (Partition is catching up, meaning that it's not replicated part of the log yet), `BROKEN` (Partition is in broken state, usually it means that its state machine threw an exception). |

### LOCKS

A node system view that lists the currently active locks.

| Column | Data Type | Description |
|---|---|---|
| OWNING_NODE_ID | STRING | The ID of the node that owns the lock. |
| TRANSACTION_ID | STRING | The ID of the transaction that created the lock. |
| OBJECT_ID | STRING | The ID of the locked object. |
| LOCK_MODE | STRING | The [lock mode](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=211885498#IEP91:Transactionprotocol-Lockingmodel). Possible values are: IS (intention shared lock), S (shared lock), IX (intention exclusive lock), SIX (shared intention exclusive lock), X (exclusive lock). |
| TX_ID | STRING | *Deprecated*. The ID of the transaction that created the lock. |
| MODE | STRING | *Deprecated*. The lock mode. |

### LOCAL_PARTITION_STATES

| Column | Data Type | Description |
|---|---|---|
| NODE_NAME | STRING | The name of the node the partition is stored on. |
| ZONE_NAME | STRING | The name of the distribution zone the partition belongs to. |
| TABLE_ID | INT32 | The ID of the table stored in the partition. |
| SCHEMA_NAME | STRING | The name of the schema the table belongs to. |
| TABLE_NAME | STRING | The name of the table stored in the partition. |
| PARTITION_ID | INT32 | The unique identifier of the partition. |
| PARTITION_STATE | STRING | Partition status. Possible values: `HEALTHY`, `INITIALIZING`, `INSTALLING_SNAPSHOT`, `CATCHING_UP`, `UNAVAILABLE`, `BROKEN`. See [Disaster Recovery](/3.1.0/configure-and-operate/operations/disaster-recovery-partitions#local-partition-states) documentation for more information. |
| ESTIMATED_ROWS | INT64 | The estimated number of rows in a partition. |
| ZONE_ID | INT32 | Unique zone identifier. |
| SCHEMA_ID | INT32 | Unique schema identifier. |
| STATE | STRING | *Deprecated*. Partition status. |

### SCHEMAS

| Column | Data Type | Description |
|---|---|---|
| SCHEMA_ID | INT32 | Unique schema identifier. |
| SCHEMA_NAME | STRING | The name of the schema. |

### SQL_QUERIES

| Column | Data Type | Description |
|---|---|---|
| INITIATOR_NODE | STRING | The name of the node that initiated the query. |
| QUERY_ID | STRING | The query ID. |
| USERNAME | STRING | The name of the user who started the query. |
| QUERY_PHASE | STRING | The query phase: INITIALIZATION (query registration and parsing), OPTIMIZATION (query validation and plan optimization), EXECUTION (query plan execution). |
| QUERY_TYPE | STRING | The query type: DDL, DML, QUERY, or SCRIPT. |
| QUERY_DEFAULT_SCHEMA | STRING | The name of the default schema that was used to execute the query. |
| SQL | STRING | The SQL query's expression. |
| QUERY_START_TIME | TIMESTAMP | The date/time the query started. |
| TRANSACTION_ID | STRING | The ID of the transaction in which the query was executed. |
| PARENT_QUERY_ID | STRING | ID of the script that initiated the query (NULL if the query was not initiated by a script). |
| QUERY_STATEMENT_ORDINAL | INT32 | The ordinal number of the query. |
| ID | STRING | *Deprecated*. The query ID. |
| PHASE | STRING | *Deprecated*. The query phase. |
| TYPE | STRING | *Deprecated*. The query type. |
| SCHEMA | STRING | *Deprecated*. The name of the default schema that was used to execute the query. |
| START_TIME | TIMESTAMP | *Deprecated*. The date/time the query started. |
| PARENT_ID | STRING | *Deprecated*. ID of the script that initiated the query. |
| STATEMENT_NUM | INT32 | *Deprecated*. The ordinal number of the query. |

### SQL_CACHED_QUERY_PLANS

| Column | Data Type | Description |
|---|---|---|
| NODE_ID | STRING | ID of the node where the plan is cached. |
| PLAN_ID | STRING | Internal identifier of the prepared plan. |
| CATALOG_VERSION | INT32 | Catalog version used when the query was prepared. |
| QUERY_DEFAULT_SCHEMA | STRING | Default schema applied during query preparation. |
| SQL | STRING | Normalized SQL text of the query. |
| QUERY_TYPE | STRING | Query type. |
| QUERY_PLAN | STRING | Serialized or explain representation of the chosen query plan. |
| QUERY_PREPARE_TIME | TIMESTAMP WITH LOCAL TIME ZONE | Time the plan was prepared on the node. |

### SYSTEM_VIEWS

Describes available system views.

| Column | Data Type | Description |
|---|---|---|
| VIEW_ID | INT32 | System view ID. |
| SCHEMA_NAME | STRING | Name of the schema used. Default is `SYSTEM`. |
| VIEW_NAME | STRING | System view name. |
| VIEW_TYPE | STRING | System view type. Possible values: NODE (The view provides node-specific information. Data will be collected from all nodes, and represented in the view.), CLUSTER (The view provides cluster-wide information. Data will be collected from one node, chosen to represent the cluster.). |
| ID | INT32 | *Deprecated*. System view ID. |
| SCHEMA | STRING | *Deprecated*. Name of the schema used. Default is `SYSTEM`. |
| NAME | STRING | *Deprecated*. System view name. |
| TYPE | STRING | *Deprecated*. System view type. |

### SYSTEM_VIEW_COLUMNS

Describes available system view columns.

| Column | Data Type | Description |
|---|---|---|
| VIEW_ID | INT32 | System view ID. |
| VIEW_NAME | STRING | Column name. |
| COLUMN_TYPE | STRING | Column type. Can be any of the [supported types](/3.1.0/sql/reference/data-types-and-functions/data-types). |
| IS_NULLABLE_COLUMN | BOOLEAN | Defines if the column can be empty. |
| COLUMN_PRECISION | INT32 | Maximum number of digits. |
| COLUMN_SCALE | INT32 | Maximum number of decimal places. |
| COLUMN_LENGTH | INT32 | Maximum length of the value. Symbols for string values or bytes for binary values. |
| NAME | STRING | *Deprecated*. Column name. |
| TYPE | STRING | *Deprecated*. Column type. |
| NULLABLE | BOOLEAN | *Deprecated*. Defines if the column can be empty. |
| PRECISION | INT32 | *Deprecated*. Maximum number of digits. |
| SCALE | INT32 | *Deprecated*. Maximum number of decimal places. |
| LENGTH | INT32 | *Deprecated*. Maximum length of the value. |

### TABLES

| Column | Data Type | Description |
|---|---|---|
| SCHEMA_NAME | STRING | The schema used by the table. |
| TABLE_NAME | STRING | Table name. |
| TABLE_ID | INT32 | Unique table identifier. |
| TABLE_PK_INDEX_ID | INT32 | The identifier of the primary key index. |
| ZONE_NAME | STRING | The distribution zone the table belongs to. |
| STORAGE_PROFILE | STRING | The storage profile the table uses. |
| TABLE_COLOCATION_COLUMNS | STRING | The name of the column that is used to colocate data. |
| SCHEMA_ID | STRING | The identifier of the schema used by the table. |
| ZONE_ID | STRING | The identifier of the zone the table belongs to. |
| IS_CACHE | BOOLEAN | Defines if it is a cache. |
| SCHEMA | STRING | *Deprecated*. The schema used by the table. |
| NAME | STRING | *Deprecated*. Table name. |
| ID | INT32 | *Deprecated*. Unique table identifier. |
| PK_INDEX_ID | INT32 | *Deprecated*. The identifier of the primary key index. |
| COLOCATION_KEY_INDEX | STRING | *Deprecated*. The name of the column that is used to colocate data. |
| ZONE | STRING | *Deprecated*. The distribution zone the table belongs to. |

### TABLE_COLUMNS

| Column | Data Type | Description |
|---|---|---|
| SCHEMA_NAME | STRING | The schema used by the table. |
| TABLE_NAME | STRING | Table name. |
| TABLE_ID | INT32 | Unique table identifier. |
| COLUMN_NAME | STRING | Column name. |
| COLUMN_TYPE | STRING | Column data type. |
| IS_NULLABLE_COLUMN | BOOLEAN | If the column can be `NULL`. |
| COLUMN_PRECISION | INT32 | Value precision. 0 if not applicable to data type. |
| COLUMN_SCALE | INT32 | Value scale. 0 if not applicable to data type. |
| COLUMN_LENGTH | INT32 | Value length, in bytes. |
| COLUMN_ORDINAL | INT32 | The ordinal number of the column. |
| SCHEMA_ID | INT32 | The id of the schema used by the sequence. |
| PK_COLUMN_ORDINAL | INT32 | Zero-based position of the column in the primary key. `NULL` if the column is not part of the primary key. |
| COLOCATION_COLUMN_ORDINAL | INT32 | Zero-based position of the column in the colocation key. `NULL` if the column is not part of the primary key. |
| SCHEMA | STRING | *Deprecated*. The schema used by the table. |
| TYPE | STRING | *Deprecated*. Column data type. |
| NULLABLE | BOOLEAN | *Deprecated*. If the column can be `NULL`. |
| PREC | INT32 | *Deprecated*. Value precision. |
| SCALE | INT32 | *Deprecated*. Value scale. |
| LENGTH | INT32 | *Deprecated*. Value length, in bytes. |

### TRANSACTIONS

:::note
This view shows only the currently active transactions.
:::

| Column | Data Type | Description |
|---|---|---|
| COORDINATOR_NODE_ID | STRING | The name of the transaction's coordinator node. |
| TRANSACTION_STATE | STRING | The transaction state. For read-only transactions, the value is always null (empty). For read-write transactions, the possible values are PENDING (the transaction is in progress) and FINISHING (the transaction is in the process of being finished). |
| TRANSACTION_ID | STRING | The transaction ID. |
| TRANSACTION_START_TIME | TIMESTAMP | The transaction's start time. |
| TRANSACTION_TYPE | STRING | The transaction type: READ_ONLY or READ_WRITE. |
| TRANSACTION_PRIORITY | STRING | The transaction priority, which is used to resolve conflicts between transactions. Currently, this value cannot be explicitly set by the user. Possible values are LOW and NORMAL (default). |
| STATE | STRING | *Deprecated*. The transaction state. |
| ID | STRING | *Deprecated*. The transaction ID. |
| START_TIME | TIMESTAMP | *Deprecated*. The transaction's start time. |
| TYPE | STRING | *Deprecated*. The transaction type. |
| PRIORITY | STRING | *Deprecated*. The transaction priority. |

### ZONES

| Column | Data Type | Description |
|---|---|---|
| ZONE_NAME | STRING | The name of the distribution zone. |
| ZONE_PARTITIONS | INT32 | The number of partitions in the distribution zone. |
| ZONE_REPLICAS | STRING | The number of copies of each partition in the distribution zone. |
| DATA_NODES_AUTO_ADJUST_SCALE_UP | INT32 | The delay in seconds between the new node joining and the start of data zone adjustment. |
| DATA_NODES_AUTO_ADJUST_SCALE_DOWN | INT32 | The delay in seconds between the node leaving the cluster and the start of data zone adjustment. |
| DATA_NODES_FILTER | STRING | The filter that specifies what nodes will be used by the distribution zone. |
| IS_DEFAULT_ZONE | BOOLEAN | If the data zone is used by default. |
| ZONE_CONSISTENCY_MODE | STRING | The zone's consistency mode. Possible values: `STRONG_CONSISTENCY`, `HIGH_AVAILABILITY`. |
| ZONE_ID | INT32 | Unique zone identifier. |
| NAME | STRING | *Deprecated*. The name of the distribution zone. |
| PARTITIONS | INT32 | *Deprecated*. The number of partitions in the distribution zone. |
| REPLICAS | STRING | *Deprecated*. The number of copies of each partition in the distribution zone. |
| CONSISTENCY_MODE | STRING | *Deprecated*. The zone's consistency mode. |

### ZONE_STORAGE_PROFILES

| Column | Data Type | Description |
|---|---|---|
| ZONE_NAME | STRING | The name of the distribution zone. |
| STORAGE_PROFILE | STRING | The name of the storage profile used by the distribution zone. |
| IS_DEFAULT_PROFILE | BOOLEAN | If the storage profile is used by default. |
| ZONE_ID | INT32 | Unique zone identifier. |
