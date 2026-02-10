---
title: System Views
sidebar_label: System Views
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

Ignite provides a number of built-in SQL views that provide information on the cluster's state and provide real-time insight into the status of its components. These views are available in the SYSTEM schema.

## Getting Data

You access system views in Ignite by using SQL and selecting data from the system view like you would from any other table. For example, you can get a list of all available system views in the following way:

```sql
SELECT id, schema, name FROM system.system_views WHERE type = 'NODE'
```

You can also use joins to combine data from multiple views. The example below returns all columns of a view that was found in the `SYSTEM_VIEWS` view:

```sql
SELECT svc.*
  FROM system.system_view_columns svc
  JOIN system.system_views sv ON svc.view_id = sv.id
 WHERE sv.name = 'SYSTEM_VIEWS'
```

## Available Views

### SYSTEM_VIEWS

Describes available system views.

| Column | Data Type | Description |
|--------|-----------|-------------|
| ID | INT32 | System view ID. |
| SCHEMA | STRING | Name of the schema used. Default is `SYSTEM`. |
| NAME | STRING | System view name. |
| TYPE | STRING | System view type. Possible values:<br/><br/>* NODE - The view provides node-specific information. Data will be collected from all nodes, and represented in the view.<br/>* CLUSTER - The view provides cluster-wide information. Data will be collected from one node, chosen to represent the cluster. |

### SYSTEM_VIEW_COLUMNS

Describes available system view columns.

| Column | Data Type | Description |
|--------|-----------|-------------|
| VIEW_ID | INT32 | System view ID. |
| NAME | STRING | Column name. |
| TYPE | STRING | Column type. Can by any of the [supported types](../../sql-reference/data-types.md). |
| NULLABLE | BOOLEAN | Defines if the column can be empty. |
| PRECISION | INT32 | Maximum number of digits. |
| SCALE | INT32 | Maximum number of decimal places. |
| LENGTH | INT32 | Maximum length of the value. Symbols for string values or bytes for binary values. |

### SYSTEM.ZONES

| Column | Data Type | Description |
|--------|-----------|-------------|
| NAME | STRING | The name of the distribution zone. |
| PARTITIONS | INT32 | The number of partitions in the distribution zone. |
| REPLICAS | STRING | The number of copies of each partition in the distribution zone. |
| DATA_NODES_AUTO_ADJUST_SCALE_UP | INT32 | The delay in seconds between the new node joining and the start of data zone adjustment. |
| DATA_NODES_AUTO_ADJUST_SCALE_DOWN | INT32 | The delay in seconds between the node leaving the cluster and the start of data zone adjustment. |
| DATA_NODES_FILTER | STRING | The filter that specifies what nodes will be used by the distribution zone. |
| IS_DEFAULT_ZONE | BOOLEAN | Defines if the data zone is used by default. |
