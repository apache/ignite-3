---
title: Data Manipulation Language (DML)
sidebar_label: DML
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

This section walks you through all data manipulation language (DML) commands supported by Apache Ignite 3.

## DELETE

Deletes data from a table.

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

- `alias` - an SQL alias for an expression or value.
- `booleanExpression` - an SQL expression that returns a boolean value. Only the records for which `TRUE` was returned will be deleted. If not specified, all records are deleted.

## INSERT

Inserts data into a table.

{/* Railroad diagram omitted - see 3.1.0 docs */}

## MERGE

Merges data into a table.

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

:::note
At least one of the `WHEN MATCHED` and `WHEN NOT MATCHED` clauses must be present.
:::

### Parameters

- `alias` - an SQL alias for an expression or value.
- `booleanExpression` - an SQL expression that returns a boolean value. If `TRUE` is returned, the `WHEN MATCHED` clause is executed, otherwise the `WHEN NOT MATCHED` is executed.
- `value` - arbitrary value that will be inserted into the table during the operation.

## UPDATE

Updates data in a table.

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

- `booleanExpression` - an SQL expression that returns a boolean value. Only the records for which `TRUE` was returned will be updated. If not specified, all records will be updated.
