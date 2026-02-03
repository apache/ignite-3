---
title: Transactions
sidebar_label: Transactions
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

A transaction is a sequence of SQL operations that starts with the `START TRANSACTION` statement and ends with the `COMMIT` statement. Either the effect of all operations will be published, or no results will be published at all.

:::warning
Transaction control statements are only allowed within a [script](../developers-guide/sql/sql-api.md#sql-scripts).
:::

In Apache Ignite 3, you start the transaction by using the `START TRANSACTION` statement:

{/* Railroad diagram omitted - see 3.1.0 docs */}

:::note
DDL statements are not supported inside transactions.
:::

Parameters:

- `READ WRITE` - both read and write operations are allowed in the transaction. Used by default.
- `READ ONLY` - only read operations are allowed in the transaction.

You close and commit the transaction by using the `COMMIT` statement:

{/* Railroad diagram omitted - see 3.1.0 docs */}

## Example

The example below inserts 3 lines into the table in a single transaction, ensuring they will all be committed together:

```sql
START TRANSACTION READ WRITE;

INSERT INTO Person (id, name, surname) VALUES (1, 'John', 'Smith');
INSERT INTO Person (id, name, surname) VALUES (2, 'Jane', 'Smith');
INSERT INTO Person (id, name, surname) VALUES (3, 'Adam', 'Mason');

COMMIT;
```
