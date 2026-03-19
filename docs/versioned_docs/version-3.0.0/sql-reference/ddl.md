---
title: Data Definition Language (DDL)
sidebar_label: DDL
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

This section walks you through all data definition language (DDL) commands supported by Ignite 3.0.

## CREATE TABLE

Creates a new table.

:::note
This can also be done via the [Java API](../developers-guide/java-to-tables.md).
:::

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF NOT EXISTS` - create the table only if a table with the same name does not exist.
* `COLOCATE BY` - colocation key. The key can be composite. Primary key must include colocation key.
* `ZONE` - sets the [Distribution Zone](distribution-zones.md). Can be specified as a case-sensitive string or case-insensitive identifier. Does not need to exist at the moment of table creation, and can be created before writing data.
* `STORAGE PROFILE` - sets the [storage profile](../administrators-guide/config/node-config.md#storage-configuration) that will be used to store the table. Must be specified as a case-sensitive string.

Examples:

Creates a Person table:

```sql
CREATE TABLE IF NOT EXISTS Person (
  id int primary key,
  city_id int,
  name varchar,
  age int,
  company varchar
)
```

Creates a Person table that uses distribution zone `MYZONE`:

```sql
CREATE TABLE IF NOT EXISTS Person (
  id int primary key,
  city_id int,
  name varchar,
  age int,
  company varchar
) ZONE MYZONE
```

Creates a Person table that uses the `default` storage profile regardless of the storage profile specified in the distribution zone:

```sql
CREATE TABLE IF NOT EXISTS Person (
  id int primary key,
  city_id int,
  name varchar,
  age int,
  company varchar
) PRIMARY ZONE MYZONE STORAGE PROFILE 'default'
```

Creates a Person table where the default value of the `city_id` column is 1:

```sql
CREATE TABLE IF NOT EXISTS Person (
  id int primary key,
  city_id int default 1,
  name varchar,
  age int,
  company varchar
)
```

## ALTER TABLE

Modifies the structure of an existing table.

### ALTER TABLE IF EXISTS table ADD COLUMN (column1 int, column2 int)

Adds column(s) to an existing table.

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.

Examples:

Add a column to the table:

```sql
ALTER TABLE Person ADD COLUMN city varchar;
```

Add a column only if the table exists:

```sql
ALTER TABLE IF EXISTS Person ADD number bigint;
```

Add several columns to the table at once:

```sql
ALTER TABLE Person ADD COLUMN (code varchar, gdp double);
```

### ALTER TABLE IF EXISTS table DROP COLUMN (column1, column2 int)

Removes column(s) from an existing table. Once a column is removed, it cannot be accessed within queries. This command has the following limitations:

- If the column was indexed, the index has to be dropped manually in advance by using the 'DROP INDEX' command.
- It is not possible to remove a column if it represents the entire value stored in the cluster. The limitation is relevant for primitive values.

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.

Examples:

Drop a column from the table:

```sql
ALTER TABLE Person DROP COLUMN city;
```

Drop a column only if the table exists:

```sql
ALTER TABLE IF EXISTS Person DROP COLUMN number;
```

Drop several columns from the table at once:

```sql
ALTER TABLE Person DROP COLUMN (code, gdp);
```

### ALTER TABLE IF EXISTS table ALTER COLUMN column SET DATA TYPE

Changes the data type for the column(s) in an existing table.

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.
* `data_type` - a valid [data type](data-types.md).

Examples:

Alter a column in the table:

```sql
ALTER TABLE Person ALTER COLUMN city SET DATA TYPE varchar;
```

#### Supported Transitions

Not all data type transitions are supported. The limitations are listed below:

* `FLOAT` can be transitioned to `DOUBLE`
* `INT8`, `INT16` and `INT32` can be transitioned to `INT64`
* `TYPE SCALE` change is forbidden
* `TYPE PRECISION` increase is allowed for DECIMAL non PK column
* `TYPE LENGTH` increase is allowed for STRING and BYTE_ARRAY non PK column

Other transitions are not supported.

Examples:

Changes the possible range of IDs to BIGINT ranges:

```sql
ALTER TABLE Person ALTER COLUMN age SET DATA TYPE BIGINT
```

Sets the length of a column text to 11:

```sql
ALTER TABLE Person ALTER COLUMN Name SET DATA TYPE varchar(11)
```

### ALTER TABLE IF EXISTS table ALTER COLUMN column SET NOT NULL

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.

#### Supported Transitions

Not all data type transitions are supported. The limitations are listed below:

* `NULLABLE` to `NOT NULL` transition is forbidden

### ALTER TABLE IF EXISTS table ALTER COLUMN column DROP NOT NULL

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.

#### Supported Transitions

Not all data type transitions are supported. The limitations are listed below:

* `NOT NULL` to `NULLABLE` transition is allowed for any non-PK column

### ALTER TABLE IF EXISTS table ALTER COLUMN column SET DEFAULT

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.

### ALTER TABLE IF EXISTS table ALTER COLUMN column DROP DEFAULT

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the specified table name does not exist.

## DROP TABLE

The `DROP TABLE` command drops an existing table. The table will be marked for deletion and will be removed by garbage collection after the [low watermark](../administrators-guide/storage/data-partitions.md#version-storage) point is reached. Until the data is removed, it will be available to [read-only transactions](../developers-guide/transactions.md#read-only-transactions) that check the time before the table was marked for deletion.

:::note
This can also be done via the [Java API](../developers-guide/java-to-tables.md).
:::

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF EXISTS` - do not throw an error if a table with the same name does not exist.

Examples:

Drop Person table if the one exists:

```sql
DROP TABLE IF EXISTS "Person";
```

## CREATE INDEX

Creates a new index.

:::note
This can also be done via the [Java API](../developers-guide/java-to-tables.md).
:::

When you create a new index, it will start building only after all transactions started before the index creation had been completed. Index build will not start if there are any "hung" transactions in the logical topology of the cluster.

The index status, with the status reason description (e.g., PENDING - "Waiting for transaction ABC to complete") is reflected in the system view.

:::note
The index cannot include the same column more than once.
:::

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `IF NOT EXISTS` - create the index only if an index with the same name does not exist.
* `name` - name of the index.
* `ON` - create index on the defined table.
* `USING TREE` -if specified, creates a tree index.
* `USING HASH` - if specified, creates a hash index.

Examples:

Create an index `department_name_idx` for the Person table:

```sql
CREATE INDEX IF NOT EXISTS department_name_idx ON Person (department_id DESC, name ASC);
```

Create a hash index `department_name_idx` for the Person table:

```sql
CREATE INDEX name_surname_idx ON Person USING HASH (name, surname);
```

Create a tree index `department_city_idx` for the Person table:

```sql
CREATE INDEX department_city_idx ON Person USING TREE (department_id ASC, city_id DESC);
```

## DROP INDEX

Drops an index.

:::note
This can also be done via the [Java API](../developers-guide/java-to-tables.md).
:::

When you drop an index, it stays in the STOPPING status until all transactions started before the DROP INDEX command had been completed (even those that do not affect any of the tables for which the index is being dropped).
Upon completion of all transactions described above, the space the dropped index had occupied is freed up only when LWM of the relevant partition becomes greater than the time when the index dropping had been activated.
The index status, with the status reason description (e.g., PENDING - "Waiting for transaction ABC to complete") is reflected in the system view.

{/* Railroad diagram omitted - see 3.1.0 docs */}

Keywords and parameters:

* `index_name` - the name of the index.
* `IF EXISTS` - do not throw an error if an index with the specified name does not exist.

Examples:

Drop index if the one exists:

```sql
DROP INDEX IF EXISTS department_name_idx;
```
