---
title: Operational Commands
sidebar_label: Operational Commands
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

## SELECT

Retrieves data from a table or multiple tables.

{/* Railroad diagram omitted - see 3.1.0 docs */}

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

- `hint_comment` - an sql [optimizer hint](../sql-tuning/sql-tuning.md#optimizer-hints).
- `where_boolean_expression` - an SQL expression that is run against table records and returns a boolean value. Only the records for which `TRUE` was returned will be returned. If not specified, all matching records are returned.
- `having_boolean_expression` - an SQL expression that is run against groups and returns a boolean value. Can use [aggregate functions](operators-and-functions.md#aggregate-functions). Only the groups for which `TRUE` was returned will be returned. If not specified, all matching groups are returned.

### JOINs

Apache Ignite supports colocated and non-colocated distributed SQL joins. Furthermore, if the data resides in different tables, Apache Ignite allows for cross-table joins as well.

## COPY INTO

### Description

Imports data from an external source into a table or exports data to a file from the table. The table the data is imported into must exist.

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

* `source` - the full path to the file to import the data data from (if `target` is a table). Then name of the table and the list of columns to export the data from (if `target` is a file). If the source is a table, you can further narrow down the selection by using a query.
* `target` - the table and table columns when importing data to the table. The path to the file when exporting data.
* `formatType` - the format of the file to work with:
  * `CSV`
  * `PARQUET`
  * `ICEBERG`
* `WITH` - accepts additional parameters. All properties are case-sensitive:
  * `delimiter` -  for CSV files only; default delimiter is `,`. Delimiter syntax is `'char'`. Any alphanumeric character can be a delimiter.
  * `pattern` - the file pattern used when importing partitioned Parquet tables in the regular expression format. The regular expression must be enclosed in `'` signs. For example, `'.*'` imports all files. Partitioned column will not be imported.
  * `header` - for CSV files only; if set to `true`, specifies that the created file should contain a header line with column names. Column names of the table are used to create the header line.
  * `quoteChar` - for CSV files only; the character to use for quoted elements. Default quote character is `"`.
  * `escapeChar` - for CSV files only; the character to use for escaping a separator or quote. Default escape character is `\`.
  * `ignoreLeadingWhiteSpace` - for CSV files only; if `true`, ignore leading whitespace, keep them otherwise. Default value is `true`.
  * `ignoreQuotations` - for CSV files only; if `true`, quoted elements are ignored, otherwise they are kept. Default value is `false`.
  * `strictQuotes` - for CSV files only; if `true`, characters outside the quoted elements are ignored, otherwise they are kept. Default value is `false`.
  * `batchSize` - the number of entries loaded in each batch. Default value is `1024`.
  * `null` - what value empty values are mapped to on import, and `null` values are mapped to on export. By default, empty values are mapped to `null` on import, and `null` values are mapped to an empty string (`""`) on export.
  * `S3`:
    * `s3.client-region` - when using S3 storage, the region to store the data in.
    * `s3.access-key-id` - when using S3 storage, the AWS access key.
    * `s3.secret-access-key` - when using S3 storage,  the AWS secret key.
    * Iceberg-specific:
      * The [catalog properties](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties) are supported.
      * The `table-identifier` property describes Apache Iceberg TableIdentifier names. The names can be dot-separated. For example, `db_name.table_name` or `table`.
      * The `warehouse` path can be defined explicitly as a `'warehouse'='path'` property, or implicitly as a source or target `COPY FROM source INTO target`. If both ways are defined, the explicit property is used.

### Examples

Imports data from columns `name` and `age` of a CSV file with header into Table1 columns `name` and `age`:

```sql
/* Import data from CSV with column headers */
COPY FROM '/path/to/dir/data.csv'
INTO Table1 (name, age)
FORMAT CSV
```

Imports data from the first two columns of a CSV file without header into Table1 columns `name` and `age`:

```sql
/* Import data from CSV without column headers  */
COPY FROM '/path/to/dir/data.csv'
INTO Table1 (name, age)
```

Imports data from columns `name` and `age` of a CSV file that uses the `~` symbol as quotation character:

```sql
/* Import data from CSV with custom quotation character */
COPY FROM '/path/to/dir/data.csv'
INTO Table1 (name, age)
FORMAT CSV
WITH 'quoteChar'='~'
```

Imports data from columns `name`, `age` and `empty` of a CSV file and maps all empty values to `no data`:

```sql
/* Import data from CSV with custom quotation character */
COPY FROM '/path/to/dir/data.csv'
INTO Table1 (name, age, empty)
FORMAT CSV
WITH 'null'='no data'
```

Imports data from CSV table in batches of 2048 entries:

```sql
/* Import data from CSV without column headers  */
COPY FROM '/path/to/dir/data.csv'
INTO Table1 (name, age)
WITH 'batchSize'='2048'
```

Exports data from Table1 to a CSV file:

```sql
/* Export data to CSV */
COPY FROM (SELECT name, age FROM Table1)
INTO  '/path/to/dir/data.csv'
FORMAT CSV
```

Imports CSV file from AWS S3 into Table1:

```sql
/* Import CSV file from s3 */
COPY FROM 's3://mybucket/data.csv'
INTO Table1 (name, age)
FORMAT CSV
WITH 'delimiter'= '|', 's3.access-key-id' = 'keyid', 's3.secret-access-key' = 'secretkey'
```

A simple example of exporting data to Iceberg. For working with local file system you can use HadoopCatalog that does not need to connect to a Hive MetaStore.

```sql
COPY FROM Table1 (id,name,height)
INTO '/tmp/person.i/'
FORMAT ICEBERG
WITH 'table-identifier'='person', 'catalog-impl'='org.apache.iceberg.hadoop.HadoopCatalog'
```

Exports data into Iceberg on AWS S3:

```sql
COPY FROM person (id,name,height)
INTO 's3://iceberg-warehouse/glue-catalog'
FORMAT ICEBERG
WITH
    'table-identifier'='iceberg_db_1.person',
    'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
    'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
    's3.client-region'='eu-central-1',
    's3.access-key-id'='YOUR_KEY',
    's3.secret-access-key'='YOUR_SECRET'
```

:::warning
Glue catalog requires the table identifier pattern `db_name.table_name - [a-z0-9_]` (all letters must be in low case with underscores and no spaces). It can be disabled by setting the `glue.skip-name-validation` property to `true` to skip validation. When database name and table name validation are skipped, there is no guarantee that downstream systems would all support the names.
:::

Imports data from partitioned Parquet database:

```sql
COPY FROM '/tmp/partitioned_table_dir'
INTO city (id, name, population)
FORMAT PARQUET
WITH 'pattern' = '.*'
```

Where the Parquet table looks like this:

```
partitioned_table_dir/
├─ CountryCode=USA/
│  ├─ 000000_0.parquet
├─ CountryCode=FR/
│  ├─ 000000_0.parquet
```

## KILL QUERY

Cancels a running query. When a query is canceled with the `KILL` command, all parts of the query running on all other nodes are canceled too.

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

* `query_id` - query identifier that can be retrieved via the `SQL_QUERIES` [system view](../administrators-guide/metrics/system-views.md).
* `NO WAIT` - if specified, the command will return control immediately, without waiting for the query to be cancelled. You can monitor query status through the `SQL_QUERIES` [system view](../administrators-guide/metrics/system-views.md) to make sure it was cancelled.

## KILL TRANSACTION

Cancels an active transaction.

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

* `transaction_id` - transaction identifier that can be retrieved via the `TRANSACTIONS` [system view](../administrators-guide/metrics/system-views.md).
* `NO WAIT` - if specified, the command will return control immediately, without waiting for the transaction to be cancelled. You can monitor transaction status through the `TRANSACTIONS` [system view](../administrators-guide/metrics/system-views.md) to make sure it was cancelled.

## KILL COMPUTE

Cancels a running compute job. When a job is canceled with the `KILL` command, all parts of the job running on all other nodes are canceled too.

{/* Railroad diagram omitted - see 3.1.0 docs */}

### Parameters

* `job_id` - job identifier that can be retrieved via the `COMPUTE_JOBS` [system view](../administrators-guide/metrics/system-views.md).
* `NO WAIT` - if specified, the command will return control immediately, without waiting for the job to be cancelled. You can monitor job status through the `COMPUTE_JOBS` [system view](../administrators-guide/metrics/system-views.md) to make sure it was cancelled.
