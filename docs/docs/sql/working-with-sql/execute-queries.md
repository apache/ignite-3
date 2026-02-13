---
id: execute-queries
title: Executing SQL Queries
sidebar_label: Execute Queries
---

# Java SQL API

In your Java projects, you can use the Java SQL API to execute SQL statements and getting results.

## Creating Tables

Here is an example of how you can create a new table on a cluster:

```java
client.sql().executeScript(
        "CREATE TABLE CITIES ("
                + "ID   INT PRIMARY KEY,"
                + "NAME VARCHAR);"

                + "CREATE TABLE ACCOUNTS ("
                + "    ACCOUNT_ID INT PRIMARY KEY,"
                + "    CITY_ID    INT,"
                + "    FIRST_NAME VARCHAR,"
                + "    LAST_NAME  VARCHAR,"
                + "    BALANCE    DOUBLE)"
);
```

### Using Sequences

When creating a table, you can designate the primary key column to be filled automatically from the sequence the values for your primary key by using sql sequences:

```java
client.sql().execute(null, "CREATE SEQUENCE IF NOT EXISTS defaultSequence;");
client.sql().execute(null, "CREATE TABLE IF NOT EXISTS Person (ID BIGINT DEFAULT NEXTVAL('defaultSequence') PRIMARY KEY, "
        + "CITY_ID BIGINT, "
        + "NAME VARCHAR, "
        + "AGE INT, "
        + "COMPANY VARCHAR);");


client.sql().execute(null,
        "INSERT INTO Person (CITY_ID, NAME, AGE, COMPANY) VALUES " +
                "(1, 'Alice', 30, 'Google'), " +
                "(2, 'Bob', 40, 'Meta'), " +
                "(3, 'Charlie', 25, 'Spotify')");
```

## Filling Tables

With Apache Ignite 3, you can fill the table by adding rows one by one, or in a batch. In both cases, you create an `INSERT` statement, and then execute it:

```java
rowsAdded = Arrays.stream(client.sql().executeBatch(tx,
                "INSERT INTO ACCOUNTS (ACCOUNT_ID, CITY_ID, FIRST_NAME, LAST_NAME, BALANCE) values (?, ?, ?, ?, ?)",
                BatchedArguments.of(1, 1, "John", "Doe", 1000.0d)
                        .add(2, 1, "Jane", "Roe", 2000.0d)
                        .add(3, 2, "Mary", "Major", 1500.0d)
                        .add(4, 3, "Richard", "Miles", 1450.0d)))
        .sum();

System.out.println("\nAdded accounts: " + rowsAdded);
```

## Partition-Specific SELECTs

When executing a SELECT operation, you can use the system `__part` column to only `SELECT` data in a specific partition. To find out partition information, use the SELECT request that explicitly includes the `__part` column as its part:

```sql
SELECT city_id, id, "__part"  FROM Person;
```

Once you know the partition, you can use it in the `WHERE` clause:

```sql
SELECT city_id, id FROM Person WHERE "__part"=23;
```

## Getting Data From Tables

To get data from a table, execute the `SELECT` statement to get a set of results. SqlRow can provide access to column values by column name or column index. You can then iterate through results to get data.

:::note
Always close the `ResultSet`, either by using a `try-with-resources` statement or by calling its `close()` method directly.
:::

```java
try (ResultSet<SqlRow> rs = client.sql().execute(null,
        "SELECT a.FIRST_NAME, a.LAST_NAME, c.NAME FROM ACCOUNTS a "
                + "INNER JOIN CITIES c on c.ID = a.CITY_ID ORDER BY a.ACCOUNT_ID")) {
    while (rs.hasNext()) {
        SqlRow row = rs.next();

        System.out.println("    "
                + row.stringValue(0) + ", "
                + row.stringValue(1) + ", "
                + row.stringValue(2));
    }
}
```

## SQL Scripts

The default API executes SQL statements one at a time. For large SQL statements, pass them to the `executeScript()` method. The statements will be batched together similar to using `SET STREAMING` command in Apache Ignite 2, significantly improving performance when executing a large number of queries at once. These statements will be executed in order.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="java" label="Java">

```java
String script = "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar, age int default 0);"
              + "INSERT INTO Person (id, name, age) VALUES ('1', 'John', '46');";
client.sql().executeScript(script);
```

</TabItem>
</Tabs>

:::note
Execution of each statement is considered complete when the first page is ready to be returned. As a result, when working with large data sets, SELECT statement may be affected by later statements in the same script.
:::

### Query Cancellation

To cancel a query, create and pass the cancellation token to the execution method:

<Tabs>
<TabItem value="java" label="Java">

```java
CancelHandle cancelHandle = CancelHandle.create();
CancellationToken cancelToken = cancelHandle.token();

client.sql().executeAsync(
        null, cancelToken,
        "SELECT a.FIRST_NAME, b.LAST_NAME " +
                "FROM ACCOUNTS a, ACCOUNTS b, ACCOUNTS c ORDER BY a.ACCOUNT_ID"
);
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
std::shared_ptr<cancel_handle> handle = cancel_handle::create();
std::shared_ptr<cancellation_token> token = handle->get_token();

client.get_sql().execute(nullptr, token.get(), "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar, age int);", {});
```

</TabItem>
</Tabs>

After the query is submitted, you can cancel all queries that use the tokens from the same `cancelHandle` object at any point by using the `cancel()` or `cancelAsync()` methods, for example:

<Tabs>
<TabItem value="java" label="Java">

```java
CompletableFuture<Void> cancelled = cancelHandle.cancelAsync();
cancelled.get(5, TimeUnit.SECONDS);

System.out.println("\nIs query cancelled: " + cancelled.isDone());
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
var cts = new CancellationTokenSource();
await using var resultSet = await Client.Sql.ExecuteAsync(null, "CREATE TABLE IF NOT EXISTS Person (id int primary key)", cts.Token);
await cts.CancelAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
handle->cancel_async(ignite_result<void> cancellationResult) {
// Handle cancellationResult here
});
```

</TabItem>
</Tabs>

Another way to cancel queries is by using the SQL [KILL QUERY](/3.1.0/sql/reference/data-types-and-functions/operational-commands#kill-query) command. The query id can be retrieved via the `SQL_QUERIES` [system view](/3.1.0/configure-and-operate/monitoring/metrics-system-views).
