---
title: SQL API
id: sql-api
sidebar_position: 5
---

# SQL API

The SQL API executes queries and DML statements against Ignite tables. Applications use standard SQL syntax to retrieve, insert, update, and delete data. The API supports parameterized queries, prepared statements, batch operations, and result streaming.

## Key Concepts

SQL execution operates through the IgniteSql facade. Query results stream through ResultSet cursors that must be closed to release resources. Statements configure query behavior including timeouts, schemas, and pagination.

Queries execute within optional transaction contexts. Pass null for auto-commit execution or provide a Transaction for multi-statement atomicity. Query parameters use positional binding with question mark placeholders.

## Basic Query Execution

Execute queries with parameters:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    "SELECT name, age FROM users WHERE age > ?",
    25
)) {
    while (rs.hasNext()) {
        SqlRow row = rs.next();
        System.out.println(row.stringValue("name") + ": " + row.intValue("age"));
    }
}
```

Always close ResultSet instances to free server resources.

## Prepared Statements

Create prepared statements for repeated execution:

```java
Statement stmt = ignite.sql().createStatement(
    "SELECT * FROM users WHERE age > ? AND status = ?"
);

try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    stmt,
    30,
    "active"
)) {
    // Process results
}
```

Use the statement builder for configuration:

```java
Statement stmt = ignite.sql().statementBuilder()
    .query("SELECT * FROM users WHERE age > ?")
    .defaultSchema("public")
    .queryTimeout(30, TimeUnit.SECONDS)
    .pageSize(1000)
    .build();
```

## Statement Configuration

Configure statement behavior through builder options:

```java
Statement stmt = ignite.sql().statementBuilder()
    .query("SELECT * FROM products WHERE category = ?")
    .defaultSchema("inventory")
    .queryTimeout(60, TimeUnit.SECONDS)
    .pageSize(500)
    .timeZoneId(ZoneId.of("UTC"))
    .build();

try (ResultSet<SqlRow> rs = ignite.sql().execute(null, stmt, "electronics")) {
    // Process results
}
```

The defaultSchema setting determines table resolution when queries omit schema names. The queryTimeout parameter limits execution time. The pageSize controls result batching for large result sets.

## Result Set Processing

Access result metadata and values:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    "SELECT id, name, created FROM users"
)) {
    ResultSetMetadata metadata = rs.metadata();
    System.out.println("Columns: " + metadata.columns().size());

    while (rs.hasNext()) {
        SqlRow row = rs.next();

        int id = row.intValue("id");
        String name = row.stringValue("name");
        LocalDateTime created = row.value("created");

        System.out.println(id + ": " + name + " created at " + created);
    }
}
```

Column metadata provides type information:

```java
for (int i = 0; i < metadata.columns().size(); i++) {
    ColumnMetadata col = metadata.columns().get(i);
    System.out.println(col.name() + " " + col.type() +
        " nullable=" + col.nullable());
}
```

## DML Operations

Execute insert, update, and delete statements:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    1, "Alice", 30
)) {
    long affected = rs.affectedRows();
    System.out.println("Inserted " + affected + " rows");
}
```

Check affected row counts for DML statements:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    "UPDATE users SET status = ? WHERE age > ?",
    "senior", 60
)) {
    System.out.println("Updated " + rs.affectedRows() + " rows");
}
```

## DDL Operations

Execute schema definition statements:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(
    null,
    "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price DECIMAL)"
)) {
    boolean applied = rs.wasApplied();
    System.out.println("DDL applied: " + applied);
}
```

DDL statements return applied status rather than row counts.

## Batch Operations

Execute multiple statements with different parameters:

```java
BatchedArguments batch = BatchedArguments.create();

batch.add(1, "Alice");
batch.add(2, "Bob");
batch.add(3, "Carol");

long[] results = ignite.sql().executeBatch(null, "INSERT INTO users (id, name) VALUES (?, ?)", batch);
System.out.println("Inserted " + results.length + " batches");
```

Batch operations reduce network overhead for multiple similar statements.

## Typed Result Mapping

Map results to custom types using mappers:

```java
class User {
    public int id;
    public String name;
    public int age;
}

try (ResultSet<User> rs = ignite.sql().execute(
    null,
    Mapper.of(User.class),
    "SELECT id, name, age FROM users WHERE age > ?",
    25
)) {
    while (rs.hasNext()) {
        User user = rs.next();
        System.out.println(user.name + " is " + user.age + " years old");
    }
}
```

The mapper automatically converts rows to objects based on column names and field names.

## Asynchronous Execution

Execute queries asynchronously:

```java
CompletableFuture<AsyncResultSet<SqlRow>> future = ignite.sql().executeAsync(
    null,
    "SELECT * FROM users WHERE age > ?",
    30
);

future.thenAccept(rs -> {
    try (rs) {
        while (rs.hasNext()) {
            SqlRow row = rs.next();
            System.out.println(row.stringValue("name"));
        }
    }
});
```

Asynchronous execution returns immediately without blocking the calling thread.

## Query Cancellation

Cancel long-running queries using cancellation handles:

```java
CancelHandle cancelHandle = CancelHandle.create();

CompletableFuture<AsyncResultSet<SqlRow>> future = ignite.sql().executeAsync(
    null,
    cancelHandle.token(),
    "SELECT * FROM large_table",
    new Object[0]
);

// Cancel after 5 seconds
CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS)
    .execute(cancelHandle::cancel);
```

Cancelled queries stop execution and release resources.

## Transaction Integration

Execute queries within transactions:

```java
ignite.transactions().runInTransaction(tx -> {
    try (ResultSet<SqlRow> rs = ignite.sql().execute(
        tx,
        "SELECT balance FROM accounts WHERE id = ?",
        1
    )) {
        SqlRow row = rs.next();
        int balance = row.intValue("balance");

        ignite.sql().execute(
            tx,
            "UPDATE accounts SET balance = ? WHERE id = ?",
            balance + 100,
            1
        ).close();
    }
});
```

All statements using the same transaction see consistent data and commit atomically.

## Result Set Types

ResultSet indicates result type through metadata:

```java
try (ResultSet<SqlRow> rs = ignite.sql().execute(null, query)) {
    if (rs.hasRowSet()) {
        // SELECT query, process rows
        while (rs.hasNext()) {
            SqlRow row = rs.next();
            // Process row
        }
    } else {
        // DML or DDL
        if (rs.affectedRows() >= 0) {
            // DML operation
            System.out.println("Affected: " + rs.affectedRows());
        } else {
            // DDL operation
            System.out.println("Applied: " + rs.wasApplied());
        }
    }
}
```

## SqlRow Access

Access row values by name or index:

```java
SqlRow row = rs.next();

// By column name
int id = row.intValue("id");
String name = row.stringValue("name");
Double price = row.value("price");

// By index
Object value0 = row.value(0);
String column0 = row.columnName(0);

// Access metadata
ResultSetMetadata metadata = row.metadata();
int columnCount = row.columnCount();
```

SqlRow extends Tuple, providing all tuple access methods.

## Reference

- SQL facade: `org.apache.ignite.sql.IgniteSql`
- Statements: `org.apache.ignite.sql.Statement`
- Results: `org.apache.ignite.sql.ResultSet<T>`
- Rows: `org.apache.ignite.sql.SqlRow`
- Metadata: `org.apache.ignite.sql.ResultSetMetadata`, `org.apache.ignite.sql.ColumnMetadata`
- Batching: `org.apache.ignite.sql.BatchedArguments`

### IgniteSql Methods

- `Statement createStatement(String query)` - Create statement from query
- `Statement.StatementBuilder statementBuilder()` - Create statement builder
- `ResultSet<SqlRow> execute(Transaction, String query, Object...)` - Execute query with parameters
- `ResultSet<SqlRow> execute(Transaction, Statement, Object...)` - Execute prepared statement
- `CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(...)` - Async execution
- `ResultSet<SqlRow> execute(Transaction, CancellationToken, String query, Object...)` - Execute with cancellation
- `<R> ResultSet<R> execute(Transaction, Mapper<R>, String query, Object...)` - Execute with result mapping
- `long[] executeBatch(Transaction, String dmlQuery, BatchedArguments)` - Execute batch
- `CompletableFuture<long[]> executeBatchAsync(Transaction, String query, BatchedArguments)` - Execute batch asynchronously

### Statement Configuration

- `String query()` - Get query string
- `long queryTimeout(TimeUnit)` - Get timeout
- `String defaultSchema()` - Get default schema
- `int pageSize()` - Get result page size
- `ZoneId timeZoneId()` - Get time zone

### Statement Builder Methods

- `query(String)` - Set query string
- `defaultSchema(String)` - Set default schema
- `queryTimeout(long, TimeUnit)` - Set query timeout
- `pageSize(int)` - Set result page size
- `timeZoneId(ZoneId)` - Set time zone
- `build()` - Build statement

### ResultSet Methods

- `ResultSetMetadata metadata()` - Get result metadata
- `boolean hasRowSet()` - Check if contains rows
- `long affectedRows()` - Get affected row count
- `boolean wasApplied()` - Check if DDL was applied
- `boolean hasNext()` - Check for more rows
- `T next()` - Get next row
- `void close()` - Close result set

### ResultSetMetadata Methods

- `List<ColumnMetadata> columns()` - Get list of column metadata
- `int indexOf(String columnName)` - Get column index by name

### ColumnMetadata Methods

- `String name()` - Column name
- `ColumnType type()` - Column type
- `boolean nullable()` - Nullability
- `int precision()` - Precision
- `int scale()` - Scale
- `Class<?> valueClass()` - Value class
- `ColumnOrigin origin()` - Column origin
