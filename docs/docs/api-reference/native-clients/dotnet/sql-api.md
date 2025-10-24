---
title: SQL API
id: sql-api
sidebar_position: 4
---

# SQL API

The SQL API executes SQL queries and scripts against Ignite tables. It supports parameterized queries, typed result mapping, metadata access, and both result set and data reader patterns for consuming query results.

## Key Concepts

SQL queries in Ignite 3 execute against distributed tables using a Calcite-based SQL engine. Queries can span multiple tables and leverage distributed execution across cluster nodes.

### Result Handling

Query results are available through two interfaces. IResultSet provides async enumeration with full metadata access and is suitable for LINQ operations. IgniteDbDataReader provides forward-only access compatible with ADO.NET patterns.

### Transaction Integration

All SQL operations accept an optional transaction parameter. Pass null for auto-commit mode or pass an ITransaction to execute queries within a transaction scope. This ensures consistency across SQL and key-value operations.

### Lazy Loading

Result sets use lazy loading. Rows are fetched from the cluster only as you enumerate them. This reduces memory usage for large result sets but means result sets can only be enumerated once.

## Usage Examples

### Basic Query Execution

```csharp
var sql = client.Sql;

// Execute query returning untyped tuples
var statement = new SqlStatement("SELECT * FROM customers WHERE region = ?");
var resultSet = await sql.ExecuteAsync(null, statement, "West");

await foreach (var row in resultSet)
{
    Console.WriteLine($"Customer: {row["name"]}, Email: {row["email"]}");
}
```

### Typed Query Results

```csharp
public class CustomerDto
{
    public long Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

var statement = new SqlStatement("SELECT id, name, email FROM customers WHERE region = ?");
var resultSet = await sql.ExecuteAsync<CustomerDto>(null, statement, "West");

await foreach (var customer in resultSet)
{
    Console.WriteLine($"{customer.Name}: {customer.Email}");
}
```

### Parameterized Queries

```csharp
// Positional parameters
var stmt = new SqlStatement(
    "SELECT * FROM orders WHERE customer_id = ? AND order_date > ?");
var results = await sql.ExecuteAsync(
    null, stmt, customerId, DateTime.UtcNow.AddDays(-30));

await foreach (var order in results)
{
    Console.WriteLine($"Order {order["order_id"]}: ${order["amount"]}");
}
```

### DML Operations

```csharp
// Insert
var insertStmt = new SqlStatement(
    "INSERT INTO customers (id, name, email) VALUES (?, ?, ?)");
var insertResult = await sql.ExecuteAsync(
    null, insertStmt, 100L, "Alice", "alice@example.com");

Console.WriteLine($"Inserted {insertResult.AffectedRows} rows");

// Update
var updateStmt = new SqlStatement(
    "UPDATE customers SET email = ? WHERE id = ?");
var updateResult = await sql.ExecuteAsync(
    null, updateStmt, "alice@newdomain.com", 100L);

Console.WriteLine($"Updated {updateResult.AffectedRows} rows");

// Delete
var deleteStmt = new SqlStatement("DELETE FROM customers WHERE id = ?");
var deleteResult = await sql.ExecuteAsync(null, deleteStmt, 100L);

Console.WriteLine($"Deleted {deleteResult.AffectedRows} rows");
```

### DDL Operations

```csharp
// Create table
var createStmt = new SqlStatement(@"
    CREATE TABLE IF NOT EXISTS products (
        id BIGINT PRIMARY KEY,
        name VARCHAR,
        price DECIMAL(10, 2)
    )");

var result = await sql.ExecuteAsync(null, createStmt);
Console.WriteLine($"Table created: {result.WasApplied}");

// Drop table
var dropStmt = new SqlStatement("DROP TABLE IF EXISTS products");
await sql.ExecuteAsync(null, dropStmt);
```

### Using Data Reader

```csharp
var statement = new SqlStatement("SELECT * FROM orders WHERE amount > ?");
using var reader = await sql.ExecuteReaderAsync(null, statement, 100.0);

while (await reader.ReadAsync())
{
    var orderId = reader.GetInt64(0);
    var amount = reader.GetDecimal(3);
    Console.WriteLine($"Order {orderId}: ${amount}");
}
```

### Batch Execution

```csharp
var statement = new SqlStatement(
    "INSERT INTO customers (id, name, email) VALUES (?, ?, ?)");

var argSets = new[]
{
    new object[] { 1L, "Alice", "alice@example.com" },
    new object[] { 2L, "Bob", "bob@example.com" },
    new object[] { 3L, "Carol", "carol@example.com" }
};

var affectedRows = await sql.ExecuteBatchAsync(null, statement, argSets);

for (int i = 0; i < affectedRows.Length; i++)
{
    Console.WriteLine($"Statement {i}: {affectedRows[i]} rows affected");
}
```

### Script Execution

```csharp
var script = new SqlStatement(@"
    CREATE TABLE temp_data (id BIGINT PRIMARY KEY, value VARCHAR);
    INSERT INTO temp_data VALUES (1, 'test');
    INSERT INTO temp_data VALUES (2, 'data');
");

await sql.ExecuteScriptAsync(script);
Console.WriteLine("Script executed successfully");
```

### Query with Metadata

```csharp
var statement = new SqlStatement("SELECT id, name, email, created_at FROM customers");
var resultSet = await sql.ExecuteAsync(null, statement);

if (resultSet.Metadata != null)
{
    Console.WriteLine("Columns:");
    foreach (var column in resultSet.Metadata.Columns)
    {
        Console.WriteLine($"  {column.Name}: {column.Type} " +
            $"(nullable: {column.Nullable}, precision: {column.Precision})");
    }
}

await foreach (var row in resultSet)
{
    // Process rows
}
```

### Transactional Queries

```csharp
var tx = await client.Transactions.BeginAsync();
try
{
    // Query within transaction
    var selectStmt = new SqlStatement(
        "SELECT balance FROM accounts WHERE id = ?");
    var result = await sql.ExecuteAsync<Account>(tx, selectStmt, accountId);

    var accounts = await result.ToListAsync();
    var account = accounts[0];

    // Update within same transaction
    var updateStmt = new SqlStatement(
        "UPDATE accounts SET balance = ? WHERE id = ?");
    await sql.ExecuteAsync(tx, updateStmt, account.Balance - 100, accountId);

    await tx.CommitAsync();
}
catch
{
    await tx.RollbackAsync();
    throw;
}
```

### Cancellation Support

```csharp
using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(30));

try
{
    var statement = new SqlStatement("SELECT * FROM large_table");
    var resultSet = await sql.ExecuteAsync(null, statement, cts.Token);

    await foreach (var row in resultSet.WithCancellation(cts.Token))
    {
        // Process rows
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Query cancelled");
}
```

### Collecting Results

```csharp
var statement = new SqlStatement("SELECT id, name FROM customers");
var resultSet = await sql.ExecuteAsync<CustomerDto>(null, statement);

// Collect to list
var customers = await resultSet.ToListAsync();

// Collect to dictionary
var customerMap = await resultSet.ToDictionaryAsync(
    c => c.Id,
    c => c.Name);

// Custom collection
var customResult = await resultSet.CollectAsync(
    constructor: size => new List<CustomerDto>(size),
    accumulator: (list, customer) => list.Add(customer));
```

## Reference

### ISql Interface

Query execution methods:

- **ExecuteAsync(ITransaction?, SqlStatement, params object?[]?)** - Execute query returning IResultSet&lt;IIgniteTuple&gt;
- **ExecuteAsync(ITransaction?, SqlStatement, CancellationToken, params object?[]?)** - With cancellation token
- **ExecuteAsync&lt;T&gt;(ITransaction?, SqlStatement, params object?[]?)** - Execute query returning IResultSet&lt;T&gt;
- **ExecuteAsync&lt;T&gt;(ITransaction?, SqlStatement, CancellationToken, params object?[]?)** - With cancellation token

Data reader methods:

- **ExecuteReaderAsync(ITransaction?, SqlStatement, params object?[]?)** - Return forward-only data reader
- **ExecuteReaderAsync(ITransaction?, SqlStatement, CancellationToken, params object?[]?)** - With cancellation token

Batch and script methods:

- **ExecuteScriptAsync(SqlStatement, params object?[]?)** - Execute multi-statement script
- **ExecuteScriptAsync(SqlStatement, CancellationToken, params object?[]?)** - With cancellation token
- **ExecuteBatchAsync(ITransaction?, SqlStatement, IEnumerable&lt;IEnumerable&lt;object?&gt;&gt;, CancellationToken)** - Execute statement with multiple argument sets (DML only)

### IResultSet&lt;T&gt; Interface

Properties:

- **Metadata** - Result set metadata (null for DML/DDL statements)
- **HasRowSet** - True if result contains rows (SELECT queries)
- **AffectedRows** - Number of rows affected by DML operation (0 for DDL, -1 if not applicable)
- **WasApplied** - True if conditional DDL statement (CREATE IF NOT EXISTS) was applied

Enumeration:

- Implements **IAsyncEnumerable&lt;T&gt;** for async iteration
- Can only be enumerated once

Collection methods:

- **ToListAsync()** - Collect all rows into a list
- **ToDictionaryAsync&lt;TK, TV&gt;(Func&lt;T, TK&gt; keySelector, Func&lt;T, TV&gt; valSelector, IEqualityComparer&lt;TK&gt;?)** - Collect into dictionary
- **CollectAsync&lt;TResult&gt;(Func&lt;int, TResult&gt; constructor, Action&lt;TResult, T&gt; accumulator)** - Custom collection logic

Resource management:

- Implements **IAsyncDisposable** and **IDisposable**
- Automatically disposed after enumeration completes

### IResultSetMetadata Interface

Properties:

- **Columns** - Read-only list of column metadata in result order

Methods:

- **IndexOf(string columnName)** - Get column index by name (returns -1 if not found)

### IColumnMetadata Interface

Properties:

- **Name** - Column name
- **Type** - Column data type (ColumnType enum)
- **Precision** - Column precision (-1 if not applicable)
- **Scale** - Column scale for numeric types
- **Nullable** - Whether column allows null values
- **Origin** - Original column information for aliased columns

The precision meaning varies by type. For numeric types it represents decimal digits, for string types it represents maximum length.

### IgniteDbDataReader Class

Forward-only data reader implementing ADO.NET patterns:

- Extends **DbDataReader** for ADO.NET compatibility
- Supports **ReadAsync()** for row-by-row access
- Provides typed **Get*** methods (GetInt64, GetString, GetDecimal, etc.)
- Supports **IsDBNull()** for null checking
- Implements **IAsyncDisposable** for resource cleanup

Use this when you need forward-only access or compatibility with ADO.NET-based tools.

### SqlStatement Record

Represents a SQL statement with parameters:

- **SqlStatement(string query)** - Create statement with query text
- Supports positional parameters using ? placeholders
- Parameters passed separately to execute methods

Query text should use ? for parameter placeholders. Parameters are bound in order.
