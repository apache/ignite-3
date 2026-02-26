---
title: Transactions API
id: transactions-api
sidebar_position: 5
---

# Transactions API

The Transactions API provides ACID transaction support for coordinating multiple operations across tables. Transactions ensure atomicity, consistency, isolation, and durability for distributed data modifications.

## Key Concepts

Transactions group multiple operations into a single atomic unit. Either all operations succeed and commit together, or all operations fail and roll back together.

### Transaction Lifecycle

Begin a transaction using the Transactions API, pass the transaction object to data operations, and explicitly commit or roll back when done. The transaction must be disposed after use.

### Auto-Commit Mode

Operations that receive null for the transaction parameter execute in auto-commit mode. Each operation commits immediately after completion. Use this for single operations that do not require coordination.

### Transaction Scope

Pass the same transaction object to multiple operations to include them in the transaction scope. Operations can span different tables and mix key-value and SQL operations.

## Usage Examples

### Basic Transaction

```csharp
var transactions = client.Transactions;
var tx = await transactions.BeginAsync();

try
{
    var table = await client.Tables.GetTableAsync("accounts");
    var view = table.GetRecordView<Account>();

    // Multiple operations in transaction
    var account1 = new Account { Id = 1 };
    var account1Data = await view.GetAsync(tx, account1);

    var account2 = new Account { Id = 2 };
    var account2Data = await view.GetAsync(tx, account2);

    // Update balances
    account1Data.Value.Balance -= 100;
    account2Data.Value.Balance += 100;

    await view.UpsertAsync(tx, account1Data.Value);
    await view.UpsertAsync(tx, account2Data.Value);

    // Commit transaction
    await tx.CommitAsync();
}
catch
{
    await tx.RollbackAsync();
    throw;
}
finally
{
    await tx.DisposeAsync();
}
```

### Using Statement Pattern

```csharp
var transactions = client.Transactions;

await using (var tx = await transactions.BeginAsync())
{
    var table = await client.Tables.GetTableAsync("orders");
    var view = table.GetRecordView<Order>();

    var order = new Order
    {
        OrderId = 1000,
        CustomerId = 5,
        Amount = 99.99m,
        Status = "pending"
    };

    await view.UpsertAsync(tx, order);
    await tx.CommitAsync();
}
```

### Transaction with Exception Handling

```csharp
try
{
    await using var tx = await client.Transactions.BeginAsync();

    var table = await client.Tables.GetTableAsync("inventory");
    var view = table.GetRecordView<Product>();

    var product = new Product { Id = 100 };
    var productData = await view.GetAsync(tx, product);

    if (!productData.HasValue)
    {
        throw new Exception("Product not found");
    }

    if (productData.Value.Stock < 10)
    {
        throw new Exception("Insufficient stock");
    }

    productData.Value.Stock -= 10;
    await view.UpsertAsync(tx, productData.Value);

    await tx.CommitAsync();
}
catch (Exception ex)
{
    Console.WriteLine($"Transaction failed: {ex.Message}");
    // Transaction automatically rolls back on exception
}
```

### RunInTransactionAsync Pattern

```csharp
var transactions = client.Transactions;

// With return value
var newBalance = await transactions.RunInTransactionAsync(async tx =>
{
    var table = await client.Tables.GetTableAsync("accounts");
    var view = table.GetRecordView<Account>();

    var account = new Account { Id = 1 };
    var accountData = await view.GetAsync(tx, account);

    accountData.Value.Balance += 50;
    await view.UpsertAsync(tx, accountData.Value);

    return accountData.Value.Balance;
});

Console.WriteLine($"New balance: {newBalance}");

// Without return value
await transactions.RunInTransactionAsync(async tx =>
{
    var table = await client.Tables.GetTableAsync("logs");
    var view = table.GetRecordView<LogEntry>();

    var entry = new LogEntry
    {
        Id = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Message = "Transaction completed",
        Timestamp = DateTime.UtcNow
    };

    await view.UpsertAsync(tx, entry);
});
```

The RunInTransactionAsync method automatically commits on success and rolls back on exception. It also handles disposal automatically.

### Mixing Key-Value and SQL Operations

```csharp
await using var tx = await client.Transactions.BeginAsync();

// Key-value operation
var accountsTable = await client.Tables.GetTableAsync("accounts");
var accountsView = accountsTable.GetRecordView<Account>();

var account = new Account { Id = 1 };
var accountData = await accountsView.GetAsync(tx, account);
accountData.Value.Balance -= 100;
await accountsView.UpsertAsync(tx, accountData.Value);

// SQL operation in same transaction
var sql = client.Sql;
var insertStmt = new SqlStatement(
    "INSERT INTO transactions (account_id, amount, timestamp) VALUES (?, ?, ?)");
await sql.ExecuteAsync(tx, insertStmt, 1L, -100.0m, DateTime.UtcNow);

await tx.CommitAsync();
```

### Cross-Table Transaction

```csharp
await using var tx = await client.Transactions.BeginAsync();

// Update orders table
var ordersTable = await client.Tables.GetTableAsync("orders");
var ordersView = ordersTable.GetRecordView<Order>();

var order = new Order
{
    OrderId = 2000,
    CustomerId = 10,
    Amount = 199.99m,
    Status = "confirmed"
};
await ordersView.UpsertAsync(tx, order);

// Update inventory table
var inventoryTable = await client.Tables.GetTableAsync("inventory");
var inventoryView = inventoryTable.GetRecordView<Product>();

var product = new Product { Id = 500 };
var productData = await inventoryView.GetAsync(tx, product);
productData.Value.Stock -= 1;
await inventoryView.UpsertAsync(tx, productData.Value);

await tx.CommitAsync();
```

### Read-Only Transaction

```csharp
var options = new TransactionOptions(ReadOnly: true);
await using var tx = await client.Transactions.BeginAsync(options);

var table = await client.Tables.GetTableAsync("products");
var view = table.GetRecordView<Product>();

// Read operations only
var product1 = await view.GetAsync(tx, new Product { Id = 1 });
var product2 = await view.GetAsync(tx, new Product { Id = 2 });

// No commit needed for read-only transactions
// Transaction automatically closes on dispose
```

Read-only transactions can provide performance benefits and prevent accidental modifications.

### Transaction Timeout

```csharp
var options = new TransactionOptions(ReadOnly: false, TimeoutMillis: 30000);

await using var tx = await client.Transactions.BeginAsync(options);

try
{
    // Perform operations
    var table = await client.Tables.GetTableAsync("data");
    var view = table.GetRecordView<DataRecord>();

    // ... operations ...

    await tx.CommitAsync();
}
catch (IgniteException ex)
{
    Console.WriteLine($"Transaction timeout or conflict: {ex.Message}");
    throw;
}
```

## Reference

### ITransactions Interface

Methods:

- **ValueTask&lt;ITransaction&gt; BeginAsync()** - Begin new transaction with default options
- **ValueTask&lt;ITransaction&gt; BeginAsync(TransactionOptions options)** - Begin new transaction with specified options
- **Task&lt;T&gt; RunInTransactionAsync&lt;T&gt;(Func&lt;ITransaction, Task&lt;T&gt;&gt; func, TransactionOptions options = default)** - Execute function within transaction and return result
- **Task RunInTransactionAsync(Func&lt;ITransaction, Task&gt; func, TransactionOptions options = default)** - Execute function within transaction (no return value)

The RunInTransactionAsync methods handle transaction lifecycle automatically. They commit on successful completion and roll back on exceptions. The transaction is disposed after the function completes.

### ITransaction Interface

Properties:

- **bool IsReadOnly** - Whether transaction is read-only

Methods:

- **Task CommitAsync()** - Commit the transaction, making all changes permanent
- **Task RollbackAsync()** - Roll back the transaction, discarding all changes

Resource management:

- Implements **IAsyncDisposable** and **IDisposable**
- Must be disposed after use
- Automatic rollback occurs if disposed without explicit commit

### TransactionOptions Record Struct

A readonly record struct that configures transaction behavior. Construct using named parameters:

```csharp
new TransactionOptions(ReadOnly: true)
new TransactionOptions(ReadOnly: false, TimeoutMillis: 30000)
```

Parameters:

- **ReadOnly** (bool) - Mark transaction as read-only (default: false). Read-only transactions provide a snapshot view of data at a certain point in time. They are lock-free and perform better than normal transactions, but do not permit data modifications.
- **TimeoutMillis** (long) - Transaction timeout in milliseconds (default: 0). A value of 0 means use the default timeout configured via ignite.transaction.timeout configuration property.

The timeout controls how long the transaction can remain active before automatic rollback.

### Best Practices

**Always dispose transactions** using using statements or explicit disposal. Undisposed transactions hold cluster resources.

**Commit explicitly** before disposal. Implicit rollback on disposal can hide logic errors.

**Keep transactions short** to reduce lock contention and improve throughput. Long-running transactions impact cluster performance.

**Handle exceptions properly** to ensure rollback occurs when operations fail. Use try-catch blocks around transaction logic.

**Use RunInTransactionAsync** for simple cases where automatic lifecycle management is sufficient. This reduces boilerplate code and ensures proper cleanup.

**Pass transaction to all operations** that should be coordinated. Mixing null and transaction parameters within related operations breaks atomicity.
