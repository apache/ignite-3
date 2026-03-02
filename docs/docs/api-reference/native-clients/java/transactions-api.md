---
title: Transactions API
id: transactions-api
sidebar_position: 6
---

# Transactions API

The Transactions API provides ACID guarantees for operations spanning multiple tables or operations. Applications use transactions to ensure data consistency when executing related updates that must succeed or fail as a unit.

## Key Concepts

Read-write transactions execute with SERIALIZABLE isolation. Operations within a transaction see a consistent view of data as it existed when the transaction started. Changes remain invisible to other transactions until commit.

The API supports explicit transaction management and closure-based implicit transactions. Closure-based transactions automatically commit on normal completion and rollback on exceptions, reducing boilerplate code.

Transactions have configurable timeouts. Read-write transactions default to 30 seconds. Read-only transactions optimize for read operations by eliminating write overhead.

## Implicit Transactions

Use runInTransaction for automatic lifecycle management:

```java
ignite.transactions().runInTransaction(tx -> {
    RecordView<Tuple> view = ignite.tables().table("accounts").recordView();

    Tuple key = Tuple.create().set("id", 1);
    Tuple record = view.get(tx, key);

    int balance = record.intValue("balance");
    record.set("balance", balance + 100);

    view.put(tx, record);
});
```

The transaction commits automatically when the closure completes normally. Exceptions trigger automatic rollback.

## Return Values from Transactions

Return values from transaction closures:

```java
int newBalance = ignite.transactions().runInTransaction(tx -> {
    RecordView<Tuple> view = ignite.tables().table("accounts").recordView();

    Tuple key = Tuple.create().set("id", 1);
    Tuple record = view.get(tx, key);

    int balance = record.intValue("balance") + 100;
    record.set("balance", balance);
    view.put(tx, record);

    return balance;
});

System.out.println("New balance: " + newBalance);
```

## Explicit Transactions

Manage transaction lifecycle explicitly:

```java
Transaction tx = ignite.transactions().begin();
try {
    RecordView<Tuple> view = ignite.tables().table("accounts").recordView();

    Tuple key = Tuple.create().set("id", 1);
    Tuple record = view.get(tx, key);

    record.set("balance", record.intValue("balance") + 100);
    view.put(tx, record);

    tx.commit();
} catch (Exception e) {
    tx.rollback();
    throw e;
}
```

Explicit management provides control over commit timing and error handling.

## Read-Only Transactions

Optimize read operations with read-only transactions:

```java
TransactionOptions options = new TransactionOptions().readOnly(true);

ignite.transactions().runInTransaction(options, tx -> {
    RecordView<Tuple> view = ignite.tables().table("users").recordView();

    Tuple key = Tuple.create().set("id", 1);
    Tuple record = view.get(tx, key);

    System.out.println("User: " + record.stringValue("name"));
});
```

Read-only transactions eliminate write coordination overhead, improving performance for read operations.

## Transaction Timeouts

Configure transaction timeouts:

```java
TransactionOptions options = new TransactionOptions().timeoutMillis(60000);

Transaction tx = ignite.transactions().begin(options);
```

Transactions that exceed the timeout automatically rollback. This prevents long-running transactions from blocking other operations.

## Multi-Table Transactions

Execute operations across multiple tables:

```java
ignite.transactions().runInTransaction(tx -> {
    RecordView<Tuple> accounts = ignite.tables().table("accounts").recordView();
    RecordView<Tuple> history = ignite.tables().table("history").recordView();

    Tuple accountKey = Tuple.create().set("id", 1);
    Tuple account = accounts.get(tx, accountKey);

    int balance = account.intValue("balance");
    account.set("balance", balance - 50);
    accounts.put(tx, account);

    Tuple historyRecord = Tuple.create()
        .set("account_id", 1)
        .set("amount", -50)
        .set("timestamp", LocalDateTime.now());
    history.put(tx, historyRecord);
});
```

Both operations commit or rollback together.

## Asynchronous Transactions

Create transactions asynchronously:

```java
ignite.transactions().beginAsync().thenCompose(tx ->
    ignite.tables().tableAsync("accounts")
        .thenCompose(table -> {
            RecordView<Tuple> view = table.recordView();
            Tuple key = Tuple.create().set("id", 1);
            return view.getAsync(tx, key)
                .thenCompose(record -> {
                    record.set("balance", record.intValue("balance") + 100);
                    return view.putAsync(tx, record);
                })
                .thenCompose(v -> tx.commitAsync());
        })
).exceptionally(ex -> {
    return null;
});
```

Asynchronous transactions enable non-blocking transaction processing.

## Async Transaction Closures

Use async closures for non-blocking transaction execution:

```java
CompletableFuture<Integer> resultFuture =
    ignite.transactions().runInTransactionAsync(tx ->
        ignite.tables().tableAsync("accounts")
            .thenCompose(table -> {
                RecordView<Tuple> view = table.recordView();
                Tuple key = Tuple.create().set("id", 1);
                return view.getAsync(tx, key);
            })
            .thenApply(record -> {
                int balance = record.intValue("balance") + 100;
                record.set("balance", balance);
                return balance;
            })
    );

resultFuture.thenAccept(balance ->
    System.out.println("New balance: " + balance)
);
```

## SQL and Transactions

Execute SQL within transactions:

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

SQL statements and table operations within the same transaction see consistent data.

## Transaction Status

Check transaction properties:

```java
Transaction tx = ignite.transactions().begin();

boolean readOnly = tx.isReadOnly();
System.out.println("Read-only: " + readOnly);

// Use transaction
tx.commit();
```

## Idempotent Operations

Commit and rollback operations are idempotent:

```java
Transaction tx = ignite.transactions().begin();
try {
    // Operations
    tx.commit();
} finally {
    tx.rollback(); // Safe even if already committed
}
```

Calling commit or rollback on completed transactions has no effect.

## Error Handling

Handle transaction-specific exceptions:

```java
try {
    ignite.transactions().runInTransaction(tx -> {
        // Operations
    });
} catch (RetriableTransactionException e) {
    // Transaction can be retried
    System.err.println("Retry transaction: " + e.getMessage());
} catch (TransactionException e) {
    // Transaction error
    System.err.println("Transaction failed: " + e.getMessage());
}
```

RetriableTransactionException indicates conflicts that may succeed on retry. Other TransactionException subtypes indicate non-retriable errors.

## Exception Types

Common transaction exceptions:

```java
try {
    ignite.transactions().runInTransaction(tx -> {
        // Operations
    });
} catch (IncompatibleSchemaException e) {
    // Schema changed during transaction
    System.err.println("Schema incompatible: " + e.getMessage());
} catch (MismatchingTransactionOutcomeException e) {
    // Inconsistent commit/rollback across replicas
    System.err.println("Outcome mismatch: " + e.getMessage());
}
```

IncompatibleSchemaException occurs when table schemas change during transaction execution. MismatchingTransactionOutcomeException indicates inconsistent transaction outcomes.

## Reference

- Transaction manager: `org.apache.ignite.tx.IgniteTransactions`
- Transaction handle: `org.apache.ignite.tx.Transaction`
- Configuration: `org.apache.ignite.tx.TransactionOptions`
- Exceptions: `org.apache.ignite.tx.TransactionException`, `org.apache.ignite.tx.RetriableTransactionException`, `org.apache.ignite.tx.IncompatibleSchemaException`, `org.apache.ignite.tx.MismatchingTransactionOutcomeException`

### IgniteTransactions Methods

- `Transaction begin()` - Start transaction with defaults
- `Transaction begin(TransactionOptions)` - Start with configuration
- `CompletableFuture<Transaction> beginAsync()` - Async start
- `CompletableFuture<Transaction> beginAsync(TransactionOptions)` - Async start with configuration
- `void runInTransaction(Consumer<Transaction>)` - Execute in transaction
- `<T> T runInTransaction(Function<Transaction, T>)` - Execute with return value
- `void runInTransaction(TransactionOptions, Consumer<Transaction>)` - Execute with options
- `<T> T runInTransaction(TransactionOptions, Function<Transaction, T>)` - Execute with options and return
- `<T> CompletableFuture<T> runInTransactionAsync(Function<Transaction, CompletableFuture<T>>)` - Async execution

### Transaction Methods

- `void commit()` - Commit transaction
- `CompletableFuture<Void> commitAsync()` - Async commit
- `void rollback()` - Rollback transaction
- `CompletableFuture<Void> rollbackAsync()` - Async rollback
- `boolean isReadOnly()` - Check read-only status

### TransactionOptions Configuration

- `readOnly(boolean)` - Set read-only mode
- `timeoutMillis(long)` - Set transaction timeout in milliseconds

### Transaction Isolation

Read-write transactions use SERIALIZABLE isolation. Each transaction acquires locks during the first read or write access and holds the lock until the transaction is committed or rolled back. Changes remain invisible to concurrent transactions until commit. Read-only transactions provide a snapshot view of data without acquiring locks.

### Transaction Best Practices

- Use implicit transactions (runInTransaction) for automatic lifecycle management
- Configure appropriate timeouts to prevent blocking operations
- Use read-only transactions for operations that only read data
- Handle RetriableTransactionException by retrying operations
- Keep transactions short to minimize lock contention
- Avoid user interaction or slow operations within transactions
