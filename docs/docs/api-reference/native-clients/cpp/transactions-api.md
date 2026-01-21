---
title: Transactions API
id: transactions-api
sidebar_position: 4
---

# Transactions API

The Transactions API provides explicit transaction control for operations across tables and SQL statements. Transactions ensure atomic, consistent, isolated, and durable data modifications.

## Key Concepts

### Transaction Lifecycle

Transactions begin through the transactions factory. They remain active until committed or rolled back. Operations within a transaction see uncommitted changes from that transaction. Other transactions see data as it existed before the transaction started.

### Explicit vs Implicit Transactions

**Explicit transactions** require manual commit or rollback. Pass the transaction pointer to operations. This provides control over transaction boundaries.

**Implicit transactions** commit automatically after each operation. Pass `nullptr` to operations for implicit transactions.

### Transaction Isolation

Transactions use snapshot isolation. Each transaction sees a consistent snapshot of data from transaction start time. Changes within a transaction are visible to that transaction but not to others until commit.

### Transaction Options

Configure transaction behavior through options:

- **timeout** - Maximum transaction duration
- **read_only** - Optimize for read-only workloads

## Basic Usage

### Beginning Transactions

Start a transaction with default options:

```cpp
using namespace ignite;

auto transactions = client.get_transactions();
auto tx = transactions.begin();
```

Start with options:

```cpp
transaction_options opts;
opts.set_timeout_millis(30000);  // 30 seconds
opts.set_read_only(false);

auto tx = transactions.begin(opts);
```

Use async begin:

```cpp
transactions.begin_async([](ignite_result<transaction> result) {
    if (!result.has_error()) {
        auto tx = std::move(result).value();
        // Use transaction
    }
});
```

### Committing Transactions

Commit to persist changes:

```cpp
auto tx = transactions.begin();

// Perform operations
tx.commit();
```

Use async commit:

```cpp
tx.commit_async([](ignite_result<void> result) {
    if (!result.has_error()) {
        std::cout << "Transaction committed" << std::endl;
    }
});
```

### Rolling Back Transactions

Rollback to discard changes:

```cpp
auto tx = transactions.begin();

try {
    // Perform operations
    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

Use async rollback:

```cpp
tx.rollback_async([](ignite_result<void> result) {
    if (!result.has_error()) {
        std::cout << "Transaction rolled back" << std::endl;
    }
});
```

## Table Operations

### Using Transactions with Record Views

Pass transaction pointer to record view operations:

```cpp
auto tx = client.get_transactions().begin();

auto table = client.get_tables().get_table("accounts").value();
auto view = table.get_record_binary_view();

try {
    ignite_tuple record{
        {"id", 42},
        {"name", "John Doe"},
        {"balance", 1000.0}
    };

    view.upsert(&tx, record);

    auto retrieved = view.get(&tx, ignite_tuple{{"id", 42}});

    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

### Using Transactions with Key-Value Views

Pass transaction to key-value operations:

```cpp
auto tx = client.get_transactions().begin();

auto table = client.get_tables().get_table("accounts").value();
auto view = table.get_key_value_binary_view();

try {
    ignite_tuple key{{"id", 42}};
    ignite_tuple value{{"name", "John Doe"}, {"balance", 1000.0}};

    view.put(&tx, key, value);
    auto retrieved = view.get(&tx, key);

    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

### Batch Operations

Batch operations execute within a transaction:

```cpp
auto tx = client.get_transactions().begin();

std::vector<ignite_tuple> records{
    {{"id", 1}, {"name", "Alice"}, {"balance", 1000.0}},
    {{"id", 2}, {"name", "Bob"}, {"balance", 2000.0}},
    {{"id", 3}, {"name", "Charlie"}, {"balance", 3000.0}}
};

try {
    view.upsert_all(&tx, records);
    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

## SQL Operations

### Executing SQL in Transactions

Pass transaction to SQL operations:

```cpp
auto tx = client.get_transactions().begin();

try {
    auto sql = client.get_sql();

    sql.execute(&tx, nullptr,
        sql_statement("INSERT INTO accounts VALUES (?, ?, ?)"),
        {42, std::string("John Doe"), 1000.0});

    sql.execute(&tx, nullptr,
        sql_statement("UPDATE accounts SET balance = ? WHERE id = ?"),
        {1500.0, 42});

    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

### Cross-Table Transactions

Execute operations across multiple tables:

```cpp
auto tx = client.get_transactions().begin();

try {
    auto sql = client.get_sql();

    // Debit from one account
    sql.execute(&tx, nullptr,
        sql_statement("UPDATE accounts SET balance = balance - ? WHERE id = ?"),
        {100.0, 1});

    // Credit to another account
    sql.execute(&tx, nullptr,
        sql_statement("UPDATE accounts SET balance = balance + ? WHERE id = ?"),
        {100.0, 2});

    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

## Transaction Options

### Configuring Timeout

Set maximum transaction duration:

```cpp
transaction_options opts;
opts.set_timeout_millis(60000);  // 60 seconds

auto tx = transactions.begin(opts);
```

Timeout of 0 means no timeout:

```cpp
opts.set_timeout_millis(0);  // No timeout
```

### Read-Only Transactions

Optimize for read operations:

```cpp
transaction_options opts;
opts.set_read_only(true);

auto tx = transactions.begin(opts);

// Only read operations allowed
auto result = view.get(&tx, key);

tx.commit();  // Lightweight commit for read-only
```

Read-only transactions provide better performance by avoiding write locks and conflict detection.

### Chaining Options

Use fluent API to chain option setters:

```cpp
transaction_options opts;
opts.set_timeout_millis(30000)
    .set_read_only(false);

auto tx = transactions.begin(opts);
```

## Transaction Visibility

### Uncommitted Changes

Changes are visible within the transaction:

```cpp
auto tx = transactions.begin();

view.upsert(&tx, record);

// This sees the upserted record
auto result = view.get(&tx, key);

// Other transactions do not see it yet
```

### Isolation from Other Transactions

Each transaction sees a consistent snapshot:

```cpp
// Transaction 1
auto tx1 = transactions.begin();
view.upsert(&tx1, record1);

// Transaction 2 (concurrent)
auto tx2 = transactions.begin();
auto result = view.get(&tx2, key);  // Does not see record1

tx1.commit();

// Transaction 2 still does not see record1 (snapshot isolation)
auto result2 = view.get(&tx2, key);  // Still does not see record1
```

## Asynchronous Transactions

### Async Begin

Start transactions asynchronously:

```cpp
transactions.begin_async([&](ignite_result<transaction> result) {
    if (!result.has_error()) {
        auto tx = std::move(result).value();

        view.upsert_async(&tx, record, [&](ignite_result<void> upsert_result) {
            if (!upsert_result.has_error()) {
                tx.commit_async([](ignite_result<void> commit_result) {
                    // Transaction committed
                });
            }
        });
    }
});
```

### Async Begin with Options

Pass options to async begin:

```cpp
transaction_options opts;
opts.set_timeout_millis(30000);

transactions.begin_async(opts, [](ignite_result<transaction> result) {
    // Use transaction
});
```

## Error Handling

### Handling Commit Failures

Commit failures indicate conflicts or constraints:

```cpp
auto tx = transactions.begin();

try {
    view.upsert(&tx, record);
    tx.commit();
} catch (const ignite_error& e) {
    std::cerr << "Commit failed: " << e.what_str() << std::endl;
    // Transaction already rolled back on commit failure
    throw;
}
```

### Handling Operation Failures

Roll back on operation errors:

```cpp
auto tx = transactions.begin();

try {
    view.upsert(&tx, record1);
    view.upsert(&tx, record2);  // May throw
    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    std::cerr << "Operation failed: " << e.what_str() << std::endl;
    throw;
}
```

### Timeout Handling

Transactions time out after configured duration:

```cpp
transaction_options opts;
opts.set_timeout_millis(1000);  // 1 second

auto tx = transactions.begin(opts);

try {
    // Long-running operation
    std::this_thread::sleep_for(std::chrono::seconds(2));
    tx.commit();  // Will fail due to timeout
} catch (const ignite_error& e) {
    // Handle timeout error
}
```

## Best Practices

### Keep Transactions Short

Minimize transaction duration to reduce conflicts:

```cpp
// Good: Short transaction
auto tx = transactions.begin();
view.upsert(&tx, record);
tx.commit();

// Avoid: Long-running transaction
auto tx2 = transactions.begin();
perform_expensive_calculation();  // Do outside transaction
view.upsert(&tx2, result);
tx2.commit();
```

### Use Read-Only for Queries

Enable read-only optimization:

```cpp
transaction_options opts;
opts.set_read_only(true);

auto tx = transactions.begin(opts);
auto results = view.get_all(&tx, keys);
tx.commit();
```

### Handle Errors Properly

Always rollback on errors:

```cpp
auto tx = transactions.begin();
bool committed = false;

try {
    // Operations
    tx.commit();
    committed = true;
} catch (const ignite_error& e) {
    if (!committed) {
        tx.rollback();
    }
    throw;
}
```

### Use RAII for Automatic Cleanup

Wrap transactions in RAII helpers:

```cpp
class transaction_guard {
    transaction& tx_;
    bool committed_ = false;

public:
    explicit transaction_guard(transaction& tx) : tx_(tx) {}

    ~transaction_guard() {
        if (!committed_) {
            try {
                tx_.rollback();
            } catch (...) {
                // Log error
            }
        }
    }

    void commit() {
        tx_.commit();
        committed_ = true;
    }
};

// Usage
auto tx = transactions.begin();
transaction_guard guard(tx);

view.upsert(&tx, record);
guard.commit();  // Automatic rollback if not committed
```

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)
- [Transactions Concept](../../../develop/work-with-data/transactions)
- [Client API](./client-api)
- [Tables API](./tables-api)
- [SQL API](./sql-api)
