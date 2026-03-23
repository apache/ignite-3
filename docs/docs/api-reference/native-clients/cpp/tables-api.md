---
title: Tables API
id: tables-api
sidebar_position: 2
---

# Tables API

The Tables API provides CRUD operations on table data. It supports both binary tuple operations and typed C++ object operations through record views and key-value views.

## Key Concepts

### Table Access

Tables are accessed through the `tables` interface obtained from the client. Each table provides multiple view types for different access patterns.

### View Types

Apache Ignite provides two view categories:

**Record Views** operate on complete row data. A single tuple or object contains all columns including the primary key. Use record views when working with complete records.

**Key-Value Views** separate primary key columns from value columns. Operations use distinct key and value tuples or objects. Use key-value views when the domain model separates keys from data.

### Binary vs Typed Operations

**Binary Views** use `ignite_tuple` for dynamic column access without schema knowledge. Column values are accessed by name or index at runtime.

**Typed Views** use C++ structs or classes with compile-time type safety. Type conversion happens through `convert_to_tuple` and `convert_from_tuple` template specializations.

### Transaction Support

All view operations accept an optional `transaction*` parameter. Pass `nullptr` for implicit transactions. Pass a transaction object for explicit transaction control.

## Getting Tables

### Retrieve a Single Table

Get a table by name:

```cpp
using namespace ignite;

auto tables = client.get_tables();
auto table = tables.get_table("my_table");

if (table.has_value()) {
    // Use table
}
```

Get a table with qualified name:

```cpp
auto table = tables.get_table("my_schema.my_table");
```

Use async retrieval:

```cpp
tables.get_table_async("my_table", [](ignite_result<std::optional<table>> result) {
    if (!result.has_error()) {
        auto table = std::move(result).value();
        if (table.has_value()) {
            // Use table
        }
    }
});
```

### List All Tables

Retrieve all tables:

```cpp
auto all_tables = tables.get_tables();
for (const auto& table : all_tables) {
    std::cout << table.get_name() << std::endl;
}
```

Use async retrieval:

```cpp
tables.get_tables_async([](ignite_result<std::vector<table>> result) {
    if (!result.has_error()) {
        auto all_tables = std::move(result).value();
        // Process tables
    }
});
```

## Record Views

### Binary Record View

Work with tuples directly:

```cpp
auto table = tables.get_table("accounts").value();
auto view = table.get_record_binary_view();

// Insert a record
ignite_tuple record{
    {"id", 42},
    {"name", "John Doe"},
    {"balance", 1000.0}
};

view.upsert(nullptr, record);

// Retrieve a record
ignite_tuple key{{"id", 42}};
auto result = view.get(nullptr, key);

if (result.has_value()) {
    auto balance = result->get<double>("balance");
}
```

### Typed Record View

Work with C++ types:

```cpp
struct account {
    int64_t id;
    std::string name;
    double balance;
};

// Define type conversion (typically in a header)
namespace ignite {
    template<>
    struct convert_to_tuple<account> {
        static ignite_tuple to_tuple(const account& obj) {
            return ignite_tuple{
                {"id", obj.id},
                {"name", obj.name},
                {"balance", obj.balance}
            };
        }
    };

    template<>
    struct convert_from_tuple<account> {
        static account from_tuple(const ignite_tuple& tuple) {
            return account{
                tuple.get<int64_t>("id"),
                tuple.get<std::string>("name"),
                tuple.get<double>("balance")
            };
        }
    };
}

// Use typed view
auto table = tables.get_table("accounts").value();
auto view = table.get_record_view<account>();

account new_account{42, "John Doe", 1000.0};
view.upsert(nullptr, new_account);

account key{42};
auto result = view.get(nullptr, key);
```

### Record View Operations

**Basic Operations:**

```cpp
// Insert (fails if exists)
bool inserted = view.insert(nullptr, record);

// Upsert (insert or replace)
view.upsert(nullptr, record);

// Replace (fails if not exists)
bool replaced = view.replace(nullptr, record);

// Replace with old value check
bool replaced = view.replace(nullptr, old_record, new_record);

// Get and replace atomically
auto old_record = view.get_and_replace(nullptr, new_record);

// Get and upsert atomically
auto old_record = view.get_and_upsert(nullptr, record);
```

**Delete Operations:**

```cpp
// Remove by key
bool removed = view.remove(nullptr, key);

// Remove exact match
bool removed = view.remove_exact(nullptr, full_record);

// Remove and return old value
auto old_record = view.get_and_remove(nullptr, key);
```

**Batch Operations:**

```cpp
std::vector<ignite_tuple> records = {record1, record2, record3};

// Get multiple records
auto results = view.get_all(nullptr, keys);

// Insert multiple (returns skipped records)
auto skipped = view.insert_all(nullptr, records);

// Upsert multiple
view.upsert_all(nullptr, records);

// Remove multiple (returns non-existent keys)
auto non_existent = view.remove_all(nullptr, keys);

// Remove exact multiple
auto not_matched = view.remove_all_exact(nullptr, records);
```

## Key-Value Views

### Binary Key-Value View

Separate keys from values:

```cpp
auto table = tables.get_table("accounts").value();
auto view = table.get_key_value_binary_view();

// Put a key-value pair
ignite_tuple key{{"id", 42}};
ignite_tuple value{
    {"name", "John Doe"},
    {"balance", 1000.0}
};

view.put(nullptr, key, value);

// Get value by key
auto result = view.get(nullptr, key);

// Check key existence
bool exists = view.contains(nullptr, key);
```

### Typed Key-Value View

Use separate C++ types for keys and values:

```cpp
struct account_key {
    int64_t id;
};

struct account_data {
    std::string name;
    double balance;
};

// Define conversions for both types
// (Similar to record view example)

auto table = tables.get_table("accounts").value();
auto view = table.get_key_value_view<account_key, account_data>();

account_key key{42};
account_data data{"John Doe", 1000.0};

view.put(nullptr, key, data);
auto result = view.get(nullptr, key);
```

### Key-Value View Operations

**Basic Operations:**

```cpp
// Put (insert or replace)
view.put(nullptr, key, value);

// Put if absent
bool inserted = view.put_if_absent(nullptr, key, value);

// Get and put atomically
auto old_value = view.get_and_put(nullptr, key, value);

// Replace
bool replaced = view.replace(nullptr, key, value);

// Replace with old value check
bool replaced = view.replace(nullptr, key, old_value, new_value);

// Get and replace atomically
auto old_value = view.get_and_replace(nullptr, key, value);

// Check existence
bool exists = view.contains(nullptr, key);
```

**Delete Operations:**

```cpp
// Remove by key
bool removed = view.remove(nullptr, key);

// Remove with value check
bool removed = view.remove(nullptr, key, expected_value);

// Remove and return value
auto old_value = view.get_and_remove(nullptr, key);
```

**Batch Operations:**

```cpp
std::vector<std::pair<K, V>> pairs = {{key1, val1}, {key2, val2}};

// Get multiple values
auto values = view.get_all(nullptr, keys);

// Put multiple pairs
view.put_all(nullptr, pairs);

// Remove multiple keys
auto non_existent = view.remove_all(nullptr, keys);

// Remove multiple pairs with value checks
auto not_matched = view.remove_all(nullptr, pairs);
```

## Ignite Tuple

### Creating Tuples

Use initializer lists:

```cpp
ignite_tuple tuple{
    {"id", 42},
    {"name", "John"},
    {"active", true}
};
```

Construct with capacity hint:

```cpp
ignite_tuple tuple(10); // Reserve space for 10 columns
tuple.set("id", 42);
tuple.set("name", "John");
```

### Accessing Values

Access by name:

```cpp
auto id = tuple.get<int64_t>("id");
auto name = tuple.get<std::string>("name");

// Or use primitive wrapper
auto value = tuple.get("id"); // Returns primitive
```

Access by index:

```cpp
auto id = tuple.get<int64_t>(0);
auto name = tuple.get<std::string>(1);
```

### Column Metadata

Query column information:

```cpp
int32_t count = tuple.column_count();
std::string name = tuple.column_name(0);
int32_t index = tuple.column_ordinal("id");
```

### Column Names

Column names are case-insensitive and normalized to uppercase unless quoted:

```cpp
tuple.set("ID", 42);
tuple.set("id", 42);  // Same as above
tuple.set("Id", 42);  // Same as above

// Use quotes for case-sensitive names
tuple.set("\"Id\"", 42);  // Different from above
```

## Asynchronous Operations

All operations have async variants with `_async` suffix:

```cpp
view.get_async(nullptr, key, [](ignite_result<std::optional<ignite_tuple>> result) {
    if (!result.has_error()) {
        auto tuple = std::move(result).value();
        if (tuple.has_value()) {
            // Use tuple
        }
    }
});

view.upsert_async(nullptr, record, [](ignite_result<void> result) {
    if (!result.has_error()) {
        // Operation succeeded
    }
});
```

## Transaction Integration

Use explicit transactions:

```cpp
auto tx = client.get_transactions().begin();

try {
    view.upsert(&tx, record1);
    view.upsert(&tx, record2);
    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)
- [Client API](./client-api)
- [SQL API](./sql-api)
- [Transactions API](./transactions-api)
