---
title: SQL API
id: sql-api
sidebar_position: 3
---

# SQL API

The SQL API executes SQL statements and scripts against Apache Ignite clusters. It supports parameterized queries, pagination, transaction integration, and query cancellation.

## Key Concepts

### Statement Execution

Single-statement queries return result sets. Multi-statement scripts execute multiple statements without returning results. Use statements for queries and DML operations. Use scripts for DDL operations and batch updates.

### Parameterized Queries

Parameters prevent SQL injection and enable query plan reuse. Use question marks (`?`) as placeholders. Pass parameter values as a vector of `primitive` values in execution order.

### Result Sets

Query results return as `result_set` objects containing rows and metadata. Result sets support pagination for large queries. Each page contains a batch of rows. Fetch additional pages explicitly.

### Transaction Integration

Pass a transaction pointer to execute statements within explicit transactions. Pass `nullptr` for implicit transactions that commit immediately.

### Query Cancellation

Pass a `cancellation_token` to cancel long-running queries. Create tokens before execution. Trigger cancellation from another thread when needed.

## Basic Execution

### Simple Query

Execute a SELECT statement:

```cpp
using namespace ignite;

auto sql = client.get_sql();
auto result = sql.execute(nullptr, nullptr, sql_statement("SELECT * FROM accounts"), {});

if (result.has_rowset()) {
    for (const auto& row : result.current_page()) {
        auto id = row.get<int64_t>("id");
        auto name = row.get<std::string>("name");
    }
}
```

### Parameterized Query

Use parameters for safe value binding:

```cpp
sql_statement stmt("SELECT * FROM accounts WHERE balance > ? AND active = ?");
std::vector<primitive> params{1000.0, true};

auto result = sql.execute(nullptr, nullptr, stmt, params);
```

### DML Operations

Execute INSERT, UPDATE, DELETE:

```cpp
// Insert
sql_statement insert("INSERT INTO accounts (id, name, balance) VALUES (?, ?, ?)");
std::vector<primitive> values{42, std::string("John Doe"), 1000.0};

auto result = sql.execute(nullptr, nullptr, insert, values);
std::cout << "Rows inserted: " << result.affected_rows() << std::endl;

// Update
sql_statement update("UPDATE accounts SET balance = ? WHERE id = ?");
auto result2 = sql.execute(nullptr, nullptr, update, {1500.0, 42});
std::cout << "Rows updated: " << result2.affected_rows() << std::endl;

// Delete
sql_statement del("DELETE FROM accounts WHERE id = ?");
auto result3 = sql.execute(nullptr, nullptr, del, {42});
std::cout << "Rows deleted: " << result3.affected_rows() << std::endl;
```

### DDL Operations

Execute schema changes:

```cpp
sql_statement ddl("CREATE TABLE new_table (id INT PRIMARY KEY, data VARCHAR)");
auto result = sql.execute(nullptr, nullptr, ddl, {});

// Check if DDL was applied
if (result.was_applied()) {
    std::cout << "Table created" << std::endl;
}
```

## SQL Statements

### Statement Configuration

Configure statement properties:

```cpp
sql_statement stmt;
stmt.query("SELECT * FROM large_table");
stmt.schema("my_schema");
stmt.page_size(100);  // Rows per page
stmt.timeout(std::chrono::seconds(30));
stmt.timezone_id("America/New_York");
```

### Statement Properties

**query()** - SQL text to execute (required)

**schema()** - Default schema name (default: "PUBLIC")

**page_size()** - Rows per result page (default: 1024)

**timeout()** - Query timeout in milliseconds (default: 0 for no timeout)

**timezone_id()** - Timezone for time functions

**properties()** - Additional statement properties as key-value map

### Builder Pattern

Chain configuration calls:

```cpp
sql_statement stmt;
stmt.query("SELECT * FROM accounts")
    .schema("PUBLIC")
    .page_size(500)
    .timeout(std::chrono::seconds(10));
```

## Result Sets

### Accessing Rows

Iterate current page:

```cpp
auto result = sql.execute(nullptr, nullptr, stmt, {});

for (const auto& row : result.current_page()) {
    // Access columns by name
    auto id = row.get<int64_t>("id");
    auto name = row.get<std::string>("name");

    // Or by index
    auto id2 = row.get<int64_t>(0);
    auto name2 = row.get<std::string>(1);
}
```

### Pagination

Handle large result sets:

```cpp
auto result = sql.execute(nullptr, nullptr, stmt, {});

// Process first page
for (const auto& row : result.current_page()) {
    // Process row
}

// Fetch and process remaining pages
while (result.has_more_pages()) {
    result.fetch_next_page();
    for (const auto& row : result.current_page()) {
        // Process row
    }
}
```

Use async pagination:

```cpp
void process_page(result_set& result) {
    for (const auto& row : result.current_page()) {
        // Process row
    }

    if (result.has_more_pages()) {
        result.fetch_next_page_async([&](ignite_result<void> res) {
            if (!res.has_error()) {
                process_page(result);
            }
        });
    }
}

auto result = sql.execute(nullptr, nullptr, stmt, {});
process_page(result);
```

### Metadata

Access result metadata:

```cpp
auto result = sql.execute(nullptr, nullptr, stmt, {});
const auto& metadata = result.metadata();

for (const auto& column : metadata.columns()) {
    std::cout << "Column: " << column.name() << std::endl;
    std::cout << "Type: " << static_cast<int>(column.type()) << std::endl;
    std::cout << "Nullable: " << column.nullable() << std::endl;

    if (column.precision() != -1) {
        std::cout << "Precision: " << column.precision() << std::endl;
    }
    if (column.scale() != -1) {
        std::cout << "Scale: " << column.scale() << std::endl;
    }
}
```

Find column index by name:

```cpp
int32_t col_index = metadata.index_of("balance");
```

### Checking Result Type

Determine if result contains rows or is a DML result:

```cpp
auto result = sql.execute(nullptr, nullptr, stmt, {});

if (result.has_rowset()) {
    // Query returned rows
    auto rows = result.current_page();
} else {
    // DML or DDL operation
    std::cout << "Affected rows: " << result.affected_rows() << std::endl;
}

// Check if conditional DDL was applied
if (result.was_applied()) {
    std::cout << "Statement applied successfully" << std::endl;
}
```

### Closing Result Sets

Close result sets explicitly to free resources:

```cpp
auto result = sql.execute(nullptr, nullptr, stmt, {});
// Use result
result.close();
```

Use async close:

```cpp
result.close_async([](ignite_result<void> res) {
    if (!res.has_error()) {
        // Result closed
    }
});
```

## Script Execution

### Multi-Statement Scripts

Execute multiple statements:

```cpp
sql_statement script(R"(
    CREATE TABLE temp1 (id INT PRIMARY KEY, data VARCHAR);
    CREATE TABLE temp2 (id INT PRIMARY KEY, data VARCHAR);
    INSERT INTO temp1 VALUES (1, 'test');
)");

sql.execute_script(nullptr, script, {});
```

Use async execution:

```cpp
sql.execute_script_async(nullptr, script, {}, [](ignite_result<void> result) {
    if (!result.has_error()) {
        std::cout << "Script executed successfully" << std::endl;
    }
});
```

Scripts do not return result sets. Use individual statements for queries. Scripts do not support transactions.

## Transaction Integration

### Explicit Transactions

Execute statements in a transaction:

```cpp
auto tx = client.get_transactions().begin();

try {
    sql_statement update1("UPDATE accounts SET balance = balance - ? WHERE id = ?");
    sql.execute(&tx, nullptr, update1, {100.0, 1});

    sql_statement update2("UPDATE accounts SET balance = balance + ? WHERE id = ?");
    sql.execute(&tx, nullptr, update2, {100.0, 2});

    tx.commit();
} catch (const ignite_error& e) {
    tx.rollback();
    throw;
}
```

### Implicit Transactions

Pass `nullptr` for auto-commit:

```cpp
// Each statement commits immediately
sql.execute(nullptr, nullptr, stmt, params);
```

## Query Cancellation

### Creating Cancellation Tokens

Create a token before execution:

```cpp
cancel_handle handle;
cancellation_token token(&handle);
```

### Cancelling Queries

Cancel from another thread:

```cpp
// Thread 1: Execute long query
auto result = sql.execute(nullptr, &token,
    sql_statement("SELECT * FROM huge_table"), {});

// Thread 2: Cancel query
handle.cancel();
```

Use with async execution:

```cpp
cancel_handle handle;
cancellation_token token(&handle);

sql.execute_async(nullptr, &token, stmt, {},
    [](ignite_result<result_set> result) {
        if (result.has_error()) {
            // May be cancellation error
        } else {
            // Process result
        }
    });

// Cancel if needed
handle.cancel();
```

## Asynchronous Execution

Execute statements without blocking:

```cpp
sql.execute_async(nullptr, nullptr, stmt, params,
    [](ignite_result<result_set> result) {
        if (!result.has_error()) {
            auto rs = std::move(result).value();
            for (const auto& row : rs.current_page()) {
                // Process row
            }
        }
    });
```

Execute scripts asynchronously:

```cpp
sql.execute_script_async(nullptr, script, {},
    [](ignite_result<void> result) {
        if (!result.has_error()) {
            std::cout << "Script completed" << std::endl;
        }
    });
```

## Data Type Mapping

C++ types map to SQL types:

| C++ Type | SQL Type |
|----------|----------|
| `bool` | BOOLEAN |
| `int8_t` | TINYINT |
| `int16_t` | SMALLINT |
| `int32_t` | INTEGER |
| `int64_t` | BIGINT |
| `float` | REAL |
| `double` | DOUBLE |
| `std::string` | VARCHAR |
| `std::vector<std::byte>` | VARBINARY |
| `uuid` | UUID |
| `ignite_date` | DATE |
| `ignite_time` | TIME |
| `ignite_timestamp` | TIMESTAMP |
| `ignite_date_time` | DATETIME |
| `big_decimal` | DECIMAL |
| `big_integer` | DECIMAL |

## Error Handling

Handle SQL errors:

```cpp
try {
    auto result = sql.execute(nullptr, nullptr, stmt, params);
} catch (const ignite_error& e) {
    std::cerr << "SQL error: " << e.what_str() << std::endl;
}
```

With async operations:

```cpp
sql.execute_async(nullptr, nullptr, stmt, params,
    [](ignite_result<result_set> result) {
        if (result.has_error()) {
            std::cerr << "Error: " << result.error().what_str() << std::endl;
        } else {
            // Process result
        }
    });
```

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)
- [SQL Reference](../../../sql/)
- [Client API](./client-api)
- [Tables API](./tables-api)
- [Transactions API](./transactions-api)
