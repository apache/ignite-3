---
title: Tables API
id: tables-api
sidebar_position: 2
---

# Tables API

The Tables API provides type-safe data access through record views and key-value views. This API supports both strongly-typed operations using C# classes and schema-free operations using tuples.

## Key Concepts

Tables in Ignite 3 are accessed through view interfaces that provide different access patterns. Record views work with complete rows as single objects, while key-value views separate keys and values into distinct objects. Both patterns support typed access (using C# classes) and untyped access (using IIgniteTuple).

### View Types

**Record View** treats each row as a single object containing all columns. Use this when your operations work with complete records.

**Key-Value View** separates key columns from value columns into distinct objects. Use this when you primarily access data by key or when your schema naturally divides into key and value sections.

**Binary View** provides untyped access using IIgniteTuple. Use this for dynamic schemas or when working with multiple table types through generic code.

### Transaction Support

All data operations accept an optional transaction parameter. Pass null for auto-commit mode or pass an ITransaction instance to include operations in a transaction scope.

## Usage Examples

### Getting Tables

```csharp
var tables = client.Tables;

// Get table by name
var table = await tables.GetTableAsync("customers");

// Get table by qualified name (schema.table)
var qualifiedName = QualifiedName.Of("public", "orders");
var table = await tables.GetTableAsync(qualifiedName);

// List all tables
var allTables = await tables.GetTablesAsync();
```

### Record View Operations

```csharp
// Define a POCO class matching table schema
public class Customer
{
    public long Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

var table = await tables.GetTableAsync("customers");
var view = table.GetRecordView<Customer>();

// Insert or replace
var customer = new Customer { Id = 1, Name = "Alice", Email = "alice@example.com" };
await view.UpsertAsync(null, customer);

// Get by key (only Id field used)
var key = new Customer { Id = 1 };
var result = await view.GetAsync(null, key);
if (result.HasValue)
{
    Console.WriteLine($"Found: {result.Value.Name}");
}

// Delete by key
await view.DeleteAsync(null, key);
```

### Key-Value View Operations

```csharp
public class OrderKey
{
    public long OrderId { get; set; }
}

public class OrderValue
{
    public long CustomerId { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Amount { get; set; }
}

var table = await tables.GetTableAsync("orders");
var view = table.GetKeyValueView<OrderKey, OrderValue>();

// Put key-value pair
var key = new OrderKey { OrderId = 100 };
var value = new OrderValue
{
    CustomerId = 1,
    OrderDate = DateTime.UtcNow,
    Amount = 49.99m
};
await view.PutAsync(null, key, value);

// Get value by key
var result = await view.GetAsync(null, key);
if (result.HasValue)
{
    Console.WriteLine($"Amount: {result.Value.Amount}");
}

// Remove by key
await view.RemoveAsync(null, key);
```

### Binary View with Tuples

```csharp
var table = await tables.GetTableAsync("products");
var view = table.RecordBinaryView;

// Create tuple
var tuple = new IgniteTuple
{
    ["id"] = 1L,
    ["name"] = "Widget",
    ["price"] = 29.99
};

// Upsert
await view.UpsertAsync(null, tuple);

// Get by key tuple
var keyTuple = new IgniteTuple { ["id"] = 1L };
var result = await view.GetAsync(null, keyTuple);
if (result.HasValue)
{
    Console.WriteLine($"Price: {result.Value["price"]}");
}
```

### Batch Operations

```csharp
var view = table.GetRecordView<Customer>();

// Upsert multiple records
var customers = new[]
{
    new Customer { Id = 1, Name = "Alice", Email = "alice@example.com" },
    new Customer { Id = 2, Name = "Bob", Email = "bob@example.com" },
    new Customer { Id = 3, Name = "Carol", Email = "carol@example.com" }
};
await view.UpsertAllAsync(null, customers);

// Get multiple records
var keys = new[]
{
    new Customer { Id = 1 },
    new Customer { Id = 2 },
    new Customer { Id = 3 }
};
var results = await view.GetAllAsync(null, keys);
```

### Conditional Operations

```csharp
var view = table.GetRecordView<Customer>();
var key = new Customer { Id = 1 };

// Insert only if not exists
var inserted = await view.InsertAsync(null, customer);

// Replace only if exists
var replaced = await view.ReplaceAsync(null, customer);

// Replace with compare-and-swap
var oldRecord = new Customer { Id = 1, Name = "Alice", Email = "alice@example.com" };
var newRecord = new Customer { Id = 1, Name = "Alice", Email = "alice@newdomain.com" };
var swapped = await view.ReplaceAsync(null, oldRecord, newRecord);
```

### Get-and-Modify Operations

```csharp
var view = table.GetRecordView<Customer>();

// Upsert and get old value
var result = await view.GetAndUpsertAsync(null, customer);
if (result.HasValue)
{
    Console.WriteLine($"Replaced: {result.Value.Name}");
}

// Delete and get old value
var key = new Customer { Id = 1 };
var deleted = await view.GetAndDeleteAsync(null, key);
if (deleted.HasValue)
{
    Console.WriteLine($"Deleted: {deleted.Value.Name}");
}
```

### LINQ Queries

```csharp
var view = table.GetRecordView<Customer>();

// Use LINQ with queryable and ToListAsync
var results = await view.AsQueryable()
    .Where(c => c.Name.StartsWith("A"))
    .OrderBy(c => c.Email)
    .ToListAsync();

foreach (var customer in results)
{
    Console.WriteLine($"{customer.Name}: {customer.Email}");
}

// Alternative: use ToResultSetAsync for streaming results
var resultSet = await view.AsQueryable()
    .Where(c => c.Name.StartsWith("A"))
    .OrderBy(c => c.Email)
    .ToResultSetAsync();

await foreach (var customer in resultSet)
{
    Console.WriteLine($"{customer.Name}: {customer.Email}");
}
```

## Reference

### ITables Interface

Methods for table discovery:

- **GetTableAsync(string name)** - Get table by name, returns null if not found
- **GetTableAsync(QualifiedName name)** - Get table by schema-qualified name
- **GetTablesAsync()** - Get all available tables in the cluster

### ITable Interface

Properties:

- **Name** - Table name without schema qualifier
- **QualifiedName** - Full schema-qualified name
- **RecordBinaryView** - Untyped record view using IIgniteTuple
- **KeyValueBinaryView** - Untyped key-value view using IIgniteTuple
- **PartitionManager** - Advanced partition management

Methods:

- **GetRecordView&lt;T&gt;()** - Create typed record view for type T
- **GetKeyValueView&lt;TK, TV&gt;()** - Create typed key-value view with key type TK and value type TV

### IRecordView&lt;T&gt; Interface

Read operations:

- **GetAsync(ITransaction?, T key)** - Get record by key, returns Option&lt;T&gt;
- **GetAllAsync(ITransaction?, IEnumerable&lt;T&gt; keys)** - Get multiple records
- **ContainsKeyAsync(ITransaction?, T key)** - Check if key exists

Write operations:

- **UpsertAsync(ITransaction?, T record)** - Insert or replace record
- **UpsertAllAsync(ITransaction?, IEnumerable&lt;T&gt; records)** - Insert or replace multiple records
- **InsertAsync(ITransaction?, T record)** - Insert only if not exists, returns bool
- **InsertAllAsync(ITransaction?, IEnumerable&lt;T&gt; records)** - Insert multiple, returns list of skipped keys
- **ReplaceAsync(ITransaction?, T record)** - Replace existing record, returns bool
- **ReplaceAsync(ITransaction?, T record, T newRecord)** - Conditional replace (compare-and-swap)

Delete operations:

- **DeleteAsync(ITransaction?, T key)** - Delete by key, returns bool
- **DeleteAllAsync(ITransaction?, IEnumerable&lt;T&gt; keys)** - Delete multiple, returns list of non-existent keys
- **DeleteExactAsync(ITransaction?, T record)** - Delete only if exact match on all columns
- **DeleteAllExactAsync(ITransaction?, IEnumerable&lt;T&gt; records)** - Delete exact matches

Get-and-modify operations:

- **GetAndUpsertAsync(ITransaction?, T record)** - Upsert and return old value
- **GetAndReplaceAsync(ITransaction?, T record)** - Replace and return old value
- **GetAndDeleteAsync(ITransaction?, T key)** - Delete and return old value

Query operations:

- **AsQueryable(ITransaction?, QueryableOptions?)** - Create LINQ queryable interface

### IKeyValueView&lt;TK, TV&gt; Interface

Read operations:

- **GetAsync(ITransaction?, TK key)** - Get value by key, returns Option&lt;TV&gt;
- **GetAllAsync(ITransaction?, IEnumerable&lt;TK&gt; keys)** - Get multiple values, returns Dictionary
- **ContainsAsync(ITransaction?, TK key)** - Check if key exists

Write operations:

- **PutAsync(ITransaction?, TK key, TV val)** - Put key-value pair
- **PutAllAsync(ITransaction?, IEnumerable&lt;KeyValuePair&lt;TK, TV&gt;&gt; pairs)** - Put multiple pairs
- **PutIfAbsentAsync(ITransaction?, TK key, TV val)** - Put only if key absent, returns bool

Replace operations:

- **ReplaceAsync(ITransaction?, TK key, TV val)** - Replace value for existing key, returns bool
- **ReplaceAsync(ITransaction?, TK key, TV oldVal, TV newVal)** - Conditional replace

Remove operations:

- **RemoveAsync(ITransaction?, TK key)** - Remove by key, returns bool
- **RemoveAsync(ITransaction?, TK key, TV val)** - Remove only if value matches
- **RemoveAllAsync(ITransaction?, IEnumerable&lt;TK&gt; keys)** - Remove multiple by key
- **RemoveAllAsync(ITransaction?, IEnumerable&lt;KeyValuePair&lt;TK, TV&gt;&gt; pairs)** - Remove by key-value pairs

Get-and-modify operations:

- **GetAndPutAsync(ITransaction?, TK key, TV val)** - Put and return old value
- **GetAndReplaceAsync(ITransaction?, TK key, TV val)** - Replace and return old value
- **GetAndRemoveAsync(ITransaction?, TK key)** - Remove and return value

Query operations:

- **AsQueryable(ITransaction?, QueryableOptions?)** - Create LINQ queryable interface

### IIgniteTuple Interface

Properties:

- **FieldCount** - Number of columns in the tuple
- **this[int ordinal]** - Get or set column value by ordinal position
- **this[string name]** - Get or set column value by name

Methods:

- **GetName(int ordinal)** - Get column name by ordinal
- **GetOrdinal(string name)** - Get column ordinal by name (returns -1 if not found)

Static methods:

- **GetHashCode(IIgniteTuple)** - Compute hash considering names and values
- **Equals(IIgniteTuple?, IIgniteTuple?)** - Compare tuples ignoring column order
- **ToString(IIgniteTuple)** - Generate string representation

### Option&lt;T&gt; Type

The Option type wraps nullable results:

- **HasValue** - True if value exists
- **Value** - The actual value (throws if HasValue is false)

This pattern avoids null reference issues and makes null handling explicit.
