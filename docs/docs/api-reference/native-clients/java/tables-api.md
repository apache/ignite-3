---
title: Tables API
id: tables-api
sidebar_position: 3
---

# Tables API

The Tables API provides structured access to data stored in Ignite tables. Applications interact with tables through views that offer different perspectives on the data: record views for full row operations and key-value views for key-based access patterns.

## Key Concepts

Tables store data in rows with typed columns. The API provides three access patterns through views. RecordView treats each row as a complete record. KeyValueView separates rows into key and value portions. Both support binary Tuple access and typed object mapping.

Operations execute within optional transaction contexts. Pass null for auto-commit behavior or provide a Transaction for multi-operation atomicity.

## Table Discovery

Access tables through the tables manager:

```java
Table users = ignite.tables().table("users");
if (users == null) {
    System.out.println("Table does not exist");
}
```

For qualified names with schemas:

```java
Table products = ignite.tables().table(QualifiedName.of("inventory", "products"));
```

List all tables asynchronously:

```java
CompletableFuture<List<Table>> tablesFuture = ignite.tables().tablesAsync();
tablesFuture.thenAccept(tables -> {
    for (Table table : tables) {
        System.out.println(table.name());
    }
});
```

## Record View

RecordView operations work with complete rows:

```java
RecordView<Tuple> view = table.recordView();

// Insert or update
Tuple record = Tuple.create()
    .set("id", 1)
    .set("name", "Alice")
    .set("age", 30);
view.upsert(null, record);

// Retrieve
Tuple key = Tuple.create().set("id", 1);
Tuple result = view.get(null, key);
System.out.println(result.stringValue("name"));

// Check existence
boolean exists = view.contains(null, key);

// Delete
boolean deleted = view.delete(null, key);
```

Pass null as the transaction parameter for operations outside transactions.

## Typed Record View

Map rows to Java objects using typed views:

```java
public class User {
    public int id;
    public String name;
    public int age;
}

RecordView<User> view = table.recordView(User.class);

User user = new User();
user.id = 1;
user.name = "Alice";
user.age = 30;

view.upsert(null, user);

User key = new User();
key.id = 1;
User retrieved = view.get(null, key);
```

The view automatically maps between object fields and table columns.

## Key-Value View

KeyValueView separates key and value portions:

```java
KeyValueView<Tuple, Tuple> view = table.keyValueView();

Tuple key = Tuple.create().set("id", 1);
Tuple value = Tuple.create()
    .set("name", "Alice")
    .set("age", 30);

view.put(null, key, value);

Tuple retrieved = view.get(null, key);
System.out.println(retrieved.stringValue("name"));

// Check for null vs missing
NullableValue<Tuple> nullable = view.getNullable(null, key);
if (nullable != null) {
    System.out.println("Found: " + nullable.get());
}
```

NullableValue distinguishes between missing entries and entries with null values.

## Typed Key-Value View

Map keys and values to separate types:

```java
public class ProductKey {
    public int id;
}

public class ProductValue {
    public String name;
    public double price;
}

KeyValueView<ProductKey, ProductValue> view =
    table.keyValueView(ProductKey.class, ProductValue.class);

ProductKey key = new ProductKey();
key.id = 100;

ProductValue value = new ProductValue();
value.name = "Widget";
value.price = 29.99;

view.put(null, key, value);

ProductValue retrieved = view.get(null, key);
```

## Batch Operations

Process multiple records in single operations:

```java
RecordView<Tuple> view = table.recordView();

List<Tuple> records = Arrays.asList(
    Tuple.create().set("id", 1).set("name", "Alice"),
    Tuple.create().set("id", 2).set("name", "Bob"),
    Tuple.create().set("id", 3).set("name", "Carol")
);

view.upsertAll(null, records);

List<Tuple> keys = Arrays.asList(
    Tuple.create().set("id", 1),
    Tuple.create().set("id", 2)
);

List<Tuple> results = view.getAll(null, keys);
```

Batch operations reduce network overhead for multiple operations.

## Key-Value Batch Operations

Similar batch support for key-value views:

```java
KeyValueView<Tuple, Tuple> view = table.keyValueView();

Map<Tuple, Tuple> entries = new HashMap<>();
entries.put(
    Tuple.create().set("id", 1),
    Tuple.create().set("name", "Alice")
);
entries.put(
    Tuple.create().set("id", 2),
    Tuple.create().set("name", "Bob")
);

view.putAll(null, entries);

Collection<Tuple> keys = Arrays.asList(
    Tuple.create().set("id", 1),
    Tuple.create().set("id", 2)
);

Map<Tuple, Tuple> results = view.getAll(null, keys);
```

## Conditional Operations

Execute operations based on current values:

```java
KeyValueView<Tuple, Tuple> view = table.keyValueView();

Tuple key = Tuple.create().set("id", 1);
Tuple oldValue = Tuple.create().set("status", "pending");
Tuple newValue = Tuple.create().set("status", "active");

// Replace only if current value matches
boolean replaced = view.replace(null, key, oldValue, newValue);

if (replaced) {
    System.out.println("Value updated");
}
```

Conditional operations provide atomic compare-and-set semantics.

## Asynchronous Operations

All operations support asynchronous execution:

```java
RecordView<Tuple> view = table.recordView();

Tuple record = Tuple.create()
    .set("id", 1)
    .set("name", "Alice");

CompletableFuture<Void> upsertFuture = view.upsertAsync(null, record);

Tuple key = Tuple.create().set("id", 1);
CompletableFuture<Tuple> getFuture = view.getAsync(null, key);

getFuture.thenAccept(result -> {
    System.out.println(result.stringValue("name"));
});
```

Asynchronous operations return immediately without blocking the calling thread.

## Partition Information

Access partition metadata through the partition manager:

```java
PartitionManager partitions = table.partitionManager();
CompletableFuture<Partition> partition =
    partitions.partitionAsync(Tuple.create().set("id", 1));

partition.thenAccept(p -> {
    System.out.println("Record belongs to partition: " + p.id());
});
```

Partition information enables colocated compute operations.

## Tuple Construction

Create tuples with various approaches:

```java
// Empty tuple
Tuple tuple1 = Tuple.create();

// With capacity hint
Tuple tuple2 = Tuple.create(10);

// From map
Map<String, Object> data = new HashMap<>();
data.put("id", 1);
data.put("name", "Alice");
Tuple tuple3 = Tuple.create(data);

// Copy existing
Tuple tuple4 = Tuple.copy(tuple3);
```

Set values by name:

```java
Tuple tuple = Tuple.create()
    .set("id", 1)
    .set("name", "Alice")
    .set("age", 30)
    .set("balance", 1000.50)
    .set("created", LocalDateTime.now());
```

## Tuple Value Access

Retrieve values by column name with type-specific methods:

```java
int id = tuple.intValue("id");
String name = tuple.stringValue("name");
Integer age = tuple.value("age");
LocalDateTime created = tuple.value("created");

// Access by index
Object value = tuple.value(0);
String columnName = tuple.columnName(0);
int columnIndex = tuple.columnIndex("name");
```

Type-specific accessors avoid boxing for primitive types.

## Reference

- Table management: `org.apache.ignite.table.IgniteTables`
- Table interface: `org.apache.ignite.table.Table`
- Record access: `org.apache.ignite.table.RecordView<R>`
- Key-value access: `org.apache.ignite.table.KeyValueView<K, V>`
- Binary records: `org.apache.ignite.table.Tuple`
- Partition info: `org.apache.ignite.table.partition.PartitionManager`

### IgniteTables Methods

- `List<Table> tables()` - Get all tables synchronously
- `CompletableFuture<List<Table>> tablesAsync()` - Get all tables asynchronously
- `Table table(String name)` - Get table by simple name
- `Table table(QualifiedName name)` - Get table by qualified name
- `CompletableFuture<Table> tableAsync(String name)` - Get table asynchronously
- `CompletableFuture<Table> tableAsync(QualifiedName name)` - Get table asynchronously with qualified name

### Table View Methods

- `RecordView<Tuple> recordView()` - Get binary record view
- `RecordView<R> recordView(Class<R>)` - Get typed record view
- `RecordView<R> recordView(Mapper<R>)` - Get record view with custom mapper
- `KeyValueView<Tuple, Tuple> keyValueView()` - Get binary key-value view
- `KeyValueView<K, V> keyValueView(Class<K>, Class<V>)` - Get typed key-value view
- `KeyValueView<K, V> keyValueView(Mapper<K>, Mapper<V>)` - Get key-value view with custom mappers

### RecordView CRUD Methods

- `R get(Transaction, R keyRec)` - Get record by key
- `CompletableFuture<R> getAsync(Transaction, R keyRec)` - Async get
- `List<R> getAll(Transaction, Collection<R>)` - Get multiple records
- `CompletableFuture<List<R>> getAllAsync(Transaction, Collection<R>)` - Async get multiple
- `boolean contains(Transaction, R keyRec)` - Check existence
- `void upsert(Transaction, R rec)` - Insert or update record
- `CompletableFuture<Void> upsertAsync(Transaction, R rec)` - Async upsert
- `void upsertAll(Transaction, Collection<R>)` - Insert or update multiple
- `boolean delete(Transaction, R keyRec)` - Delete record
- `CompletableFuture<Boolean> deleteAsync(Transaction, R keyRec)` - Async delete

### KeyValueView Methods

- `V get(Transaction, K key)` - Get value by key
- `CompletableFuture<V> getAsync(Transaction, K key)` - Async get
- `NullableValue<V> getNullable(Transaction, K key)` - Get with null distinction
- `Map<K, V> getAll(Transaction, Collection<K>)` - Get multiple values
- `void put(Transaction, K key, V value)` - Put key-value pair
- `CompletableFuture<Void> putAsync(Transaction, K key, V value)` - Async put
- `void putAll(Transaction, Map<K, V>)` - Put multiple pairs
- `boolean replace(Transaction, K key, V old, V new)` - Conditional replace
- `void remove(Transaction, K key)` - Remove by key
- `void removeAll(Transaction, Collection<K>)` - Remove multiple
