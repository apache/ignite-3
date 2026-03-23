---
title: Criteria API
id: criteria-api
sidebar_position: 9
---

# Criteria API

The Criteria API builds type-safe query predicates for table operations. Applications construct queries using Java code instead of string-based SQL. This approach provides compile-time validation and IDE support for query construction.

## Key Concepts

Criteria queries filter table data using predicates. Predicates combine column conditions with logical operators. The API supports common comparison operations including equality, ranges, null checks, and pattern matching.

Both RecordView and KeyValueView implement CriteriaQuerySource, enabling criteria queries on any view type. Results stream through cursors that must be closed to release resources.

## Basic Criteria Queries

Query with simple equality conditions:

```java
RecordView<Tuple> view = table.recordView();

Criteria criteria = Criteria.columnValue(
    "age",
    Condition.equalTo(30)
);

try (Cursor<Tuple> cursor = view.query(null, criteria, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name"));
    }
}
```

Always close cursors to free server resources.

## Comparison Conditions

Use various comparison operators:

```java
// Greater than
Criteria criteria1 = Criteria.columnValue("age", Condition.greaterThan(25));

// Greater or equal
Criteria criteria2 = Criteria.columnValue("age", Condition.greaterThanOrEqualTo(25));

// Less than
Criteria criteria3 = Criteria.columnValue("age", Condition.lessThan(65));

// Less or equal
Criteria criteria4 = Criteria.columnValue("age", Condition.lessThanOrEqualTo(65));

// Not equal
Criteria criteria5 = Criteria.columnValue("status", Condition.notEqualTo("inactive"));
```

## Null Checks

Check for null or non-null values:

```java
// IS NULL
Criteria nullCriteria = Criteria.columnValue("middleName", Condition.nullValue());

try (Cursor<Tuple> cursor = view.query(null, nullCriteria, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name"));
    }
}

// IS NOT NULL
Criteria notNullCriteria = Criteria.columnValue("email", Condition.notNullValue());
```

## IN Conditions

Match against multiple values:

```java
Criteria criteria = Criteria.columnValue(
    "category",
    Condition.in("electronics", "appliances", "gadgets")
);

try (Cursor<Tuple> cursor = view.query(null, criteria, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name"));
    }
}

// NOT IN
Criteria notInCriteria = Criteria.columnValue(
    "status",
    Condition.notIn("deleted", "archived")
);
```

## AND Conditions

Combine multiple conditions with AND:

```java
Criteria ageCriteria = Criteria.columnValue("age", Condition.greaterThan(25));
Criteria statusCriteria = Criteria.columnValue("status", Condition.equalTo("active"));

Criteria combined = Criteria.and(ageCriteria, statusCriteria);

try (Cursor<Tuple> cursor = view.query(null, combined, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name"));
    }
}
```

All conditions must match for records to satisfy AND criteria.

## OR Conditions

Combine conditions with OR:

```java
Criteria junior = Criteria.columnValue("age", Condition.lessThan(30));
Criteria senior = Criteria.columnValue("age", Condition.greaterThan(60));

Criteria combined = Criteria.or(junior, senior);

try (Cursor<Tuple> cursor = view.query(null, combined, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name") + ": " +
            record.intValue("age"));
    }
}
```

Records matching any condition satisfy OR criteria.

## Complex Predicates

Nest AND and OR conditions:

```java
Criteria youngActive = Criteria.and(
    Criteria.columnValue("age", Condition.lessThan(30)),
    Criteria.columnValue("status", Condition.equalTo("active"))
);

Criteria seniorActive = Criteria.and(
    Criteria.columnValue("age", Condition.greaterThan(60)),
    Criteria.columnValue("status", Condition.equalTo("active"))
);

Criteria combined = Criteria.or(youngActive, seniorActive);

try (Cursor<Tuple> cursor = view.query(null, combined, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name"));
    }
}
```

## Typed Criteria Queries

Query typed views with criteria:

```java
public class User {
    public int id;
    public String name;
    public int age;
    public String status;
}

RecordView<User> view = table.recordView(User.class);

Criteria criteria = Criteria.and(
    Criteria.columnValue("age", Condition.greaterThan(25)),
    Criteria.columnValue("status", Condition.equalTo("active"))
);

try (Cursor<User> cursor = view.query(null, criteria, null, null)) {
    for (User user : cursor) {
        System.out.println(user.name + " is " + user.age + " years old");
    }
}
```

## Key-Value View Queries

Query key-value views with criteria:

```java
KeyValueView<Tuple, Tuple> view = table.keyValueView();

Criteria criteria = Criteria.columnValue("category", Condition.equalTo("electronics"));

try (Cursor<Entry<Tuple, Tuple>> cursor = view.query(null, criteria, null, null)) {
    for (Entry<Tuple, Tuple> entry : cursor) {
        Tuple key = entry.getKey();
        Tuple value = entry.getValue();
        System.out.println("Product " + key.intValue("id") + ": " +
            value.stringValue("name"));
    }
}
```

Key-value queries return Entry instances containing keys and values.

## Query Options

Configure query behavior:

```java
CriteriaQueryOptions options = CriteriaQueryOptions.builder()
    .pageSize(100)
    .build();

try (Cursor<Tuple> cursor = view.query(null, criteria, null, options)) {
    for (Tuple record : cursor) {
        // Process records
    }
}
```

Query options control result pagination and fetching behavior.

## Asynchronous Queries

Execute criteria queries asynchronously:

```java
Criteria criteria = Criteria.columnValue("age", Condition.greaterThan(30));

CompletableFuture<AsyncCursor<Tuple>> cursorFuture =
    view.queryAsync(null, criteria, null, null);

cursorFuture.thenAccept(cursor -> {
    for (Tuple record : cursor.currentPage()) {
        System.out.println(record.stringValue("name"));
    }
    cursor.closeAsync();
});
```

Asynchronous queries return immediately without blocking. Use currentPage() to access results and closeAsync() to release resources.

## Transaction Integration

Execute queries within transactions:

```java
ignite.transactions().runInTransaction(tx -> {
    RecordView<Tuple> view = table.recordView();

    Criteria criteria = Criteria.columnValue("balance", Condition.greaterThan(1000));

    try (Cursor<Tuple> cursor = view.query(tx, criteria, null, null)) {
        for (Tuple record : cursor) {
            int balance = record.intValue("balance");
            record.set("balance", balance * 1.05);
            view.upsert(tx, record);
        }
    }
});
```

Queries within transactions see consistent data snapshots.

## Range Queries

Combine conditions for range queries:

```java
Criteria rangeCriteria = Criteria.and(
    Criteria.columnValue("price", Condition.greaterThanOrEqualTo(10.0)),
    Criteria.columnValue("price", Condition.lessThanOrEqualTo(50.0))
);

try (Cursor<Tuple> cursor = view.query(null, rangeCriteria, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name") + ": $" +
            record.doubleValue("price"));
    }
}
```

## Multiple Column Conditions

Query on multiple columns:

```java
Criteria criteria = Criteria.and(
    Criteria.columnValue("category", Condition.equalTo("electronics")),
    Criteria.columnValue("inStock", Condition.equalTo(true)),
    Criteria.columnValue("price", Condition.lessThan(1000.0)),
    Criteria.columnValue("rating", Condition.greaterThanOrEqualTo(4.0))
);

try (Cursor<Tuple> cursor = view.query(null, criteria, null, null)) {
    for (Tuple record : cursor) {
        System.out.println(record.stringValue("name"));
    }
}
```

## Cursor Iteration

Process cursor results:

```java
Criteria criteria = Criteria.columnValue("status", Condition.equalTo("pending"));

try (Cursor<Tuple> cursor = view.query(null, criteria, null, null)) {
    while (cursor.hasNext()) {
        Tuple record = cursor.next();
        System.out.println(record.stringValue("orderId"));
    }
}
```

Cursors implement Iterator for standard iteration patterns.

## Reference

- Criteria builder: `org.apache.ignite.table.criteria.Criteria`
- Conditions: `org.apache.ignite.table.criteria.Condition`
- Query source: `org.apache.ignite.table.criteria.CriteriaQuerySource<R>`
- Query options: `org.apache.ignite.table.criteria.CriteriaQueryOptions`
- Column reference: `org.apache.ignite.table.criteria.Column`

### Criteria Factory Methods

- `static Criteria columnValue(String, Condition)` - Create column condition
- `static Criteria and(Criteria...)` - Combine with AND
- `static Criteria or(Criteria...)` - Combine with OR

### Condition Factory Methods

- `static Condition equalTo(Object)` - Equality comparison
- `static Condition notEqualTo(Object)` - Inequality comparison
- `static Condition greaterThan(Object)` - Greater than
- `static Condition greaterThanOrEqualTo(Object)` - Greater or equal
- `static Condition lessThan(Object)` - Less than
- `static Condition lessThanOrEqualTo(Object)` - Less or equal
- `static Condition in(Object...)` - IN clause
- `static Condition notIn(Object...)` - NOT IN clause
- `static Condition nullValue()` - IS NULL
- `static Condition notNullValue()` - IS NOT NULL

### CriteriaQuerySource Methods

- `Cursor<R> query(Transaction, Criteria, String, CriteriaQueryOptions)` - Execute query
- `CompletableFuture<AsyncCursor<R>> queryAsync(Transaction, Criteria, String, CriteriaQueryOptions)` - Async query

### CriteriaQueryOptions Configuration

- `pageSize(int)` - Set result page size
- `build()` - Build options

### Cursor Operations

- `boolean hasNext()` - Check for more results
- `R next()` - Get next result
- `void close()` - Close cursor and release resources
