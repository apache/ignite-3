---
title: Catalog API
id: catalog-api
sidebar_position: 8
---

# Catalog API

The Catalog API manages table schemas, indexes, and distribution zones. Applications define schemas using Java annotations or fluent builders, enabling programmatic DDL without writing SQL statements. This approach provides compile-time validation and type safety.

## Key Concepts

The catalog facade creates, modifies, and drops database objects. Table definitions specify columns, primary keys, indexes, and colocation strategies. Distribution zones control data placement across cluster nodes.

Annotations provide declarative schema definitions. Classes annotated with @Table become table definitions. The catalog creates tables from these classes automatically. Fluent builders offer programmatic alternatives when annotations are impractical.

## Annotation-Based Tables

Define tables using annotations:

```java
@Table("users")
public class User {
    @Id
    public Integer id;

    @Column
    public String name;

    @Column(nullable = false)
    public Integer age;

    @Column(length = 100)
    public String email;
}
```

Create the table from the annotated class:

```java
CompletableFuture<Table> tableFuture =
    ignite.catalog().createTableAsync(User.class);

Table table = tableFuture.join();
```

The catalog generates DDL from annotations.

## Key-Value Table Definitions

Define tables with separate key and value classes:

```java
@Table("products")
public class ProductKey {
    @Id
    public Integer productId;
}

public class ProductValue {
    @Column
    public String name;

    @Column
    public Double price;

    @Column
    public String category;
}
```

Create the table:

```java
CompletableFuture<Table> tableFuture =
    ignite.catalog().createTableAsync(
        ProductKey.class,
        ProductValue.class
    );
```

## Column Configuration

Configure column properties:

```java
@Table("items")
public class Item {
    @Id
    public Integer id;

    @Column(value = "item_name", nullable = false, length = 50)
    public String name;

    @Column(precision = 10, scale = 2)
    public BigDecimal price;

    @Column(columnDefinition = "VARCHAR(255) DEFAULT 'N/A'")
    public String description;
}
```

Column annotations support:
- `value` - Column name (defaults to field name)
- `nullable` - Allow null values (default true)
- `length` - Maximum length for strings
- `precision` - Numeric precision
- `scale` - Numeric scale
- `columnDefinition` - Full SQL type definition

## Composite Primary Keys

Define multi-column primary keys:

```java
@Table("orders")
public class Order {
    @Id
    public Integer customerId;

    @Id
    public Integer orderId;

    @Column
    public LocalDateTime orderDate;

    @Column
    public String status;
}
```

Fields with @Id annotations form the composite primary key.

## Primary Key Ordering

Specify sort order for primary keys:

```java
@Table("events")
public class Event {
    @Id(SortOrder.ASC)
    public Integer categoryId;

    @Id(SortOrder.DESC)
    public Long timestamp;

    @Column
    public String message;
}
```

## Index Definitions

Add indexes through table annotations:

```java
@Table(
    value = "users",
    indexes = {
        @Index(
            value = "idx_email",
            columns = @ColumnRef("email")
        ),
        @Index(
            value = "idx_name_age",
            columns = {
                @ColumnRef("name"),
                @ColumnRef(value = "age", sort = SortOrder.DESC)
            }
        )
    }
)
public class User {
    @Id
    public Integer id;

    @Column
    public String name;

    @Column
    public Integer age;

    @Column
    public String email;
}
```

Index annotations support single and composite indexes with sort orders.

## Colocation Configuration

Configure data colocation:

```java
@Table(
    value = "orders",
    colocateBy = {
        @ColumnRef("customerId")
    }
)
public class Order {
    @Id
    public Integer customerId;

    @Id
    public Integer orderId;

    @Column
    public String product;
}
```

Colocation ensures rows with the same colocation key values reside on the same node.

## Zone Configuration

Define distribution zones:

```java
@Table(
    value = "cache_data",
    zone = @Zone(
        value = "cache_zone",
        partitions = 64,
        replicas = 3,
        storageProfiles = "default"
    )
)
public class CacheRecord {
    @Id
    public String key;

    @Column
    public String value;
}
```

Zone annotations configure partitioning, replication, and storage.

## Fluent Table Definitions

Build tables programmatically:

```java
TableDefinition definition = TableDefinition.builder("products")
    .columns(
        ColumnDefinition.column("id", ColumnType.INT32),
        ColumnDefinition.column("name", ColumnType.VARCHAR),
        ColumnDefinition.column("price", ColumnType.decimal(10, 2)),
        ColumnDefinition.column("category", ColumnType.varchar(50))
    )
    .primaryKey("id")
    .build();

ignite.catalog().createTableAsync(definition).join();
```

## Index Definitions with Builders

Add indexes to table definitions:

```java
TableDefinition definition = TableDefinition.builder("users")
    .columns(
        ColumnDefinition.column("id", ColumnType.INT32),
        ColumnDefinition.column("email", ColumnType.VARCHAR),
        ColumnDefinition.column("name", ColumnType.VARCHAR)
    )
    .primaryKey("id")
    .index("idx_email", IndexType.SORTED, ColumnSorted.column("email"))
    .index("idx_name", IndexType.SORTED, ColumnSorted.column("name"))
    .build();

ignite.catalog().createTableAsync(definition).join();
```

## Conditional Table Creation

Create tables only if they do not exist:

```java
TableDefinition definition = TableDefinition.builder("cache_data")
    .columns(
        ColumnDefinition.column("key", ColumnType.VARCHAR),
        ColumnDefinition.column("value", ColumnType.VARCHAR)
    )
    .primaryKey("key")
    .ifNotExists()
    .build();

ignite.catalog().createTableAsync(definition).join();
```

## Table Deletion

Drop tables by name:

```java
ignite.catalog().dropTableAsync("products").join();
```

Drop using qualified names:

```java
QualifiedName tableName = QualifiedName.of("schema", "products");
ignite.catalog().dropTableAsync(tableName).join();
```

## Distribution Zone Management

Create distribution zones:

```java
ZoneDefinition zone = ZoneDefinition.builder("fast_zone")
    .partitions(128)
    .replicas(2)
    .storageProfiles("ssd")
    .dataNodesAutoAdjustScaleUp(300)
    .dataNodesAutoAdjustScaleDown(600)
    .build();

ignite.catalog().createZoneAsync(zone).join();
```

Zone configuration options:
- `partitions` - Number of partitions
- `replicas` - Number of replicas per partition
- `storageProfiles` - Storage profile name (comma-separated for multiple)
- `dataNodesAutoAdjustScaleUp` - Scale-up timeout in seconds
- `dataNodesAutoAdjustScaleDown` - Scale-down timeout in seconds
- `filter` - Node filter expression
- `consistencyMode` - Consistency mode

## Zone Deletion

Drop distribution zones:

```java
ignite.catalog().dropZoneAsync("fast_zone").join();
```

## Schema Names

Specify schemas in table annotations:

```java
@Table(value = "users", schemaName = "app_schema")
public class User {
    @Id
    public Integer id;

    @Column
    public String name;
}
```

Tables without explicit schemas default to the PUBLIC schema.

## Complex Zone Configuration

Configure zones with advanced options:

```java
ZoneDefinition zone = ZoneDefinition.builder("replicated_zone")
    .partitions(32)
    .replicas(5)
    .quorumSize(3)
    .storageProfiles("ssd,hdd")
    .distributionAlgorithm("rendezvous")
    .consistencyMode("strong")
    .filter("region == 'us-east'")
    .build();

ignite.catalog().createZoneAsync(zone).join();
```

## Reference

- Catalog facade: `org.apache.ignite.catalog.IgniteCatalog`
- Table definition: `org.apache.ignite.catalog.definitions.TableDefinition`
- Column definition: `org.apache.ignite.catalog.definitions.ColumnDefinition`
- Index definition: `org.apache.ignite.catalog.definitions.IndexDefinition`
- Zone definition: `org.apache.ignite.catalog.definitions.ZoneDefinition`

### Annotations

- `@Table` - Mark class as table definition
- `@Column` - Configure column properties
- `@Id` - Mark primary key columns
- `@Index` - Define table indexes
- `@Zone` - Configure distribution zone
- `@ColumnRef` - Reference columns in indexes and colocation

### IgniteCatalog Methods

- `CompletableFuture<Table> createTableAsync(Class<?>)` - Create from annotated class
- `CompletableFuture<Table> createTableAsync(Class<?>, Class<?>)` - Create from key-value classes
- `CompletableFuture<Table> createTableAsync(TableDefinition)` - Create from definition
- `CompletableFuture<Void> dropTableAsync(String)` - Drop table by name
- `CompletableFuture<Void> dropTableAsync(QualifiedName)` - Drop by qualified name
- `CompletableFuture<Void> createZoneAsync(ZoneDefinition)` - Create distribution zone
- `CompletableFuture<Void> dropZoneAsync(String)` - Drop zone

### TableDefinition Builder Methods

- `static Builder builder(String)` - Create builder
- `columns(ColumnDefinition...)` - Add columns
- `primaryKey(String...)` - Set primary key
- `index(String...)` - Add index on specified columns (uses default index type)
- `index(String, IndexType, ColumnSorted...)` - Add named index with type and sorted columns
- `colocateBy(String...)` - Set colocation columns
- `zone(String)` - Set zone name
- `ifNotExists()` - Create only if not exists
- `build()` - Build definition

### ZoneDefinition Builder Methods

- `static Builder builder(String)` - Create builder
- `partitions(int)` - Set partition count
- `replicas(int)` - Set replica count
- `quorumSize(int)` - Set quorum size
- `storageProfiles(String)` - Set storage profile (single string, comma-separated for multiple)
- `distributionAlgorithm(String)` - Set distribution algorithm
- `dataNodesAutoAdjustScaleUp(int)` - Set scale-up timeout
- `dataNodesAutoAdjustScaleDown(int)` - Set scale-down timeout
- `filter(String)` - Set node filter expression
- `consistencyMode(String)` - Set consistency mode
- `build()` - Build definition
