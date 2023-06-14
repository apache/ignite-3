# Apache Ignite LINQ provider - Development Notes

Translates C# LINQ expressions into Ignite-specific SQL.


## Async

LINQ provider uses underlying `Sql` API, which is fully async. 
While users can call sync methods like `ToList()`, `First()`, `Sum()`, etc, this will lead to thread blocking, which is not ideal for scalability.

To retrieve LINQ results asynchronously, `IgniteQueryableExtensions` class provides extension methods:  
* Universal `ToResultSetAsync()`, which returns Ignite-specific generic `IResultSet`.
* Async variants for all standard methods: `ToListAsync`, `ToDictionaryAsync`, `FirstAsync`, `SumAsync`, etc.


## User Type Mapping

There are two ways to map columns to user type members:
1. Load schema and map only matching columns.
   - GOOD: Potentially nicer to the user, allows unmapped members in user types without extra steps, more flexible with updated schemas.
   - BAD: Requires loading schema before query translation (worse perf, worse complexity).
   - BAD: Can't cache metadata and delegates per type (worse perf, worse complexity).
   - BAD: Obstacle for compiled queries, because updated schema won't be picked up.
2. Do not load schema, map all object columns.
   - GOOD: Simpler, faster.
   - BAD: All columns are mapped by default, unmapped columns should be explicitly excluded with `NotMappedAttribute`.

We take the second approach for the sake of performance, simplicity and clarity.

**Primitive type mapping is not supported**: not possible to determine column names without schema.

### Type Member to Column Mapping

1. The mapping logic is based on **fields** only.
2. Backing fields for [automatic properties](https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/classes-and-structs/auto-implemented-properties) get the column name from the property or its attributes.
3. When `[Column("CustomName")]` attribute is present with non-zero `Name`, we use the specified name as it is, in quoted form: `select TABLE_NAME."CustomName" from ...`
4. Otherwise, uppercase type member name is used without quotes (case-insensitive): `select TABLE_NAME.FIELDNAME from ...`
5. Fields and properties with `[NotMapped]` attribute are ignored.


## Differences with Ignite 2.x provider

This provider is a port of existing Ignite 2.x code. Major differences are:

* Underlying async API (see above).
* Fully integrated into core .NET client (as opposed to an extension in 2.x). Entry points are `IRecordView.AsQueryable` and `IKeyValueView.AsQueryable`. Record and KV views do not implement `IEnumerable` and/or `IQueryable` to avoid any confusing behavior.
* Supports transactions.
