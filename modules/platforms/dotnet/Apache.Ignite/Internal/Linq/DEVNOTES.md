# Apache Ignite LINQ provider

Translates C# LINQ expressions into Ignite-specific SQL.

## Async

LINQ provider uses underlying `Sql` API, which is fully async. While users can call sync methods like `ToList()`, `First()`, `Sum()`, etc, this will lead to thread blocking, which is not ideal for scalability.

* Recommended approach is to use `ToResultSetAsync()` async extension method.
* Later we should also provide `ToListAsync()`, `FirstAsync()`, `SumAsync()` and other extension methods (IGNITE-18084, inspired by EF Core).

## User Type Mapping

TODO: Explain schema issues, see comments in code.

## Differences with Ignite 2.x provider

This provider is a port of existing Ignite 2.x code. Major differences are:

* Underlying async API (see above).
* Fully integrated into core .NET client (as opposed to an extension in 2.x). Entry points are `IRecordView.AsQueryable` and `IKeyValueView.AsQueryable`. Record and KV views do not implement `IEnumerable` and/or `IQueryable` to avoid any confusing behavior.
* Supports transactions.
