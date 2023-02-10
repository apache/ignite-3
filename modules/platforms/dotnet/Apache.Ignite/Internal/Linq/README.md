# Apache Ignite LINQ provider

## What is it?

LINQ provider translates C# LINQ expressions into Ignite-specific SQL.

For example, the following two snippets achieve the same result:

```csharp
```

## Why?

LINQ has the following advantages over SQL:

* Queries are strongly typed and compile-time checked:
  * Easier to write and maintain with IDE support (auto-completion, navigation, find usages).
  * Refactoring-friendly: rename a column and all queries are updated at once.
* Ignite-specific SQL knowledge is not required, and most C# developers are already familiar with LINQ.
* Safe against SQL injections.
* Results are mapped to types naturally.


TODO:
* Why do we need this?
* Getting started
* Supported features
  * Joins
  * Groupings
  * Aggregate operators
  * Strings, math, regex
  * Async extensions
