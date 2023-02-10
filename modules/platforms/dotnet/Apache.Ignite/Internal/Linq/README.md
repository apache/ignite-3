# Apache Ignite LINQ provider

## What is it?

LINQ provider translates C# LINQ expressions into Ignite-specific SQL.

For example, the following two snippets achieve the same result:

**SQL**

```csharp
var query = "select KEY, VAL from PUBLIC.TBL1 where (KEY > ?) order by KEY asc";
await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(transaction: null, query, 3);

var queryResults = new List<Poco>();

await foreach (IIgniteTuple row in resultSet)
{
    queryResults.Add(new Poco { Key = (long)row[0]!, Val = (string?)row[1] });
}
```

**LINQ**

```csharp
var table = await Client.Tables.GetTableAsync("TBL1");

IQueryable<Poco> query = table!.GetRecordView<Poco>().AsQueryable()
    .Where(x => x.Key > 3)
    .OrderBy(x => x.Key);

List<Poco> queryResults = await query.ToListAsync();
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
  * DML / bulk ops
  * Composable queries
* Column name mapping explained
