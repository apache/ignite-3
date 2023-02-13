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


## Getting Started

1. Create a table.
```csharp
await Client.Sql.ExecuteAsync(null, @"CREATE TABLE PUBLIC.PERSON (NAME VARCHAR PRIMARY KEY, AGE INT)");
```

2. Define classes (or records) that represent tables.
   * Member names should match column names (case-insensitive).
   * If a column name is not a valid C# identifier, use `[Column("name")]` attribute to specify the name.
```csharp
public record Person(string Name, int Age);
```

3. Obtain a table reference
```csharp
ITable table = await Client.Tables.GetTableAsync("PERSON");
```
 
5. Use `GetRecordView<T>()` to get a typed view of the table.
```csharp
IRecordView<Person> view = table.GetRecordView<Person>();
```

6. Use `AsQueryable()` to perform LINQ queries on `IRecordView<T>`.
```csharp
List<string> names = await view.AsQueryable()
    .Where(x => x.Age > 30)
    .Select(x => x.Name)
    .ToListAsync();
```

## Inspecting Generated SQL

Viewing generated SQL is useful for debugging and performance tuning. There are two ways to do it:

* `IgniteQueryableExtensions.ToQueryString()` extension method:

```csharp
IQueryable<Person> query = table.GetRecordView<Person>().AsQueryable().Where(x => x.Age > 30);

string sql = query.ToQueryString();
```

* Debug logging:

```csharp
var cfg = new IgniteClientConfiguration
{
    Logger = new ConsoleLogger { MinLevel = LogLevel.Debug },
    ...
};

using var client = IgniteClient.StartAsync(cfg);
...
```

All generated SQL will be logged with `Debug` level to the specified logger.

## Using Transactions

Transaction can be passed to the LINQ provider via the first `AsQueryeable` parameter:

```csharp
await using var tx = await client.Transactions.BeginAsync();
var view = (await client.Tables.GetTableAsync("person"))!.GetRecordView<Person>();

pocoView.AsQueryable(tx)...;
```

## Custom Query Options

Custom query options (timeout, page size) can be specified via the second `AsQueryable` parameter with `QueryableOptions`:

```csharp
var options = new QueryableOptions
{
    PageSize = 512,
    Timeout = TimeSpan.FromSeconds(30)
};

table.GetRecordView<Person>().AsQueryable(options: options)...;
```

## Supported Features

### Result Materialization
TODO

### Projections
TODO

### Inner Joins
TODO

### Outer Joins
TODO

### Groupings
TODO

### Aggregates
TODO

### Union, Intersect, Except

### Math Functions
TODO

### String Functions
TODO

### Regular Expressions
TODO

### DML (Bulk Update and Delete)
TODO

### Composing Queries
TODO


### Column Name Mapping
TODO

### KeyValueView

TODO
