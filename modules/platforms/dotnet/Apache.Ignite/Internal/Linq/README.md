# Apache Ignite LINQ provider

## What?

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
public record Person(string Name, int Age, string Address, string Status);
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
    Logger = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug)),
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

## Result Materialization

Materialization is the process of converting query results (`IQueryable<T>`) into an object or a collection of objects. 

LINQ is lazy. Nothing happens (no network calls, no SQL translation) until the query is materialized. 
For example, the following code only constructs an expression, but does not execute anything:

```csharp
IQueryable<Person> query = table!.GetRecordView<Person>().AsQueryable()
    .Where(x => x.Key > 3)
    .OrderBy(x => x.Key);
```

Query execution and materialization can be triggered in multiple ways:

### Iteration

```csharp
foreach (var person in query) { ... }

await foreach (var person in query.AsAsyncEnumerable()) { ... }
```

### ToList, ToDictionary

```csharp
List<Person> list = query.ToList();
Dictionary<string, int> dict = query.ToDictionary(x => x.Name, x => x.Age);
```

Async variants are available in `IgniteQueryableExtensions`:

```csharp
List<Person> list = await query.ToListAsync();
Dictionary<string, int> dict = await query.ToDictionaryAsync(x => x.Name, x => x.Age);
```

### Single Result Functions

```csharp
Person first = query.First();
Person? firstOrDefault = query.FirstOrDefault();
Person single = query.Single();
Person? singleOrDefault = query.SingleOrDefault();
int maxAge = query.Max(x => x.Age); 
int minAge = query.Min(x => x.Age); 
int avgAge = query.Average(x => x.Age);
int sumAge = query.Sum(x => x.Age);
int count = query.Count();
long longCount = query.LongCount();
bool any = query.Any(x => x.Age > 30);
bool all = query.All(x => x.Age > 30);
```

Async variants are available in `IgniteQueryableExtensions`:

```csharp
Person first = await query.FirstAsync();
Person? firstOrDefault = await query.FirstOrDefaultAsync();
Person single = await query.SingleAsync();
Person? singleOrDefault = await query.SingleOrDefaultAsync();
int maxAge = await query.MaxAsync(x => x.Age); 
int minAge = await query.MinAsync(x => x.Age); 
int avgAge = await query.AverageAsync(x => x.Age);
int sumAge = await query.SumAsync(x => x.Age);
int count = await query.CountAsync();
long longCount = await query.LongCountAsync();
bool any = await query.AnyAsync(x => x.Age > 30);
bool all = await query.AllAsync(x => x.Age > 30);
```

### Ignite-specific IResultSet

Underlying `IResultSet` can be obtained via `IgniteQueryableExtensions.ToResultSetAsync()` extension method:

```csharp
await using IResultSet<Person> resultSet = await query.ToResultSetAsync();

Console.WriteLine(resultSet.Metadata);
var rows = resultSet.CollectAsync(...);
```

Obtaining `IResultSet` can be useful for access to metadata and `CollectAsync` method, which provides more control over result materialization.

## Supported LINQ Features

### Projections

Projection is the process of converting query results into a different type. 
Among other things, projections are used to select a subset of columns.

For example, `Person` table may have many columns, but we only need `Name` and `Age`. First, create a projection class:

```csharp
public record PersonInfo(string Name, int Age);
```

Then, use `Select` to project query results:

```csharp
List<PersonInfo> result = query
    .Select(x => new PersonInfo(x.Name, x.Age))
    .ToList();
```

Resulting SQL will select only those two columns, avoiding overfetching 
(overfetching is a common issue when ORM-generated query includes all table columns, but only a few of them are needed by the business logic).

Ignite also supports anonymous type projections:

```csharp
var result = query.Select(x => new { x.Name, x.Age }).ToList();
```

### Inner Joins

Use standard `Join` method to perform joins on other tables:

```csharp
var customerQuery = customerTable.GetRecordView<Customer>().AsQueryable();
var orderQuery = orderTable.GetRecordView<Order>().AsQueryable();

var ordersByCustomer = customerQuery
    .Join(orderQuery, cust => cust.Id, order => order.CustId, (cust, order) => new { cust.Name, order.Amount })
    .ToList();
```

### Outer Joins

Outer joins are supported via `DefaultIfEmpty` method. 
For example, not every book in a library is borrowed by a student, so a left outer join is used to retrieve all books and their current borrowers (if any):

```csharp
var bookQuery = bookTable.GetRecordView<Book>().AsQueryable();
var studentQuery = studentTable.GetRecordView<Student>().AsQueryable();

var booksWithStudents = bookQuery
    .Join(studentQuery.DefaultIfEmpty(), book => book.StudentId, student => student.Id, (book, student) => new { book.Title, student.Name })
    .ToList();
```

### Grouping

Single column:

```csharp
var bookCountByAuthor = bookTable.GetRecordView<Book>().AsQueryable()
    .GroupBy(book => book.Author)
    .Select(grp => new { Author = grp.Key, Count = x.Count() })
    .ToList();
```

Multiple columns - use anonymous type:

```csharp
var bookCountByAuthorAndYear = bookTable.GetRecordView<Book>().AsQueryable()
    .GroupBy(book => new { book.Author, book.Year })
    .Select(grp => new { Author = grp.Key.Author, Year = grp.Key.Year, Count = x.Count() })
    .ToList();
```

Aggregate functions `Count`, `Sum`, `Min`, `Max`, `Average` can be used with groupings.

### Ordering

`OrderBy`, `OrderByDescending`, `ThenBy`, `ThenByDescending` are supported. Combine them to order by multiple columns:

```csharp
var booksOrderedByAuthorAndYear = bookTable.GetRecordView<Book>().AsQueryable()
    .OrderBy(book => book.Author)
    .ThenByDescending(book => book.Year)
    .ToList();
```

### Aggregates

All aggregate functions are supported: `Count`, `Sum`, `Min`, `Max`, `Average`. Async variants are available in `IgniteQueryableExtensions`.

See examples in "Single Result Functions" above.

### Union, Intersect, Except

Multiple result sets can be combined using `Union`, `Intersect`, `Except` methods.

```csharp
IQueryable<string> employeeEmails = employeeTable.GetRecordView<Employee>().AsQueryable()
    .Select(x => x.Email);
    
IQueryable<string> customerEmails = customerTable.GetRecordView<Customer>().AsQueryable()
    .Select(x => x.Email);
    
List<string> allEmails = employeeEmails.Union(customerEmails)
    .OrderBy(x => x)
    .ToList();
    
List<string> employeesThatAreCustomers = employeeEmails.Intersect(customerEmails).ToList();
```

### Math Functions

The following `Math` functions are supported (will be translated to SQL equivalents):
`Abs`, `Cos`, `Cosh`, `Acos`, `Sin`, `Sinh`, `Asin`, `Tan`, `Tanh`, `Atan`, `Ceiling`, `Floor`, 
`Exp`, `Log`, `Log10`, `Pow`, `Round`, `Sign`, `Sqrt`, `Truncate`.

The following `Math` functions are NOT supported (no equivalent in Ignite SQL engine):
`Acosh`, `Asinh`, `Atanh`, `Atan2`, `Log2`, `Log(x, y)`.

Example:

```csharp
var triangles = table.GetRecordView<Triangle>().AsQueryable()
    .Select(t => new { 
            Hypotenuse, 
            Opposite = t.Hypotenuse * Math.Sin(t.Angle), 
            Adjacent = t.Hypotenuse * Math.Cos(t.Angle) 
        })
    .ToList();
```

### String Functions

`string.Compare(string)`, `string.Compare(string, bool ignoreCase)`, concatenation `s1 + s2 + s3`, `ToUpper`, `ToLower`, 
`Substring(start)`, `Substring(start, len)`, 
`Trim`, `Trim(char)`, `TrimStart`, `TrimStart(char)`, `TrimEnd`, `TrimEnd(char)`, 
`Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Length`, `Replace`.

Example:

```csharp
List<string> fullNames = table.GetRecordView<Person>().AsQueryable()
    .Where(p => p.FirstName.StartsWith("Jo"))
    .Select(p => new { FullName = p.FirstName.ToUpper() + " " + p.LastName.ToLower() })
    .ToList();
```

### Regular Expressions

`Regex.Replace` is translated to `regexp_replace` function.

```csharp
List<string> addresses = table.GetRecordView<Person>().AsQueryable()
    .Select(p => new { Address = Regex.Replace(p.Address, @"(\d+)", "[$1]")
    .ToList();
```

Keep in mind that regex engine within SQL may behave differently from .NET regex engine.

### DML (Bulk Update and Delete)

Bulk update and delete with optional conditions are supported via `ExecuteUpdateAsync` and `ExecuteDeleteAsync` extensions methods on `IQueryable<T>`:

```csharp
var orders = orderTable.GetRecordView<Order>().AsQueryable();

await orders.Where(x => x.Amount == 0).ExecuteDeleteAsync();
```

Update statement can set properties to constant values or to an expression based on other properties of the same row:

```csharp
var orders = orderTable.GetRecordView<Order>().AsQueryable();

await orders
    .Where(x => x.CustomerId == customerId)
    .ExecuteUpdateAsync(
        order => order.SetProperty(x => x.Discount, 0.1m)
                      .SetProperty(x => x.Note, x => x.Note + " Happy birthday, " + x.CustomerName));
```

Resulting SQL:

```sql
update PUBLIC.tbl1 as _T0 
set NOTE = concat(concat(_T0.NOTE, ?), _T0.CUSTOMERNAME), DISCOUNT = ? 
where (_T0.CUSTOMERID IS NOT DISTINCT FROM ?)
```

### Composing Queries

`IQueryable<T>` expressions can be composed dynamically. A common use case is to compose a query based on user input. 
For example, optional filters on different columns can be applied to a query:

```csharp
public List<Book> GetBooks(string? author, int? year)
{
    IQueryable<Book> query = bookTable.GetRecordView<Book>().AsQueryable();

    if (!string.IsNullOrEmpty(author))
        query = query.Where(x => x.Author == author);
        
    if (year != null)
        query = query.Where(x => x.Year == year);

    return query.ToList();
}
```

### Column Name Mapping

Unless custom mapping is provided with `[Column]`, LINQ provider will use property or field names as column names, 
using unquoted identifiers, which are case-insensitive.

**C#**
```csharp
bookTable.GetRecordView<Book>().AsQueryable().Select(x => x.Author).ToList();
```

**Resulting SQL**
```sql
select _T0.AUTHOR from PUBLIC.books as _T0
```

To use quoted identifiers, or to map column names to different property names, use `[Column]` attribute:

```csharp
public class Book 
{
    [Column("book_author")]
    public string Author { get; set; }
}

// Or a record:
public record Book([property: Column("book_author")] string Author);
```

**Resulting SQL**

```sql
select _T0."book_author" from PUBLIC.books as _T0
```


### KeyValueView

All examples above use `IRecordView<T>` to perform queries; LINQ provider supports `IKeyValueView<TK, TV>` equally well:

```csharp
IQueryable<KeyValuePair<int, Book>> query = bookTable.GetKeyValueView<int, Book>().AsQueryable();

List<Book> books = query
    .Where(x => x.Key > 10)
    .Select(x => x.Value)
    .ToList();
```


## Performance Considerations

Our benchmarks indicate that in real-world scenarios LINQ queries are on par with equivalent SQL queries. 

However, a small overhead still exists (due to query translation), and your mileage may vary depending on the query complexity, 
so it's recommended to measure the performance of your queries.
