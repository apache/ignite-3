# Apache Ignite Caching Extensions

Distributed cache implementation of `Microsoft.Extensions.Caching.Distributed.IDistributedCache` using Apache Ignite 3 database.

Add to your services:

```csharp
services
    .AddIgniteClientGroup(new IgniteClientGroupConfiguration
    {
        ClientConfiguration = new IgniteClientConfiguration("localhost")
    })
    .AddIgniteDistributedCache(options => options.TableName = "IGNITE_DISTRIBUTED_CACHE");
```

* `AddIgniteClientGroup` is required for the cache to work. It is used to create a connection to the Ignite cluster.
* Ignite table will be created automatically if it does not exist, with columns: `KEY VARCHAR PRIMARY KEY, VAL VARBINARY`.

## Configuration

`IgniteDistributedCacheOptions` has the following properties:
* `TableName` - name of the table in Ignite where the cache is stored. Default is `IGNITE_DOTNET_DISTRIBUTED_CACHE`.
* `KeyColumnName` - name of the column in the table where the cache keys are stored. Default is `KEY`.
* `ValueColumnName` - name of the column in the table where the cache values are stored. Default is `VAL`.
* `CacheKeyPrefix` - optional prefix for the cache keys. Default is `null`.
* `IgniteClientGroupServiceKey` - optional service collection key to resolve an `IIgniteClientGroup` instance. Default is `null`. 
