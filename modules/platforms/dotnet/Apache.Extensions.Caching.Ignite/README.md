# Apache Ignite Caching Extensions

Distributed cache implementation of `Microsoft.Extensions.Caching.Distributed.IDistributedCache` using Apache Ignite 3 distributed database.

Add to your services:

```csharp
services
    .AddIgniteClientGroup(new IgniteClientGroupConfiguration
    {
        ClientConfiguration = new IgniteClientConfiguration("localhost")
    })
    .AddIgniteDistributedCache(options => options.TableName = "IGNITE_DISTRIBUTED_CACHE");
```

`AddIgniteClientGroup` is required for the cache to work. It is used to create a connection to the Ignite cluster.

## Configuration
