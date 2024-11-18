namespace Apache.Extensions.Cache.Ignite;

using Apache.Ignite;
using Microsoft.Extensions.Options;

public class IgniteDistributedCacheOptions : IOptions<IgniteDistributedCacheOptions>
{
    public IgniteClientConfiguration ClientConfiguration { get; set; } = new("localhost");

    /// <inheritdoc/>
    public IgniteDistributedCacheOptions Value => this; // TODO: Why does RedisCacheOptions have this?
}
