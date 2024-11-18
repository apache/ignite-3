namespace Apache.Extensions.Cache.Ignite;

using Apache.Ignite;
using Microsoft.Extensions.Options;

public class IgniteCacheOptions : IOptions<IgniteCacheOptions>
{
    public IgniteClientConfiguration ClientConfiguration { get; set; } = new("localhost");

    /// <inheritdoc/>
    public IgniteCacheOptions Value => this; // TODO: Why does RedisCacheOptions have this?
}
