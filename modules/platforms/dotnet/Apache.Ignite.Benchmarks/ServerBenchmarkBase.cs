namespace Apache.Ignite.Benchmarks;

using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Tests;

public abstract class ServerBenchmarkBase
{
    protected JavaServer JavaServer { get; set; } = null!;
    protected IIgniteClient Client { get; set; } = null!;

    [GlobalSetup]
    public virtual async Task GlobalSetup()
    {
        JavaServer = await JavaServer.StartAsync();
        Client = await IgniteClient.StartAsync(new IgniteClientConfiguration("127.0.0.1:" + JavaServer.Port));
    }

    [GlobalCleanup]
    public virtual void GlobalCleanup()
    {
        Client?.Dispose();
        JavaServer?.Dispose();
    }
}
