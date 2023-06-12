/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Benchmarks.Table;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Ignite.Table;
using Tests;

/// <summary>
/// Data streamer benchmark.
/// <para />
/// Results on i9-12900H, .NET SDK 6.0.408, Ubuntu 22.04:
/// Delay: 1 ms per 1000 rows
/// |           Method | ServerCount |      Mean |    Error |   StdDev | Ratio | RatioSD | Allocated |
/// |----------------- |------------ |----------:|---------:|---------:|------:|--------:|----------:|
/// |     DataStreamer |           1 | 113.98 ms | 1.000 ms | 0.935 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           1 | 113.13 ms | 1.482 ms | 1.386 ms |  0.99 |    0.02 |      4 MB |
/// | UpsertAllBatched |           1 | 156.66 ms | 3.073 ms | 2.874 ms |  1.37 |    0.03 |      4 MB |
/// |                  |             |           |          |          |       |         |           |
/// |     DataStreamer |           2 |  81.11 ms | 1.062 ms | 0.993 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           2 | 113.55 ms | 1.569 ms | 1.468 ms |  1.40 |    0.03 |      4 MB |
/// | UpsertAllBatched |           2 | 159.01 ms | 3.121 ms | 3.468 ms |  1.96 |    0.05 |      4 MB |
/// |                  |             |           |          |          |       |         |           |
/// |     DataStreamer |           4 |  81.89 ms | 0.729 ms | 0.682 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           4 | 111.83 ms | 1.405 ms | 1.315 ms |  1.37 |    0.02 |      4 MB |
/// | UpsertAllBatched |           4 | 155.45 ms | 2.750 ms | 2.438 ms |  1.90 |    0.03 |      4 MB |.
/// </summary>
[MemoryDiagnoser]
public class DataStreamerBenchmark
{
    private IList<FakeServer> _servers = null!;
    private IIgniteClient _client = null!;
    private ITable _table = null!;
    private IReadOnlyList<IIgniteTuple> _data = null!;

    // TODO: Why no scale up beyond 2 servers? Do we need more per-node operations?
    [Params(1, 2, 4)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Benchmark parameter")]
    public int ServerCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _servers = Enumerable.Range(0, ServerCount)
            .Select(x => new FakeServer(disableOpsTracking: true, nodeName: "server-" + x)
            {
                MultiRowOperationDelayPerRow = TimeSpan.FromMilliseconds(0.001) // 1 ms per 1000 rows
            })
            .ToList();

        // 10 partitions per node.
        var partitionAssignment = Enumerable.Range(1, 10).SelectMany(_ => _servers.Select(x => x.Node.Id)).ToArray();

        var cfg = new IgniteClientConfiguration();
        foreach (var server in _servers)
        {
            cfg.Endpoints.Add(server.Endpoint);
            server.PartitionAssignment = partitionAssignment;
        }

        _client = await IgniteClient.StartAsync(cfg);
        _table = (await _client.Tables.GetTableAsync(FakeServer.ExistingTableName))!;
        _data = Enumerable.Range(1, 100_000).Select(x => new IgniteTuple { ["id"] = x, ["name"] = "name " + x }).ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _client.Dispose();

        foreach (var server in _servers)
        {
            server.Dispose();
        }
    }

    [Benchmark(Baseline = true)]
    public async Task DataStreamer() => await _table.RecordBinaryView.StreamDataAsync(_data.ToAsyncEnumerable());

    [Benchmark]
    public async Task UpsertAll() => await _table.RecordBinaryView.UpsertAllAsync(null, _data);

    [Benchmark]
    public async Task UpsertAllBatched()
    {
        var batchSize = DataStreamerOptions.Default.BatchSize;
        var batch = new List<IIgniteTuple>(batchSize);

        foreach (var tuple in _data)
        {
            batch.Add(tuple);

            if (batch.Count == batchSize)
            {
                await _table.RecordBinaryView.UpsertAllAsync(null, batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await _table.RecordBinaryView.UpsertAllAsync(null, batch);
        }
    }
}
