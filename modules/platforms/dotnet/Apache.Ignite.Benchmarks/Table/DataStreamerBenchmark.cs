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
/// Server imitates work by doing Thread.Sleep based on the batch size, so UpsertAll wins in single-server scenario because it
/// inserts everything in one batch, and streamer sends multiple batches. With multiple servers, streamer scales linearly because
/// it sends batches to different nodes in parallel.
/// <para />
/// Results on i9-12900H, .NET SDK 6.0.408, Ubuntu 22.04:
/// |           Method | ServerCount |      Mean |    Error |   StdDev | Ratio | RatioSD | Allocated |
/// |----------------- |------------ |----------:|---------:|---------:|------:|--------:|----------:|
/// |     DataStreamer |           1 | 141.56 ms | 2.725 ms | 3.244 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           1 | 112.99 ms | 1.203 ms | 1.125 ms |  0.80 |    0.02 |      4 MB |
/// | UpsertAllBatched |           1 | 159.11 ms | 3.175 ms | 4.451 ms |  1.12 |    0.04 |      4 MB |
/// |                  |             |           |          |          |       |         |           |
/// |     DataStreamer |           2 |  67.29 ms | 1.331 ms | 3.058 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           2 | 113.68 ms | 0.915 ms | 0.856 ms |  1.64 |    0.05 |      4 MB |
/// | UpsertAllBatched |           2 | 162.47 ms | 3.169 ms | 5.118 ms |  2.42 |    0.14 |      4 MB |
/// |                  |             |           |          |          |       |         |           |
/// |     DataStreamer |           4 |  32.64 ms | 0.507 ms | 0.475 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           4 | 113.84 ms | 1.276 ms | 1.193 ms |  3.49 |    0.05 |      4 MB |
/// | UpsertAllBatched |           4 | 159.17 ms | 3.148 ms | 5.172 ms |  4.79 |    0.17 |      4 MB |.
/// </summary>
[MemoryDiagnoser]
public class DataStreamerBenchmark
{
    private IList<FakeServer> _servers = null!;
    private IIgniteClient _client = null!;
    private ITable _table = null!;
    private IReadOnlyList<IIgniteTuple> _data = null!;

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

        // 17 partitions per node.
        var partitionAssignment = Enumerable.Range(0, 17).SelectMany(_ => _servers.Select(x => x.Node.Id)).ToArray();

        var cfg = new IgniteClientConfiguration();
        foreach (var server in _servers)
        {
            cfg.Endpoints.Add(server.Endpoint);
            server.PartitionAssignment = partitionAssignment;
        }

        _client = await IgniteClient.StartAsync(cfg);
        _table = (await _client.Tables.GetTableAsync(FakeServer.ExistingTableName))!;
        _data = Enumerable.Range(1, 100_000).Select(x => new IgniteTuple { ["id"] = x }).ToList();
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
        var pageSize = DataStreamerOptions.Default.PageSize;
        var batch = new List<IIgniteTuple>(pageSize);

        foreach (var tuple in _data)
        {
            batch.Add(tuple);

            if (batch.Count == pageSize)
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
