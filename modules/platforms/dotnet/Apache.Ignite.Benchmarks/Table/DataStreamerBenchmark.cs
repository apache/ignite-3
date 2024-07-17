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
/// Results on i9-12900H, .NET SDK 6.0.421, Ubuntu 22.04:
/// |           Method | ServerCount |      Mean |    Error |   StdDev | Ratio | RatioSD | Allocated |
/// |----------------- |------------ |----------:|---------:|---------:|------:|--------:|----------:|
/// |     DataStreamer |           1 | 109.33 ms | 0.805 ms | 0.753 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           1 | 112.34 ms | 1.060 ms | 0.991 ms |  1.03 |    0.01 |      4 MB |
/// | UpsertAllBatched |           1 | 158.85 ms | 3.115 ms | 5.374 ms |  1.44 |    0.06 |      4 MB |
/// |                  |             |           |          |          |       |         |           |
/// |     DataStreamer |           2 |  56.03 ms | 0.619 ms | 0.579 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           2 | 112.38 ms | 1.527 ms | 1.428 ms |  2.01 |    0.03 |      4 MB |
/// | UpsertAllBatched |           2 | 162.67 ms | 2.833 ms | 3.149 ms |  2.91 |    0.07 |      4 MB |
/// |                  |             |           |          |          |       |         |           |
/// |     DataStreamer |           4 |  43.86 ms | 0.528 ms | 0.494 ms |  1.00 |    0.00 |      4 MB |
/// |        UpsertAll |           4 | 113.32 ms | 0.880 ms | 0.823 ms |  2.58 |    0.04 |      4 MB |
/// | UpsertAllBatched |           4 | 164.51 ms | 3.220 ms | 3.446 ms |  3.76 |    0.10 |      4 MB |.
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
