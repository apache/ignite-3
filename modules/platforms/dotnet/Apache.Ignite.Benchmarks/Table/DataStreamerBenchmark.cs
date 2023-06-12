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
/// |                          Method | ServerCount |     Mean |    Error |   StdDev |   Median | Ratio | RatioSD |   Gen 0 | Allocated |
/// |-------------------------------- |------------ |---------:|---------:|---------:|---------:|------:|--------:|--------:|----------:|
/// |                    DataStreamer |           1 | 20.63 ms | 0.157 ms | 0.131 ms | 20.64 ms |  1.00 |    0.00 |       - |      4 MB |
/// |                       UpsertAll |           1 | 12.90 ms | 0.345 ms | 1.016 ms | 13.13 ms |  0.63 |    0.05 |       - |      4 MB |
/// |                UpsertAllBatched |           1 | 16.78 ms | 0.325 ms | 0.477 ms | 16.83 ms |  0.81 |    0.03 |       - |      4 MB |
/// | UpsertAllBatchedAsyncEnumerable |           1 | 22.62 ms | 0.429 ms | 0.459 ms | 22.72 ms |  1.09 |    0.03 |       - |      4 MB |
/// |                                 |             |          |          |          |          |       |         |         |           |
/// |                    DataStreamer |           2 | 19.92 ms | 0.292 ms | 0.259 ms | 19.90 ms |  1.00 |    0.00 |       - |      4 MB |
/// |                       UpsertAll |           2 | 12.97 ms | 0.368 ms | 1.084 ms | 13.26 ms |  0.63 |    0.07 | 15.6250 |      4 MB |
/// |                UpsertAllBatched |           2 | 17.10 ms | 0.334 ms | 0.627 ms | 17.06 ms |  0.87 |    0.03 |       - |      4 MB |
/// | UpsertAllBatchedAsyncEnumerable |           2 | 23.32 ms | 0.450 ms | 0.500 ms | 23.41 ms |  1.18 |    0.03 |       - |      4 MB |
/// |                                 |             |          |          |          |          |       |         |         |           |
/// |                    DataStreamer |           4 | 20.01 ms | 0.396 ms | 0.407 ms | 19.98 ms |  1.00 |    0.00 |       - |      4 MB |
/// |                       UpsertAll |           4 | 13.36 ms | 0.330 ms | 0.973 ms | 13.43 ms |  0.66 |    0.06 | 15.6250 |      4 MB |
/// |                UpsertAllBatched |           4 | 16.40 ms | 0.319 ms | 0.327 ms | 16.43 ms |  0.82 |    0.03 |       - |      4 MB |
/// | UpsertAllBatchedAsyncEnumerable |           4 | 23.42 ms | 0.466 ms | 0.992 ms | 23.30 ms |  1.20 |    0.03 |       - |      4 MB |.
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

    [Params(0, 5)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Benchmark parameter")]
    public int OperationDelayMs { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _servers = Enumerable.Range(0, ServerCount)
            .Select(_ => new FakeServer(disableOpsTracking: true) { OperationDelay = TimeSpan.FromMilliseconds(OperationDelayMs) })
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

    [Benchmark]
    public async Task UpsertAllBatchedAsyncEnumerable()
    {
        var batchSize = DataStreamerOptions.Default.BatchSize;
        var batch = new List<IIgniteTuple>(batchSize);

        // Use async enumerable for a fair comparison with DataStreamer.
        await foreach (var tuple in _data.ToAsyncEnumerable())
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
