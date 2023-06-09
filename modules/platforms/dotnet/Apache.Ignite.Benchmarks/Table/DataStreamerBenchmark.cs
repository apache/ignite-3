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
/// |           Method |     Mean |    Error |   StdDev | Ratio | RatioSD |   Gen 0 | Allocated |
/// |----------------- |---------:|---------:|---------:|------:|--------:|--------:|----------:|
/// |     DataStreamer | 19.48 ms | 0.148 ms | 0.123 ms |  1.00 |    0.00 |       - |      4 MB |
/// |        UpsertAll | 12.58 ms | 0.292 ms | 0.861 ms |  0.67 |    0.03 | 15.6250 |      4 MB |
/// | UpsertAllBatched | 16.41 ms | 0.308 ms | 0.330 ms |  0.84 |    0.02 |       - |      4 MB |.
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
        _servers = Enumerable.Range(0, ServerCount).Select(_ => new FakeServer(disableOpsTracking: true)).ToList();

        var cfg = new IgniteClientConfiguration();
        foreach (var server in _servers)
        {
            cfg.Endpoints.Add(server.Endpoint);
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
