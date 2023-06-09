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
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Ignite.Table;
using Tests;

/// <summary>
/// Data streamer benchmark.
/// </summary>
[MemoryDiagnoser]
public class DataStreamerSingleNodeBenchmark
{
    private FakeServer _server = null!;
    private IIgniteClient _client = null!;
    private ITable _table = null!;
    private IReadOnlyList<IIgniteTuple> _data = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _server = new FakeServer(true);
        _client = await IgniteClient.StartAsync(new IgniteClientConfiguration(_server.Endpoint));
        _table = (await _client.Tables.GetTableAsync(FakeServer.ExistingTableName))!;
        _data = Enumerable.Range(1, 100_000).Select(x => new IgniteTuple { ["id"] = x, ["name"] = "name " + x }).ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _client.Dispose();
        _server.Dispose();
    }

    [Benchmark]
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
