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
using Tests;

/// <summary>
/// Measures simple operation on a fake server from multiple threads.
/// Demonstrates how client scales with the number of server connections.
/// <para />
/// Results on i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
/// |   Method | ServerCount |     Mean |   Error |  StdDev |
/// |--------- |------------ |---------:|--------:|--------:|
/// | TableGet |           1 | 514.1 ms | 1.29 ms | 1.20 ms |
/// | TableGet |           2 | 259.5 ms | 0.41 ms | 0.39 ms |
/// | TableGet |           4 | 129.7 ms | 0.33 ms | 0.31 ms |.
/// </summary>
public class TableGetMultiThreadedBenchmarks
{
    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Reviewed.")]
    private List<FakeServer> _servers = null!;
    private IIgniteClient _client = null!;

    // ReSharper disable once UnusedAutoPropertyAccessor.Global, MemberCanBePrivate.Global (benchmark parameter).
    [Params(1, 2, 4)]
    public int ServerCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        // Use a delay on server to imitate some work.
        _servers = Enumerable.Range(0, ServerCount)
            .Select(_ => new FakeServer(true) { OperationDelay = TimeSpan.FromMilliseconds(5) })
            .ToList();

        _client = await IgniteClient.StartAsync(new IgniteClientConfiguration(_servers.Select(s => s.Endpoint).ToArray()));
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _client.Dispose();
        _servers.ForEach(s => s.Dispose());
    }

    [Benchmark]
    public void TableGet() =>
        Parallel.For(1, 100, _ => _client.Tables.GetTableAsync(FakeServer.ExistingTableName).GetAwaiter().GetResult());
}
