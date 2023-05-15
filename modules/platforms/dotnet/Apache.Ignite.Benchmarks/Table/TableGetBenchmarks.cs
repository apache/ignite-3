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

using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Tests;

/// <summary>
/// Measures simple operation on a fake server - retrieve table id by name. Indicates overall networking performance.
/// <para />
/// Results on i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
/// |   Method |     Mean |    Error |   StdDev | Allocated |
/// |--------- |---------:|---------:|---------:|----------:|
/// | TableGet | 24.22 us | 0.347 us | 0.308 us |      2 KB |.
/// </summary>
[MemoryDiagnoser]
public class TableGetBenchmarks
{
    private FakeServer _server = null!;
    private IIgniteClient _client = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _server = new FakeServer(true);
        _client = await IgniteClient.StartAsync(new IgniteClientConfiguration(_server.Endpoint));
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _client.Dispose();
        _server.Dispose();
    }

    [Benchmark]
    public async Task TableGet() => await _client.Tables.GetTableAsync(FakeServer.ExistingTableName);
}
