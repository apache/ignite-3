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

namespace Apache.Ignite.Benchmarks.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Jobs;
    using Ignite.Table;
    using Tests;

    /// <summary>
    /// Measures SQL result set enumeration with two pages of data (two network requests per enumeration).
    /// <para />
    /// Results on Intel Core i7-9700K, GC = Concurrent Server, Ubuntu 20.04:
    /// |          Method |       Runtime |     Mean |   Error |   StdDev | Ratio |  Gen 0 |  Gen 1 | Allocated |
    /// |---------------- |-------------- |---------:|--------:|---------:|------:|-------:|-------:|----------:|
    /// |     ToListAsync |      .NET 6.0 | 218.4 us | 2.14 us |  2.01 us |  0.59 | 8.3008 | 1.7090 |    391 KB |
    /// | AsyncEnumerable |      .NET 6.0 | 325.8 us | 6.14 us |  6.03 us |  0.88 | 8.7891 | 0.4883 |    392 KB |
    /// |     ToListAsync | .NET Core 3.1 | 368.6 us | 7.14 us |  7.01 us |  1.00 | 7.3242 | 1.4648 |    383 KB |
    /// | AsyncEnumerable | .NET Core 3.1 | 497.9 us | 9.59 us | 12.12 us |  1.35 | 7.8125 | 2.9297 |    384 KB |.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.NetCoreApp31, baseline: true)]
    /* [SimpleJob(RuntimeMoniker.Net60)] */
    public class ResultSetBenchmarks
    {
        private FakeServer? _server;
        private IIgniteClient? _client;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _server = new FakeServer(disableOpsTracking: true);
            _client = await _server.ConnectClientAsync();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _server?.Dispose();
            _client?.Dispose();
        }

        [Benchmark(Baseline = true)]
        public async Task ToListAsync()
        {
            await using var resultSet = await _client!.Sql.ExecuteAsync(null, "select 1");
            var rows = await resultSet.ToListAsync();

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        [Benchmark]
        public async Task AsyncEnumerable()
        {
            await using var resultSet = await _client!.Sql.ExecuteAsync(null, "select 1");
            var rows = new List<IIgniteTuple>(1100);

            await foreach (var row in resultSet)
            {
                rows.Add(row);
            }

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }
    }
}
