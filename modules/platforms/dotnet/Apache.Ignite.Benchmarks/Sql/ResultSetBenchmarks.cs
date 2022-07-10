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
    using Ignite.Table;
    using Tests;

    /// <summary>
    /// Measures SQL result set enumeration with two pages of data (two network requests per enumeration).
    /// <para />
    /// Results on Intel Core i7-9700K, GC = Concurrent Workstation, Ubuntu 20.04:
    /// |          Method |       Runtime |     Mean |    Error |   StdDev | Ratio |   Gen 0 |   Gen 1 | Allocated |
    /// |---------------- |-------------- |---------:|---------:|---------:|------:|--------:|--------:|----------:|
    /// |     ToListAsync |      .NET 6.0 | 259.1 us |  5.16 us |  4.03 us |  0.64 | 64.4531 | 23.9258 |    391 KB |
    /// | AsyncEnumerable |      .NET 6.0 | 361.2 us |  7.22 us |  7.73 us |  0.89 | 64.4531 | 25.3906 |    392 KB |
    /// |     ToListAsync | .NET Core 3.1 | 405.7 us |  8.07 us | 10.50 us |  1.00 | 62.5000 | 23.4375 |    383 KB |
    /// | AsyncEnumerable | .NET Core 3.1 | 543.1 us | 10.82 us | 15.17 us |  1.34 | 62.5000 | 22.4609 |    384 KB |.
    /// </summary>
    [MemoryDiagnoser]
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
