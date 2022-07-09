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
    /// Results on Intel Core i7-9700K, .NET SDK 6.0.301, Ubuntu 20.04:
    /// |          Method |     Mean |     Error |   StdDev |   Gen 0 |   Gen 1 | Allocated |
    /// |---------------- |---------:|----------:|---------:|--------:|--------:|----------:|
    /// |     GetAllAsync | 4.118 ms | 0.5637 ms | 1.662 ms | 62.5000 | 23.4375 |    383 KB |
    /// | AsyncEnumerable | 5.039 ms | 0.6565 ms | 1.936 ms | 62.5000 | 23.4375 |    384 KB |.
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

        [Benchmark]
        public async Task GetAllAsync()
        {
            await using var resultSet = await _client!.Sql.ExecuteAsync(null, "select 1");
            var rows = await resultSet.GetAllAsync();

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
