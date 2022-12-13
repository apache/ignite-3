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
    using System.Linq;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Jobs;
    using Ignite.Sql;
    using Ignite.Table;
    using Tests;

    /// <summary>
    /// Measures SQL result set enumeration with two pages of data (two network requests per enumeration).
    /// <para />
    /// Results on Intel Core i7-7700HQ, .NET 6, GC = Concurrent Server, Ubuntu 20.04:
    /// |                         Method |     Mean |     Error |    StdDev | Ratio | RatioSD |   Gen 0 | Allocated |
    /// |------------------------------- |---------:|----------:|----------:|------:|--------:|--------:|----------:|
    /// |                    ToListAsync | 2.325 ms | 0.0207 ms | 0.0183 ms |  1.00 |    0.00 | 15.6250 |    392 KB |
    /// |                AsyncEnumerable | 2.487 ms | 0.0202 ms | 0.0189 ms |  1.07 |    0.01 | 15.6250 |    393 KB |
    /// |                LinqToListAsync | 2.206 ms | 0.0121 ms | 0.0113 ms |  0.95 |    0.01 |       - |     80 KB |
    /// | LinqSelectOneColumnToListAsync | 2.171 ms | 0.0278 ms | 0.0246 ms |  0.93 |    0.02 |       - |     57 KB |.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.Net60)]
    public class ResultSetBenchmarks
    {
        private FakeServer? _server;
        private IIgniteClient? _client;
        private IRecordView<Rec>? _recordView;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _server = new FakeServer(disableOpsTracking: true);
            _client = await _server.ConnectClientAsync();

            var table = await _client.Tables.GetTableAsync(FakeServer.ExistingTableName);
            _recordView = table!.GetRecordView<Rec>();
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

        [Benchmark]
        public async Task LinqToListAsync()
        {
            await using var resultSet = await _recordView!.AsQueryable().ToResultSetAsync();

            var rows = await resultSet.ToListAsync();

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        [Benchmark]
        public async Task LinqSelectOneColumnToListAsync()
        {
            await using var resultSet = await _recordView!.AsQueryable().Select(x => x.Id).ToResultSetAsync();

            var rows = await resultSet.ToListAsync();

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        private record Rec(int Id);
    }
}
