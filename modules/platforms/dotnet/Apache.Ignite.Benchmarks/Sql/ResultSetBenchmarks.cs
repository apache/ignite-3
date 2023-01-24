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
    /// |                         Method |       Mean |    Error |   StdDev | Ratio | RatioSD |   Gen 0 |  Gen 1 | Allocated |
    /// |------------------------------- |-----------:|---------:|---------:|------:|--------:|--------:|-------:|----------:|
    /// |                    ToListAsync |   967.2 us | 18.17 us | 17.00 us |  1.00 |    0.00 | 15.6250 |      - |    392 KB |
    /// |                AsyncEnumerable | 1,172.7 us | 23.41 us | 27.87 us |  1.21 |    0.03 | 15.6250 | 1.9531 |    393 KB |
    /// |                LinqToListAsync |   786.1 us | 14.94 us | 15.34 us |  0.81 |    0.02 |  2.9297 |      - |     80 KB |
    /// | LinqSelectOneColumnToListAsync |   677.0 us | 13.42 us | 11.21 us |  0.70 |    0.02 |  1.9531 |      - |     57 KB |.
    ///
    /// NEW (TODO DESC):
    /// |                         Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 |  Gen 1 | Allocated |
    /// |------------------------------- |---------:|--------:|--------:|------:|--------:|-------:|-------:|----------:|
    /// |                    ToListAsync | 222.2 us | 2.66 us | 2.49 us |  1.00 |    0.00 | 1.4648 | 0.2441 |    392 KB |
    /// |                AsyncEnumerable | 264.1 us | 4.10 us | 3.83 us |  1.19 |    0.02 | 1.4648 | 0.4883 |    393 KB |
    /// |                LinqToListAsync | 181.3 us | 2.48 us | 2.32 us |  0.82 |    0.01 | 0.2441 |      - |     80 KB |
    /// | LinqSelectOneColumnToListAsync | 159.6 us | 2.70 us | 3.41 us |  0.72 |    0.02 | 0.2441 |      - |     57 KB |
    /// |                   DbDataReader | 188.2 us | 1.96 us | 1.83 us |  0.85 |    0.01 |      - |      - |     51 KB |.
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

        [Benchmark]
        public async Task DbDataReader()
        {
            await using var reader = await _client!.Sql.ExecuteReaderAsync(null, "select 1");
            var rows = new List<int>(1100);

            while (await reader.ReadAsync())
            {
                rows.Add(reader.GetInt32(0));
            }

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        private record Rec(int Id);
    }
}
