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
    using System.Threading;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Jobs;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Table.Mapper;
    using Tests;

    /// <summary>
    /// Measures SQL result set enumeration with two pages of data (two network requests per enumeration).
    /// <para />
    /// Results on i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
    /// |                         Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |------------------------------- |---------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// |                    ToListAsync | 229.0 us | 3.69 us | 3.45 us |  1.00 |    0.00 | 1.4648 |    392 KB |
    /// |                AsyncEnumerable | 263.2 us | 5.05 us | 4.72 us |  1.15 |    0.02 | 1.4648 |    393 KB |
    /// |                LinqToListAsync | 180.1 us | 3.18 us | 2.65 us |  0.79 |    0.02 | 0.2441 |     80 KB |
    /// | LinqSelectOneColumnToListAsync | 156.0 us | 3.06 us | 4.48 us |  0.68 |    0.02 | 0.2441 |     57 KB |
    /// |                   DbDataReader | 189.3 us | 1.61 us | 1.51 us |  0.83 |    0.01 |      - |     51 KB |
    /// |       ObjectMappingToListAsync | 165.8 us | 3.28 us | 3.07 us |  0.74 |    0.02 | 0.2441 |     76 KB |
    /// |    PrimitiveMappingToListAsync | 121.2 us | 2.41 us | 4.10 us |  0.54 |    0.02 |      - |     48 KB |.
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

            while (await reader.ReadAsync(CancellationToken.None))
            {
                rows.Add(reader.GetInt32(0));
            }

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        [Benchmark]
        public async Task ObjectMappingToListAsync()
        {
            await using var resultSet = await _client!.Sql.ExecuteAsync<Rec>(null, "select 1");
            var rows = await resultSet.ToListAsync();

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        [Benchmark]
        public async Task ObjectMappingWithMapperToListAsync()
        {
            await using var resultSet = await _client!.Sql.ExecuteAsync(null, new RecMapper(), "select 1", CancellationToken.None);
            var rows = await resultSet.ToListAsync();

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        [Benchmark]
        public async Task PrimitiveMappingToListAsync()
        {
            await using var resultSet = await _client!.Sql.ExecuteAsync<int>(null, "select 1");
            var rows = await resultSet.ToListAsync();

            if (rows.Count != 1012)
            {
                throw new Exception("Wrong count");
            }
        }

        private sealed record Rec(int Id);

        private sealed class RecMapper : IMapper<Rec>
        {
            public void Write(Rec obj, ref RowWriter rowWriter, IMapperSchema schema) =>
                throw new NotImplementedException();

            public Rec Read(ref RowReader rowReader, IMapperSchema schema) =>
                new(rowReader.ReadInt()!.Value);
        }
    }
}
