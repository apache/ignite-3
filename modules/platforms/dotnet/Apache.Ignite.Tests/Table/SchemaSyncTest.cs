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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Table;
using Internal.Table;
using NUnit.Framework;

/// <summary>
/// Tests schema synchronization.
/// </summary>
public class SchemaSyncTest : IgniteTestsBase
{
    private string _tempTableName = string.Empty;

    private int _tempTableId;

    private IIgniteClient _tempClient = null!;

    [SetUp]
    [TearDown]
    public async Task DropTempTable()
    {
        if (!string.IsNullOrEmpty(_tempTableName))
        {
            await Client.Sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {_tempTableName}");
            _tempTableName = string.Empty;
        }
    }

    [SetUp]
    public async Task ConnectTempClient() => _tempClient = await IgniteClient.StartAsync(GetConfig());

    [TearDown]
    public void CloseTempClient() => _tempClient.Dispose();

    [Test]
    public async Task TestSchemaUpdateOnGet() =>
        await TestSchemaUpdate(
            async view =>
            {
                var (value, _) = await view.GetAsync(null, new IgniteTuple { ["id"] = 1 });
                Assert.AreEqual("IgniteTuple { ID = 1, VAL = 1 }", value.ToString());
            },
            async view =>
            {
                var (value, _) = await view.GetAsync(null, new IgniteTuple { ["id"] = 1 });
                Assert.AreEqual("IgniteTuple { ID = 1, VAL = 1, VAL2 = 2 }", value.ToString());
            });

    [Test]
    public async Task TestBackgroundSchemaUpdateOnPut() =>
        await TestSchemaUpdate(
            async view => await view.UpsertAsync(null, new IgniteTuple { ["id"] = 1 }),
            async view =>
            {
                // TODO IGNITE-18733: Schema is synchronized in background, so we don't update the new column on first Upsert.
                await view.UpsertAsync(null, new IgniteTuple { ["id"] = 1, ["val2"] = 4 });
                Assert.IsNull(await ExecuteSingleAsync<int?>("select val2 from " + _tempTableName));

                // Second Upsert uses updated schema.
                await view.UpsertAsync(null, new IgniteTuple { ["id"] = 1, ["val2"] = 4 });
                Assert.AreEqual(4, await ExecuteSingleAsync<int?>("select val2 from " + _tempTableName));
            });

    [Test]
    public async Task TestBackgroundSchemaUpdateOnAllOperations()
    {
        await Test(async (view, tuple) => await view.GetAsync(null, tuple));
        await Test(async (view, tuple) => await view.UpsertAsync(null, tuple));

        async Task Test(Func<IRecordView<IIgniteTuple>, IIgniteTuple, Task> action)
        {
            await TestSchemaUpdate(
                async view => await view.UpsertAsync(null, new IgniteTuple { ["id"] = 1 }),
                async view =>
                {
                    await action(view, new IgniteTuple { ["id"] = 1 });

                    var schemas = view
                        .GetFieldValue<ITable>("_table")
                        .GetFieldValue<IDictionary<int, Schema>>("_schemas");

                    TestUtils.WaitForCondition(() => schemas.ContainsKey(2), 1000, () => string.Join(", ", schemas.Keys));
                });
        }
    }

    private async Task TestSchemaUpdate(
        Func<IRecordView<IIgniteTuple>, Task> before,
        Func<IRecordView<IIgniteTuple>, Task> after)
    {
        await CreateTempTable();
        var table = await _tempClient.Tables.GetTableAsync(_tempTableName);

        await before(table!.RecordBinaryView);
        await AddTempTableColumn();
        await after(table.RecordBinaryView);
    }

    private async Task AddTempTableColumn()
    {
        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {_tempTableName} ADD COLUMN val2 int");
        await Client.Sql.ExecuteAsync(null, $"UPDATE {_tempTableName} SET val2 = 2");
    }

    private async Task CreateTempTable()
    {
        _tempTableName = $"{nameof(SchemaSyncTest)}_{Interlocked.Increment(ref _tempTableId)}";

        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {_tempTableName} (id int primary key, val int)");
        await Client.Sql.ExecuteAsync(null, $"INSERT INTO {_tempTableName} (id, val) VALUES (1, 1)");
    }

    private async Task<T> ExecuteSingleAsync<T>(string sql)
    {
        var cursor = await Client.Sql.ExecuteAsync<T>(null, sql);

        return await cursor.SingleAsync();
    }
}
