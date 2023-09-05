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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using Internal.Table;
using NUnit.Framework;

/// <summary>
/// Tests for client table schema synchronization.
/// </summary>
public class SchemaSynchronizationTest : IgniteTestsBase
{
    private static readonly TestMode[] TestModes = Enum.GetValues<TestMode>();

    private static readonly TestMode[] ReadTestModes = { TestMode.One, TestMode.Multiple };

    public enum TestMode
    {
        One,
        Two,
        Multiple,
        Compute
    }

    private static string TestTableName => TestContext.CurrentContext.Test.Name
        .Replace("(", string.Empty)
        .Replace(")", string.Empty);

    [TearDown]
    public async Task DeleteTable() => await Client.Sql.ExecuteAsync(null, $"DROP TABLE {TestTableName}");

    [Test]
    public async Task TestClientUsesLatestSchemaOnWriteDropColumn([ValueSource(nameof(TestModes))] TestMode testMode)
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR NOT NULL)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple
        {
            ["ID"] = 1,
            ["NAME"] = "name"
        };

        await view.InsertAsync(null, rec);

        // Modify table, insert data - client will use old schema, receive error, retry with new schema.
        // The process is transparent for the user: updated schema is in effect immediately.
        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} DROP COLUMN NAME");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        var rec2 = new IgniteTuple
        {
            ["ID"] = 2,
            ["NAME"] = "name2"
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            switch (testMode)
            {
                case TestMode.One:
                    await view.InsertAsync(null, rec2);
                    break;

                case TestMode.Two:
                    await view.ReplaceAsync(null, rec2, rec2);
                    break;

                case TestMode.Multiple:
                    await view.InsertAllAsync(null, new[] { rec2, rec2, rec2 });
                    break;

                case TestMode.Compute:
                    await Client.Compute.ExecuteColocatedAsync<string>(
                        table.Name, rec2, Array.Empty<DeploymentUnit>(), ComputeTests.NodeNameJob);
                    break;

                default:
                    Assert.Fail("Invalid test mode: " + testMode);
                    break;
            }
        });

        StringAssert.StartsWith("Tuple doesn't match schema", ex!.Message);
        StringAssert.EndsWith("extraColumns=NAME", ex.Message);
    }

    [Test]
    public async Task TestClientUsesLatestSchemaOnWriteAddColumn([ValueSource(nameof(TestModes))] TestMode testMode)
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple
        {
            ["ID"] = 1
        };

        await view.InsertAsync(null, rec);

        // Modify table, insert data with new columns - client will validate against old schema, throw error for unmapped columns,
        // then force reload schema and retry.
        // The process is transparent for the user: updated schema is in effect immediately.
        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'name2'");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        var rec2 = new IgniteTuple
        {
            ["ID"] = 2,
            ["NAME"] = "name2"
        };

        switch (testMode)
        {
            case TestMode.One:
                await view.InsertAsync(null, rec2);
                break;

            case TestMode.Two:
                await view.ReplaceAsync(null, rec2, rec2);
                break;

            case TestMode.Multiple:
                await view.InsertAllAsync(null, new[] { rec2, rec2, rec2 });
                break;

            case TestMode.Compute:
                // ExecuteColocated requires key part only.
                await Client.Compute.ExecuteColocatedAsync<string>(
                    table.Name, rec, Array.Empty<DeploymentUnit>(), ComputeTests.NodeNameJob);
                break;

            default:
                Assert.Fail("Invalid test mode: " + testMode);
                break;
        }
    }

    [Test]
    public async Task TestClientUsesLatestSchemaOnRead([ValueSource(nameof(ReadTestModes))] TestMode testMode)
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple { ["ID"] = 1 };
        await view.InsertAsync(null, rec);

        // Modify table, insert data - client will use old schema, receive error, retry with new schema.
        // The process is transparent for the user: updated schema is in effect immediately.
        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'name1'");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        switch (testMode)
        {
            case TestMode.One:
            {
                var res = await view.GetAsync(null, rec);

                Assert.IsTrue(res.HasValue);
                Assert.AreEqual("name1", res.Value["NAME"]);
                break;
            }

            case TestMode.Multiple:
            {
                var res = await view.GetAllAsync(null, new[] { rec, rec });

                Assert.AreEqual(2, res.Count);

                foreach (var r in res)
                {
                    Assert.IsTrue(r.HasValue);
                    Assert.AreEqual("name1", r.Value["NAME"]);
                }

                break;
            }

            default:
                Assert.Fail("Invalid test mode: " + testMode);
                break;
        }
    }

    [Test]
    public async Task TestClientUsesLatestSchemaOnReadPoco([ValueSource(nameof(ReadTestModes))] TestMode testMode)
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple { ["ID"] = 1 };
        await view.InsertAsync(null, rec);

        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'name1'");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        var pocoView = table.GetRecordView<Poco>();
        var poco = new Poco(1, string.Empty);

        switch (testMode)
        {
            case TestMode.One:
            {
                var res = await pocoView.GetAsync(null, new Poco(1, string.Empty));

                Assert.IsTrue(res.HasValue);
                Assert.AreEqual(1, res.Value.Id);
                Assert.AreEqual("name1", res.Value.Name);
                break;
            }

            case TestMode.Multiple:
            {
                var res = await pocoView.GetAllAsync(null, new[] { poco, poco });

                Assert.AreEqual(2, res.Count);

                foreach (var r in res)
                {
                    Assert.IsTrue(r.HasValue);
                    Assert.AreEqual("name1", r.Value.Name);
                }

                break;
            }

            default:
                Assert.Fail("Invalid test mode: " + testMode);
                break;
        }
    }

    [Test]
    public async Task TestClientUsesLatestSchemaOnWritePoco([ValueSource(nameof(TestModes))] TestMode testMode)
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple { ["ID"] = 1 };
        await view.InsertAsync(null, rec);

        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'name1'");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        var pocoView = table.GetRecordView<Poco>();

        switch (testMode)
        {
            case TestMode.One:
                await pocoView.UpsertAsync(null, new Poco(1, "foo"));
                break;

            case TestMode.Two:
                var replaceRes = await pocoView.ReplaceAsync(null, new Poco(1, "foo"), new Poco(1, "foo"));
                Assert.IsFalse(replaceRes);
                break;

            case TestMode.Multiple:
                await pocoView.UpsertAllAsync(null, new[] { new Poco(1, "foo"), new Poco(2, "bar") });
                break;

            case TestMode.Compute:
                await Client.Compute.ExecuteColocatedAsync<string, Poco>(
                    table.Name, new Poco(1, "foo"), Array.Empty<DeploymentUnit>(), ComputeTests.NodeNameJob);
                break;

            default:
                Assert.Fail("Invalid test mode: " + testMode);
                break;
        }

        if (testMode is TestMode.One or TestMode.Multiple)
        {
            var res = await view.GetAsync(null, rec);
            Assert.AreEqual("foo", res.Value["NAME"]);
        }
    }

    [Test]
    public async Task TestClientUsesLatestSchemaOnReadKv()
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple { ["ID"] = 1 };
        await view.InsertAsync(null, rec);

        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'name1'");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        var pocoView = table.GetKeyValueView<int, string>();
        var res = await pocoView.GetAsync(null, 1);

        Assert.IsTrue(res.HasValue);
        Assert.AreEqual("name1", res.Value);
    }

    [Test]
    public async Task TestSchemaUpdateWhileStreaming()
    {
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (KEY bigint PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var options = DataStreamerOptions.Default with { BatchSize = 2 };
        await view.StreamDataAsync(GetData(), options);

        // Inserted with old schema.
        var res1 = await view.GetAsync(null, GetTuple(1));
        Assert.AreEqual("FOO", res1.Value["VAL"]);

        // Inserted with new schema.
        var res2 = await view.GetAsync(null, GetTuple(19));
        Assert.AreEqual("FOO", res2.Value["VAL"]);

        async IAsyncEnumerable<IIgniteTuple> GetData()
        {
            // First set of batches uses old schema.
            for (int i = 0; i < 10; i++)
            {
                yield return GetTuple(i);
            }

            // Update schema.
            // New schema has a new column with a default value, so it is not required to provide it in the streamed data.
            await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN VAL varchar DEFAULT 'FOO'");
            await WaitForNewSchemaOnAllNodes(TestTableName, 2);

            for (int i = 10; i < 20; i++)
            {
                yield return GetTuple(i);
            }
        }
    }

    [Test]
    public async Task TestSchemaUpdateBeforeStreaming()
    {
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (KEY bigint PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        // Insert some data - old schema gets cached.
        await view.InsertAsync(null, GetTuple(-1));

        // Update schema.
        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN VAL varchar DEFAULT 'FOO'");
        await WaitForNewSchemaOnAllNodes(TestTableName, 2);

        // Stream data with new schema. Client does not yet know about the new schema,
        // but unmapped column exception will trigger schema reload.
        await view.StreamDataAsync(new[] { GetTuple(1, "BAR") }.ToAsyncEnumerable());

        // Inserted with old schema.
        var res1 = await view.GetAsync(null, GetTuple(-1));
        Assert.AreEqual("FOO", res1.Value["VAL"]);

        // Inserted with new schema.
        var res2 = await view.GetAsync(null, GetTuple(1));
        Assert.AreEqual("BAR", res2.Value["VAL"]);
    }

    private async Task WaitForNewSchemaOnAllNodes(string tableName, int schemaVer, int timeoutMs = 5000)
    {
        // TODO IGNITE-18733, IGNITE-18449: remove this workaround when issues are fixed.
        // Currently new schema version is not immediately available on all nodes.
        // Use separate client to check schema sync without affecting the system under test.
        var configs = Client.Configuration.Endpoints.Select(e => new IgniteClientConfiguration(e)).ToList();

        foreach (var cfg in configs)
        {
            using var client = await IgniteClient.StartAsync(cfg);
            var table = await client.Tables.GetTableAsync(tableName);
            var tableImpl = (Table)table!;
            var sw = Stopwatch.StartNew();

            while (true)
            {
                var schema = await tableImpl.GetSchemaAsync(Apache.Ignite.Internal.Table.Table.SchemaVersionForceLatest);
                if (schema.Version >= schemaVer)
                {
                    break;
                }

                if (sw.Elapsed > TimeSpan.FromMilliseconds(timeoutMs))
                {
                    Assert.Fail($"Schema version {schema.Version} is not available on node {cfg.Endpoints[0]} after {timeoutMs}ms");
                }

                await Task.Delay(50);
            }
        }
    }

    private record Poco(int Id, string Name);
}
