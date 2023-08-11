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
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
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
    public async Task TestClientUsesLatestSchemaOnWrite([ValueSource(nameof(TestModes))] TestMode testMode)
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

        var rec2 = new IgniteTuple
        {
            ["ID"] = 2,
            ["NAME"] = "name2"
        };

        // TODO this should fail when we implement IGNITE-19836 Reject Tuples and POCOs with unmapped fields
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

        StringAssert.StartsWith("Record doesn't match schema", ex!.Message);
        StringAssert.EndsWith("extraColumns=NAME (Parameter 'record')", ex.Message);
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
    public async Task TestClientUsesLatestSchemaOnReadPoco()
    {
        // Create table, insert data.
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TestTableName} (ID INT NOT NULL PRIMARY KEY)");

        var table = await Client.Tables.GetTableAsync(TestTableName);
        var view = table!.RecordBinaryView;

        var rec = new IgniteTuple { ["ID"] = 1 };
        await view.InsertAsync(null, rec);

        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TestTableName} ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'name1'");

        var pocoView = table.GetRecordView<Poco>();
        var res = await pocoView.GetAsync(null, new Poco(1, string.Empty));

        Assert.IsTrue(res.HasValue);
        Assert.AreEqual(1, res.Value.Id);
        Assert.AreEqual("name1", res.Value.Name);
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

        var pocoView = table.GetKeyValueView<int, string>();
        var res = await pocoView.GetAsync(null, 1);

        Assert.IsTrue(res.HasValue);
        Assert.AreEqual("name1", res.Value);
    }

    private record Poco(int Id, string Name);
}
