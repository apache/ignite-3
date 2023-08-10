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

using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for client table schema synchronization.
/// </summary>
public class SchemaSynchronizationTest : IgniteTestsBase
{
    private static string TestTableName => TestContext.CurrentContext.Test.Name;

    [TearDown]
    public async Task DeleteTable() => await Client.Sql.ExecuteAsync(null, $"DROP TABLE {TestTableName}");

    [Test]
    public async Task TestClientUsesLatestSchemaOnWrite()
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
        await view.InsertAsync(null, rec2);
    }
}
