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
using NUnit.Framework;

/// <summary>
/// Tests schema synchronization.
/// </summary>
public class SchemaSyncTest : IgniteTestsBase
{
    private const string TempTableName = nameof(SchemaSyncTest);

    [TearDown]
    public async Task DropTempTable() => await Client.Sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {TempTableName}");

    [Test]
    public async Task TestSchemaUpdateWithSql()
    {
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE IF NOT EXISTS {TempTableName} (id int primary key, val int)");
        await Client.Sql.ExecuteAsync(null, $"INSERT INTO {TempTableName} (id, val) VALUES (1, 1)");
        await Client.Sql.ExecuteAsync(null, $"ALTER TABLE {TempTableName} ADD COLUMN val2 int");
        await Client.Sql.ExecuteAsync(null, $"UPDATE {TempTableName} SET val2 = 2");
    }
}
