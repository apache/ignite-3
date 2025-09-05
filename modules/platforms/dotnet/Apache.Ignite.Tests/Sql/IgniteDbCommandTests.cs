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

namespace Apache.Ignite.Tests.Sql;

using System;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteDbCommand"/>.
/// </summary>
public class IgniteDbCommandTests : IgniteTestsBase
{
    private const string TestTable = nameof(IgniteDbCommandTests);

    [TearDown]
    public async Task DropTestTable() =>
        await Client.Sql.ExecuteScriptAsync("DROP TABLE IF EXISTS " + TestTable);

    [Test]
    public async Task TestSelect()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(1, reader.GetInt32(0));
        Assert.IsFalse(await reader.ReadAsync());
    }

    [Test]
    public async Task TestSelectWithParameter()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT @p";

        var param = cmd.CreateParameter();
        param.ParameterName = "p";
        param.Value = 42;
        cmd.Parameters.Add(param);

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(42, reader.GetInt32(0));
        Assert.IsFalse(await reader.ReadAsync());
    }

    [Test]
    public async Task TestPrepareNotSupported()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        Assert.Throws<NotSupportedException>(() => cmd.Prepare());
    }

    [Test]
    public async Task TestDdl()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE TABLE {TestTable} (id INT PRIMARY KEY, val VARCHAR)";

        var result = await cmd.ExecuteNonQueryAsync();
        Assert.AreEqual(-1, result); // DDL returns -1
    }

    [Test]
    public async Task TestDml()
    {
    }
}
