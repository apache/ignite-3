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
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;
using static Common.Table.TestTables;

/// <summary>
/// Tests for <see cref="IgniteDbCommand"/>.
/// </summary>
[SuppressMessage("ReSharper", "MethodHasAsyncOverload", Justification = "Tests.")]
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

        await using var reader = cmd.ExecuteReader();
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
        cmd.CommandText = "SELECT ?";

        var param = cmd.CreateParameter();
        param.Value = 42;
        cmd.Parameters.Add(param);

        await using var reader = cmd.ExecuteReader();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(42, reader.GetInt32(0));
        Assert.IsFalse(await reader.ReadAsync());
    }

    [Test]
    public async Task TestExecuteScalar()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        var result = cmd.ExecuteScalar();
        Assert.AreEqual(1, result);
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

        var result = cmd.ExecuteNonQuery();
        Assert.AreEqual(1, result);
    }

    [Test]
    public async Task TestDdlWithTxThrows()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE TABLE {TestTable} (id INT PRIMARY KEY, val VARCHAR)";

        await using var transaction = await conn.BeginTransactionAsync();
        cmd.Transaction = transaction;

        var ex = Assert.CatchAsync<DbException>(async () => await cmd.ExecuteNonQueryAsync());
        Assert.AreEqual("DDL doesn't support transactions.", ex?.Message);
    }

    [Test]
    public async Task TestDml([Values(true, false)] bool tx)
    {
        await Client.Sql.ExecuteScriptAsync($"DELETE FROM {TableName}");

        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();

        cmd.CommandText = $"INSERT INTO {TableName} (key, val) VALUES (?, ?)";
        cmd.Parameters.Add(new IgniteDbParameter { Value = 1 });
        cmd.Parameters.Add(new IgniteDbParameter { Value = "dml1" });

        await using var transaction = tx ? await conn.BeginTransactionAsync() : null;
        cmd.Transaction = transaction;

        var result = cmd.ExecuteNonQuery();
        Assert.AreEqual(1, result); // One row inserted

        if (tx)
        {
            // Not visible outside the transaction.
            Assert.IsFalse(await TupleView.ContainsKeyAsync(null, GetTuple(1)));
            transaction?.Commit();
        }

        Assert.AreEqual("dml1", (await TupleView.GetAsync(null, GetTuple(1))).Value["val"]);
    }

    [Test]
    public async Task TestTimeout()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        await using var conn = new IgniteDbConnection(null);
        conn.Open(client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT_FOO";
        cmd.CommandTimeout = 123;

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.AreEqual(123_000, server.LastSqlTimeoutMs);
    }

    [Test]
    public async Task TestExecuteScalarException()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM NON_EXISTENT_TABLE";

        var ex = Assert.Catch<DbException>(() => cmd.ExecuteScalar());
        StringAssert.StartsWith("Failed to validate query", ex.Message);
    }

    [Test]
    public async Task TestExecuteNonQueryException()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO NON_EXISTENT_TABLE (id) VALUES (1)";

        var ex = Assert.Catch<DbException>(() => cmd.ExecuteNonQuery());
        StringAssert.StartsWith("Failed to validate query", ex.Message);
    }

    [Test]
    public async Task TestExecuteReaderException()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM NON_EXISTENT_TABLE";

        var ex = Assert.Catch<DbException>(() => cmd.ExecuteReader());
        StringAssert.StartsWith("Failed to validate query", ex.Message);
    }

    [Test]
    public async Task TestCancel()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 UNION ALL SELECT 1";

        cmd.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(async () => await cmd.ExecuteReaderAsync());
    }

    [Test]
    public async Task TestToString()
    {
        await using var conn = new IgniteDbConnection(null);
        conn.Open(Client);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        cmd.CommandTimeout = 123;
        cmd.Transaction = await conn.BeginTransactionAsync();
        ((IgniteDbCommand)cmd).PageSize = 321;

        var str = cmd.ToString();
        StringAssert.StartsWith(
            "IgniteDbCommand { CommandText = SELECT 1, CommandTimeout = 123, PageSize = 321, " +
            "Transaction = IgniteDbTransaction { IgniteTransaction = Transaction { Id =",
            str);
    }
}
