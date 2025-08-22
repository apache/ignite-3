// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Sql;

using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NodaTime;
using Table;
using Transactions;

public class IgniteDbCommand : DbCommand
{
    private IgniteDbParameterCollection? _parameters;

    /// <inheritdoc />
    public override string CommandText { get; set; }

    /// <inheritdoc />
    public override int CommandTimeout { get; set; }

    /// <inheritdoc />
    public override CommandType CommandType { get; set; }

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <inheritdoc />
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc />
    protected override DbConnection DbConnection { get; set; }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection =>
        _parameters ??= new IgniteDbParameterCollection();

    /// <inheritdoc />
    protected override DbTransaction DbTransaction { get; set; }

    /// <inheritdoc />
    public override void Cancel()
    {
        // TODO: cancellation token
    }

    /// <inheritdoc />
    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        // TODO: DDL does not support transactions, but DML does, we should determine this based on the command type.
        // TODO: Use ExecuteBatch for multiple statements.
        await using IResultSet<object> resultSet = await GetSql().ExecuteAsync<object>(
            transaction: null, // DDL does not support transactions.
            statement,
            cancellationToken,
            args);

        Debug.Assert(!resultSet.HasRowSet, "!resultSet.HasRowSet");

        if (resultSet.AffectedRows < 0)
        {
            return resultSet.WasApplied ? 1 : 0;
        }

        return (int)resultSet.AffectedRows;
    }

    public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        await using IResultSet<IIgniteTuple> resultSet = await GetSql().ExecuteAsync(
            transaction: GetTransaction(),
            statement,
            cancellationToken,
            args);

        await foreach (var row in resultSet)
        {
            // Return the first result.
            return row[0];
        }

        throw new InvalidOperationException("Query returned no results: " + statement);
    }

    public override object ExecuteScalar() => ExecuteScalarAsync().GetAwaiter().GetResult();

    public override void Prepare()
    {
        // No-op.
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        // TODO: Remove debug output.
        Console.WriteLine($"IgniteCommand.ExecuteDbDataReaderAsync [statement={statement}, parameters={string.Join(", ", args)}]");

        return await GetSql().ExecuteReaderAsync(
            GetTransaction(),
            statement,
            cancellationToken,
            args);
    }

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter() => new IgniteDbParameter();

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

    private ISql GetSql()
    {
        if (DbConnection is not IgniteDbConnection igniteConn)
        {
            throw new InvalidOperationException("DbConnection is not an IgniteConnection or is null.");
        }

        var client = igniteConn.Client
                     ?? throw new InvalidOperationException("Ignite client is not initialized (connection is not open).");

        return client.Sql;
    }

    private SqlStatement GetStatement() => new(CommandText);

    private ITransaction? GetTransaction() =>
        DbTransaction is IgniteTransaction igniteTx
            ? igniteTx.InternalTransaction
            : null;

    private object[] GetArgs()
    {
        if (_parameters == null || _parameters.Count == 0)
        {
            return [];
        }

        var arr = new object[_parameters.Count];

        for (var i = 0; i < _parameters.Count; i++)
        {
            // TODO: Review conversion logic for DateTime.
            // TODO: Support NodaTime types.
            arr[i] = _parameters[i].Value switch
            {
                DBNull => null,
                ushort u16 => (short)u16,
                uint u32 => (int)u32,
                DateTime dt => LocalDateTime.FromDateTime(dt),
                var other => other
            };
        }

        return arr;
    }
}
