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

namespace Apache.Ignite.EntityFrameworkCore.DataCommon;

#nullable disable // TODO: Remove nullable disable.
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Sql;
using Table;
using Transactions;

public class IgniteCommand : DbCommand
{
    private IgniteParameterCollection _parameters;

    public override string CommandText { get; set; }

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    public override bool DesignTimeVisible { get; set; }

    public CommandSource CommandSource { get; set; }

    protected override DbConnection DbConnection { get; set; }

    protected override DbParameterCollection DbParameterCollection =>
        _parameters ??= new IgniteParameterCollection();

    protected override DbTransaction DbTransaction { get; set; }

    public override void Cancel()
    {
        // No-op.
    }

    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        // TODO: Remove debug output.
        Console.WriteLine($"IgniteCommand.ExecuteNonQueryAsync [statement={statement}, parameters={string.Join(", ", args)}]");

        await using IResultSet<object> resultSet = await GetSql().ExecuteAsync<object>(
            transaction: GetTransaction(),
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

        // TODO: Remove debug output.
        Console.WriteLine($"IgniteCommand.ExecuteScalarAsync [statement={statement}, parameters={string.Join(", ", args)}]");

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
        if (CommandSource == CommandSource.SaveChanges)
        {
            // Ignite-specific: SaveChangesDataReader is used to return the number of rows affected.
            var rowsAffected = await ExecuteNonQueryAsync(cancellationToken);

            return new IgniteSaveChangesDataReader(rowsAffected);
        }

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

    protected override DbParameter CreateDbParameter() => new IgniteParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }

    private ISql GetSql()
    {
        if (DbConnection is not IgniteConnection igniteConn)
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

    private object[] GetArgs() => _parameters?.ToObjectArray() ?? [];
}
