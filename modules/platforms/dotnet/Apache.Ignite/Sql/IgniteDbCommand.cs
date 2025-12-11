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

namespace Apache.Ignite.Sql;

using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Internal.Common;
using NodaTime;
using Table;
using Transactions;

/// <summary>
/// Ignite database command.
/// </summary>
public sealed class IgniteDbCommand : DbCommand
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    private IgniteDbParameterCollection? _parameters;

    private string _commandText = string.Empty;

    /// <inheritdoc />
    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? string.Empty;
    }

    /// <inheritdoc />
    public override int CommandTimeout { get; set; }

    /// <summary>
    /// Gets or sets the page size (number of rows fetched at a time by the underlying Ignite data reader).
    /// </summary>
    public int PageSize { get; set; } = SqlStatement.DefaultPageSize;

    /// <inheritdoc />
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <inheritdoc />
    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    /// Gets or sets the transaction within which the command executes.
    /// </summary>
    public IgniteDbTransaction? IgniteDbTransaction
    {
        get => (IgniteDbTransaction?)Transaction;
        set => Transaction = value;
    }

    /// <inheritdoc />
    protected override DbConnection? DbConnection { get; set; }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection =>
        _parameters ??= new IgniteDbParameterCollection();

    /// <inheritdoc />
    protected override DbTransaction? DbTransaction { get; set; }

    /// <inheritdoc />
    public override void Cancel() => _cancellationTokenSource.Cancel();

    /// <inheritdoc />
    public override int ExecuteNonQuery() =>
        ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive")]
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _cancellationTokenSource.Token);

        try
        {
            await using IResultSet<object> resultSet = await GetSql().ExecuteAsync<object>(
                transaction: GetIgniteTx(),
                statement,
                linkedCts.Token,
                args).ConfigureAwait(false);

            Debug.Assert(!resultSet.HasRowSet, "!resultSet.HasRowSet");

            if (resultSet.AffectedRows < 0)
            {
                return resultSet.WasApplied ? 1 : 0;
            }

            return (int)resultSet.AffectedRows;
        }
        catch (SqlException sqlEx)
        {
            throw new IgniteDbException(sqlEx.Message, sqlEx);
        }
    }

    /// <inheritdoc />
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive")]
    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _cancellationTokenSource.Token);

        try
        {
            await using IResultSet<IIgniteTuple> resultSet = await GetSql().ExecuteAsync(
                transaction: GetIgniteTx(),
                statement,
                linkedCts.Token,
                args).ConfigureAwait(false);

            await foreach (var row in resultSet)
            {
                // Return the first result.
                return row[0];
            }
        }
        catch (SqlException sqlEx)
        {
            throw new IgniteDbException(sqlEx.Message, sqlEx);
        }

        throw new IgniteDbException("Query returned no results: " + statement);
    }

    /// <inheritdoc />
    public override object? ExecuteScalar() => ExecuteScalarAsync().GetAwaiter().GetResult();

    /// <inheritdoc />
    public override void Prepare() => throw new NotSupportedException("Prepare is not supported.");

    /// <inheritdoc/>
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(CommandText)
            .Append(CommandTimeout)
            .Append(PageSize)
            .Append(Transaction)
            .Append(Connection)
            .Build();

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing)
        {
            _cancellationTokenSource.Dispose();
        }
    }

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var args = GetArgs();
        var statement = GetStatement();

        // Can't create linked CTS here because the returned reader outlasts this method.
        if (cancellationToken == CancellationToken.None)
        {
            cancellationToken = _cancellationTokenSource.Token;
        }

        try
        {
            return await GetSql().ExecuteReaderAsync(
                transaction: GetIgniteTx(),
                statement,
                cancellationToken,
                args).ConfigureAwait(false);
        }
        catch (SqlException sqlEx)
        {
            throw new IgniteDbException(sqlEx.Message, sqlEx);
        }
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

    private SqlStatement GetStatement() => new(CommandText)
    {
        PageSize = PageSize,
        Timeout = TimeSpan.FromSeconds(CommandTimeout) // 0 means no timeout, both in ADO.NET and Ignite.
    };

    private ITransaction? GetIgniteTx() => IgniteDbTransaction?.IgniteTransaction;

    private object?[] GetArgs()
    {
        if (_parameters == null || _parameters.Count == 0)
        {
            return [];
        }

        var arr = new object?[_parameters.Count];

        for (var i = 0; i < _parameters.Count; i++)
        {
            arr[i] = _parameters[i].Value switch
            {
                DBNull => null,
                byte u8 => (sbyte)u8,
                ushort u16 => (short)u16,
                uint u32 => (int)u32,
                DateTime dt => LocalDateTime.FromDateTime(dt),
                var other => other
            };
        }

        return arr;
    }
}
