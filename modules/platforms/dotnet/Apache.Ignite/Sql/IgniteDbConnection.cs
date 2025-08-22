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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Internal.Common;

/// <summary>
/// Ignite database connection.
/// </summary>
public sealed class IgniteDbConnection : DbConnection
{
    private IIgniteClient? _igniteClient;

    private string _connectionString = string.Empty;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbConnection"/> class.
    /// </summary>
    /// <param name="connectionString">Connection string.</param>
    public IgniteDbConnection(string? connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbConnection"/> class.
    /// </summary>
    /// <param name="igniteClient">Ignite client.</param>
    public IgniteDbConnection(IIgniteClient igniteClient)
    {
        IgniteArgumentCheck.NotNull(igniteClient);
        _igniteClient = igniteClient;
    }

    /// <inheritdoc />
    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (State != ConnectionState.Closed)
            {
                throw new InvalidOperationException("Cannot set ConnectionString while the connection is open.");
            }

            _connectionString = value ?? string.Empty;
        }
    }

    /// <inheritdoc />
    public override string Database => string.Empty;

    /// <inheritdoc />
    public override ConnectionState State => _igniteClient == null
        ? ConnectionState.Closed
        : ConnectionState.Open;

    /// <inheritdoc />
    public override string DataSource => string.Empty;

    /// <inheritdoc />
    public override string ServerVersion => "TODO"; // TODO: Set once connected - there is a ticket - IGNITE-25936

    /// <summary>
    /// Gets the underlying Ignite client instance, or null if the connection is not open.
    /// </summary>
    public IIgniteClient? Client => _igniteClient;

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("Changing database is not supported in Ignite.");
    }

    /// <inheritdoc />
    public override void Close()
    {
        _igniteClient?.Dispose();
        _igniteClient = null;
    }

    /// <inheritdoc />
    public override void Open() => OpenAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        var connStrBuilder = new IgniteDbConnectionStringBuilder(ConnectionString);
        IgniteClientConfiguration cfg = connStrBuilder.ToIgniteClientConfiguration();

        _igniteClient ??= await IgniteClient.StartAsync(cfg).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        BeginDbTransactionAsync(isolationLevel, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc />
    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
        IsolationLevel isolationLevel,
        CancellationToken cancellationToken)
    {
        var tx = await _igniteClient!.Transactions.BeginAsync().ConfigureAwait(false);

        return new IgniteDbTransaction(tx, isolationLevel, this);
    }

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand() => new IgniteDbCommand
    {
        Connection = this
    };

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }

        base.Dispose(disposing);
    }
}
