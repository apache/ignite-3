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

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

public sealed class IgniteConnection : DbConnection
{
    private readonly IgniteClientConfiguration _config;

    private IIgniteClient? _igniteClient;

    public IgniteConnection(string connectionString)
    {
        // TODO: Parse connection string?
        _config = new IgniteClientConfiguration(connectionString);
        ConnectionString = connectionString;
    }

    public override string ConnectionString { get; set; }

    public override string Database => string.Empty;

    public override ConnectionState State => _igniteClient == null ? ConnectionState.Closed : ConnectionState.Open;

    public override string DataSource => string.Empty;

    // TODO: Set once connected.
    public override string ServerVersion { get; }

    public int DefaultTimeout { get; set; }

    public IIgniteClient? Client => _igniteClient;

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    public override void Close()
    {
        _igniteClient?.Dispose();
        _igniteClient = null;
    }

    public override void Open() => OpenAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        _igniteClient ??= await IgniteClient.StartAsync(_config);
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        BeginDbTransactionAsync(isolationLevel, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
        IsolationLevel isolationLevel,
        CancellationToken cancellationToken)
    {
        var tx = await _igniteClient!.Transactions.BeginAsync();

        return new IgniteTransaction(tx, isolationLevel, this);
    }

    protected override DbCommand CreateDbCommand() => new IgniteCommand
    {
        Connection = this,
        CommandTimeout = DefaultTimeout
    };

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }

        base.Dispose(disposing);
    }
}
