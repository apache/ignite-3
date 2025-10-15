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

using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Internal.Common;
using Transactions;

/// <summary>
/// Ignite database transaction.
/// </summary>
public sealed class IgniteDbTransaction : DbTransaction
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbTransaction"/> class.
    /// </summary>
    /// <param name="tx">Underlying Ignite transaction.</param>
    /// <param name="isolationLevel">Isolation level.</param>
    /// <param name="connection">Connection.</param>
    public IgniteDbTransaction(ITransaction tx, IsolationLevel isolationLevel, DbConnection connection)
    {
        IgniteTransaction = tx;
        IsolationLevel = isolationLevel;
        DbConnection = connection;
    }

    /// <inheritdoc />
    public override IsolationLevel IsolationLevel { get; }

    /// <inheritdoc />
    public override bool SupportsSavepoints => false;

    /// <summary>
    /// Gets the underlying Ignite transaction.
    /// </summary>
    public ITransaction IgniteTransaction { get; }

    /// <inheritdoc />
    protected override DbConnection DbConnection { get; }

    /// <inheritdoc />
    public override void Commit() => CommitAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override void Rollback() => RollbackAsync(null!, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await IgniteTransaction.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task CommitAsync(CancellationToken cancellationToken = default) =>
        await IgniteTransaction.CommitAsync().ConfigureAwait(false);

    /// <inheritdoc />
    public override async Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default) =>
        await IgniteTransaction.RollbackAsync().ConfigureAwait(false);

    /// <inheritdoc/>
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(IgniteTransaction)
            .Append(Connection)
            .Build();

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        IgniteTransaction.Dispose();
        base.Dispose(disposing);
    }
}
