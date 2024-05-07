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

namespace Apache.Ignite.Internal.Transactions;

using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Ignite.Transactions;
using Proto;

/// <summary>
/// Lazy Ignite transaction.
/// </summary>
internal sealed class LazyTransaction : ITransaction
{
    private readonly TransactionOptions _options;

    private volatile Task<Transaction>? _tx;

    /// <summary>
    /// Initializes a new instance of the <see cref="LazyTransaction"/> class.
    /// </summary>
    /// <param name="options">Options.</param>
    public LazyTransaction(TransactionOptions options)
    {
        _options = options;
    }

    /// <inheritdoc/>
    public bool IsReadOnly => _options.ReadOnly;

    /// <inheritdoc/>
    public async Task CommitAsync()
    {
        var txTask = _tx;

        if (txTask == null)
        {
            // No operations were performed, nothing to commit.
            return;
        }

        var tx = await txTask.ConfigureAwait(false);
        await tx.CommitAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task RollbackAsync()
    {
        var txTask = _tx;

        if (txTask == null)
        {
            // No operations were performed, nothing to roll back.
            return;
        }

        var tx = await txTask.ConfigureAwait(false);
        await tx.RollbackAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await RollbackAsync().ConfigureAwait(false);

    /// <summary>
    /// Ensures that the underlying transaction is actually started on the server.
    /// </summary>
    /// <param name="socket">Socket.</param>
    /// <param name="preferredNode">Preferred target node.</param>
    /// <returns>Task that will be completed when the transaction is started.</returns>
    [SuppressMessage("Reliability", "CA2002:Do not lock on objects with weak identity", Justification = "Reviewed.")]
    internal Task<Transaction> EnsureStarted(ClientFailoverSocket socket, PreferredNode preferredNode)
    {
        lock (this)
        {
            var txTask = _tx;

            if (txTask != null)
            {
                return txTask;
            }

            txTask = BeginAsync(socket, preferredNode);
            _tx = txTask;

            return txTask;
        }
    }

    private async Task<Transaction> BeginAsync(ClientFailoverSocket failoverSocket, PreferredNode preferredNode)
    {
        using var writer = ProtoCommon.GetMessageWriter();
        Write();

        // Transaction and all corresponding operations must be performed using the same connection.
        var (resBuf, socket) = await failoverSocket.DoOutInOpAndGetSocketAsync(
            ClientOp.TxBegin, request: writer, preferredNode: preferredNode).ConfigureAwait(false);

        using (resBuf)
        {
            var txId = resBuf.GetReader().ReadInt64();

            return new Transaction(txId, socket, failoverSocket, _options.ReadOnly);
        }

        void Write()
        {
            var w = writer.MessageWriter;
            w.Write(_options.ReadOnly);
            w.Write(failoverSocket.ObservableTimestamp);
        }
    }
}
