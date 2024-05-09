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

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Ignite.Transactions;
using Proto;
using Tx;

/// <summary>
/// Lazy Ignite transaction.
/// </summary>
internal sealed class LazyTransaction : ITransaction
{
    /// <summary>
    /// Transaction ID placeholder. Uses MaxValue to reserve bytes in varint format.
    /// </summary>
    public const long TxIdPlaceholder = long.MaxValue;

    /** Open state. */
    private const int StateOpen = 0;

    /** Committed state. */
    private const int StateCommitted = 1;

    /** Rolled back state. */
    private const int StateRolledBack = 2;

    private readonly TransactionOptions _options;

    private int _state = StateOpen;

    private volatile Task<Transaction>? _tx;

    /// <summary>
    /// Initializes a new instance of the <see cref="LazyTransaction"/> class.
    /// </summary>
    /// <param name="options">Options.</param>
    public LazyTransaction(TransactionOptions options) => _options = options;

    /// <inheritdoc/>
    public bool IsReadOnly => _options.ReadOnly;

    /// <summary>
    /// Gets the transaction ID.
    /// </summary>
    internal long Id =>
        _tx is { IsCompleted: true }
            ? _tx.Result.Id
            : TxIdPlaceholder;

    /// <summary>
    /// Gets the transaction state.
    /// </summary>
    internal string State => _state switch
    {
        StateOpen => "Open",
        StateCommitted => "Committed",
        _ => "RolledBack"
    };

    /// <inheritdoc/>
    public async Task CommitAsync()
    {
        if (TrySetState(StateCommitted))
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
    }

    /// <inheritdoc/>
    public async Task RollbackAsync()
    {
        if (TrySetState(StateRolledBack))
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
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await RollbackAsync().ConfigureAwait(false);

    /// <inheritdoc/>
    public override string ToString()
    {
        var builder = new IgniteToStringBuilder(typeof(Transaction));

        builder.Append(Id);
        builder.Append(State);
        builder.Append(IsReadOnly);

        return builder.Build();
    }

    /// <summary>
    /// Ensures that the lazy transaction is actually started on the server.
    /// </summary>
    /// <param name="tx">Lazy transaction.</param>
    /// <param name="socket">Socket.</param>
    /// <param name="preferredNode">Preferred target node.</param>
    /// <returns>Task that will be completed when the transaction is started.</returns>
    internal static async ValueTask<Transaction?> EnsureStartedAsync(ITransaction? tx, ClientFailoverSocket socket, PreferredNode preferredNode) =>
        Get(tx) is { } lazyTx
            ? await lazyTx.EnsureStartedAsync(socket, preferredNode).ConfigureAwait(false)
            : null;

    /// <summary>
    /// Gets the underlying lazy transaction or throws if the transaction type is not supported.
    /// </summary>
    /// <param name="tx">Public transaction.</param>
    /// <returns>Internal lazy transaction.</returns>
    internal static LazyTransaction? Get(ITransaction? tx) => tx switch
    {
        null => null,
        LazyTransaction t => t,
        _ => throw new TransactionException(
            Guid.NewGuid(),
            ErrorGroups.Common.Internal,
            "Unsupported transaction implementation: " + tx.GetType())
    };

    /// <summary>
    /// Ensures that the underlying transaction is actually started on the server.
    /// </summary>
    /// <param name="socket">Socket.</param>
    /// <param name="preferredNode">Preferred target node.</param>
    /// <returns>Task that will be completed when the transaction is started.</returns>
    [SuppressMessage("Reliability", "CA2002:Do not lock on objects with weak identity", Justification = "Reviewed.")]
    private Task<Transaction> EnsureStartedAsync(ClientFailoverSocket socket, PreferredNode preferredNode)
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

    /// <summary>
    /// Attempts to set the specified state.
    /// </summary>
    /// <param name="state">State to set.</param>
    /// <returns>True when specified state was set successfully; false otherwise.</returns>
    private bool TrySetState(int state) => Interlocked.CompareExchange(ref _state, state, StateOpen) == StateOpen;
}
