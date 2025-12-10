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
using System.Threading;
using System.Threading.Tasks;
using Common;
using Ignite.Transactions;
using Microsoft.Extensions.Logging;
using Proto;
using Tx;

/// <summary>
/// Lazy Ignite transaction.
/// </summary>
internal sealed class LazyTransaction : ITransaction
{
    /// <summary>
    /// Transaction ID placeholder. Uses MaxValue to reserve bytes in varint format.
    /// It will also work correctly if the actual tx id matches the placeholder.
    /// </summary>
    public const long TxIdPlaceholder = long.MaxValue;

    private const int StateOpen = 0;

    private const int StateCommitted = 1;

    private const int StateRolledBack = 2;

    private readonly object _syncRoot = new();

    private readonly TransactionOptions _options;

    private readonly long _observableTimestamp;

    private readonly ILogger<LazyTransaction> _logger;

    private int _state = StateOpen;

    private volatile Task<Transaction>? _tx;

    /// <summary>
    /// Initializes a new instance of the <see cref="LazyTransaction"/> class.
    /// </summary>
    /// <param name="options">Options.</param>
    /// <param name="observableTimestamp">Observable timestamp.</param>
    /// <param name="logger">Logger.</param>
    public LazyTransaction(TransactionOptions options, long observableTimestamp, ILogger<LazyTransaction> logger)
    {
        _options = options;
        _observableTimestamp = observableTimestamp;
        _logger = logger;
    }

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
    private string State => _state switch
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
            await DoOpAsync(_tx, ClientOp.TxCommit).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async Task RollbackAsync()
    {
        if (TrySetState(StateRolledBack))
        {
            await DoOpAsync(_tx, ClientOp.TxRollback).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await RollbackAsync().ConfigureAwait(false);

    /// <inheritdoc/>
    public void Dispose()
    {
        // It is recommended to implement IDisposable when IAsyncDisposable is implemented,
        // so we have to do sync-over-async here.
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

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
    /// Gets a value indicating whether the underlying lazy transaction is started.
    /// </summary>
    /// <param name="tx">Transaction.</param>
    /// <returns>True when the underlying lazy transaction is started, false otherwise.</returns>
    internal static bool IsStarted(ITransaction? tx) => Get(tx)?._tx != null;

    private static async Task DoOpAsync(Task<Transaction>? txTask, ClientOp op)
    {
        if (txTask == null)
        {
            // No operations were performed, nothing to commit or roll back.
            return;
        }

        var tx = await txTask.ConfigureAwait(false);

        using var writer = ProtoCommon.GetMessageWriter();
        writer.MessageWriter.Write(tx.Id);
        using var buffer = await tx.Socket.DoOutInOpAsync(op, writer).ConfigureAwait(false);
    }

    private Task<Transaction> EnsureStartedAsync(ClientFailoverSocket socket, PreferredNode preferredNode)
    {
        lock (_syncRoot)
        {
            var txTask = _tx;

            if (txTask != null)
            {
                return txTask;
            }

            txTask = BeginAsync(socket, preferredNode, _observableTimestamp);
            _tx = txTask;

            return txTask;
        }
    }

    private async Task<Transaction> BeginAsync(ClientFailoverSocket failoverSocket, PreferredNode preferredNode, long observableTimestamp)
    {
        using var writer = ProtoCommon.GetMessageWriter();
        Write();

        // Transaction and all corresponding operations must be performed using the same connection.
        var (resBuf, socket) = await failoverSocket.DoOutInOpAndGetSocketAsync(
            ClientOp.TxBegin, request: writer, preferredNode: preferredNode).ConfigureAwait(false);

        using (resBuf)
        {
            var txId = resBuf.GetReader().ReadInt64();

            return new Transaction(txId, socket, failoverSocket);
        }

        void Write()
        {
            var w = writer.MessageWriter;
            w.Write(_options.ReadOnly);
            w.Write(_options.TimeoutMillis);
            w.Write(observableTimestamp);
        }
    }

    private bool TrySetState(int state) => Interlocked.CompareExchange(ref _state, state, StateOpen) == StateOpen;
}
