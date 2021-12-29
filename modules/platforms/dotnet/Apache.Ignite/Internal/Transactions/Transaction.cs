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

namespace Apache.Ignite.Internal.Transactions
{
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Buffers;
    using Ignite.Transactions;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Ignite transaction.
    /// </summary>
    internal class Transaction : ITransaction
    {
        /** Open state. */
        private const int StateOpen = 0;

        /** Committed state. */
        private const int StateCommitted = 1;

        /** Rolled back state. */
        private const int StateRolledBack = 2;

        /** Transaction id. */
        private readonly long _id;

        /** Underlying connection. */
        private readonly ClientSocket _socket;

        /** State. */
        private int _state = StateOpen;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction"/> class.
        /// </summary>
        /// <param name="id">Transaction id.</param>
        /// <param name="socket">Associated connection.</param>
        public Transaction(long id, ClientSocket socket)
        {
            _id = id;
            _socket = socket;
        }

        /// <inheritdoc/>
        public async Task CommitAsync()
        {
            SetState(StateCommitted);

            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            await _socket.DoOutInOpAsync(ClientOp.TxCommit, writer).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task RollbackAsync()
        {
            SetState(StateRolledBack);

            await RollbackAsyncInternal().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            // Roll back if the transaction is still open, otherwise do nothing.
            if (TrySetState(StateRolledBack))
            {
                await RollbackAsyncInternal().ConfigureAwait(false);
            }
        }

        private async Task RollbackAsyncInternal()
        {
            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            await _socket.DoOutInOpAsync(ClientOp.TxRollback, writer).ConfigureAwait(false);
        }

        private bool TrySetState(int state)
        {
            return Interlocked.CompareExchange(ref _state, state, StateOpen) == StateOpen;
        }

        private void SetState(int state)
        {
            var oldState = Interlocked.CompareExchange(ref _state, state, StateOpen);

            if (oldState == StateOpen)
            {
                return;
            }

            var message = oldState == StateCommitted
                ? "Transaction is already committed."
                : "Transaction is already rolled back.";

            throw new TransactionException(message);
        }

        private void Write(MessagePackWriter w)
        {
            w.Write(_id);
            w.Flush();
        }
    }
}
