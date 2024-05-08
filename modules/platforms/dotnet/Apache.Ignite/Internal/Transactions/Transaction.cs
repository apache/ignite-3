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
    using Common;
    using Proto;
    using Proto.MsgPack;

    /// <summary>
    /// Ignite transaction.
    /// </summary>
    internal sealed class Transaction
    {
        /** Open state. */
        private const int StateOpen = 0;

        /** Committed state. */
        private const int StateCommitted = 1;

        /** Rolled back state. */
        private const int StateRolledBack = 2;

        /** State. */
        private int _state = StateOpen;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction"/> class.
        /// </summary>
        /// <param name="id">Transaction id.</param>
        /// <param name="socket">Associated connection.</param>
        /// <param name="failoverSocket">Associated connection multiplexer.</param>
        /// <param name="isReadOnly">Read-only flag.</param>
        public Transaction(long id, ClientSocket socket, ClientFailoverSocket failoverSocket, bool isReadOnly)
        {
            Id = id;
            Socket = socket;
            FailoverSocket = failoverSocket;
            IsReadOnly = isReadOnly;
        }

        /// <summary>
        /// Gets the owner socket.
        /// </summary>
        public ClientSocket Socket { get; }

        /// <summary>
        /// Gets the owner multiplexer socket.
        /// </summary>
        public ClientFailoverSocket FailoverSocket { get; }

        /// <summary>
        /// Gets the transaction id.
        /// </summary>
        public long Id { get; }

        /// <summary>
        /// Gets a value indicating whether the transaction is read-only.
        /// </summary>
        public bool IsReadOnly { get; }

        /// <summary>
        /// Commits the transaction.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task CommitAsync()
        {
            if (TrySetState(StateCommitted))
            {
                using var writer = ProtoCommon.GetMessageWriter();
                Write(writer.MessageWriter);

                using var buffer = await Socket.DoOutInOpAsync(ClientOp.TxCommit, writer).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Commits the transaction.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task RollbackAsync()
        {
            if (TrySetState(StateRolledBack))
            {
                using var writer = ProtoCommon.GetMessageWriter();
                Write(writer.MessageWriter);

                using var buffer = await Socket.DoOutInOpAsync(ClientOp.TxRollback, writer).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            var state = _state switch
            {
                StateOpen => "Open",
                StateCommitted => "Committed",
                _ => "RolledBack"
            };

            var builder = new IgniteToStringBuilder(typeof(Transaction));
            builder.Append(Id);
            builder.Append(state, "State");
            builder.Append(IsReadOnly);

            return builder.Build();
        }

        /// <summary>
        /// Attempts to set the specified state.
        /// </summary>
        /// <param name="state">State to set.</param>
        /// <returns>True when specified state was set successfully; false otherwise.</returns>
        private bool TrySetState(int state) => Interlocked.CompareExchange(ref _state, state, StateOpen) == StateOpen;

        /// <summary>
        /// Writes the transaction.
        /// </summary>
        /// <param name="writer">Writer.</param>
        private void Write(MsgPackWriter writer) => writer.Write(Id);
    }
}
