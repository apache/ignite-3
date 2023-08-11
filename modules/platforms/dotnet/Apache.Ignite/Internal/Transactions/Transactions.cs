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
    using System.Threading.Tasks;
    using Common;
    using Ignite.Transactions;
    using Proto;

    /// <summary>
    /// Transactions API.
    /// </summary>
    internal class Transactions : ITransactions
    {
        /** Underlying connection. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transactions"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public Transactions(ClientFailoverSocket socket)
        {
            _socket = socket;
        }

        /// <inheritdoc/>
        public async Task<ITransaction> BeginAsync(TransactionOptions options)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            // Transaction and all corresponding operations must be performed using the same connection.
            var (resBuf, socket) = await _socket.DoOutInOpAndGetSocketAsync(ClientOp.TxBegin, request: writer).ConfigureAwait(false);

            using (resBuf)
            {
                var txId = resBuf.GetReader().ReadInt64();

                return new Transaction(txId, socket, _socket, options.ReadOnly);
            }

            void Write()
            {
                var w = writer.MessageWriter;
                w.Write(options.ReadOnly);
                w.Write(_socket.ObservableTimestamp);
            }
        }

        /// <inheritdoc />
        public override string ToString() => IgniteToStringBuilder.Build(GetType());
    }
}
