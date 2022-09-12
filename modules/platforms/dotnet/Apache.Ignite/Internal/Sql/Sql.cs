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

namespace Apache.Ignite.Internal.Sql
{
    using System;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Transactions;
    using Proto;
    using Transactions;

    /// <summary>
    /// SQL API.
    /// </summary>
    internal sealed class Sql : ISql
    {
        /** Underlying connection. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="Sql"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public Sql(ClientFailoverSocket socket)
        {
            _socket = socket;
        }

        /// <inheritdoc/>
        public async Task<IResultSet<IIgniteTuple>> ExecuteAsync(ITransaction? transaction, SqlStatement statement, params object[] args)
        {
            IgniteArgumentCheck.NotNull(statement, nameof(statement));

            var tx = transaction.ToInternal();

            using var bufferWriter = Write();
            var (buf, socket) = await _socket.DoOutInOpAndGetSocketAsync(ClientOp.SqlExec, tx, bufferWriter).ConfigureAwait(false);

            // ResultSet will dispose the pooled buffer.
            return new ResultSet(socket, buf);

            PooledArrayBufferWriter Write()
            {
                var writer = ProtoCommon.GetMessageWriter();
                var w = writer.GetMessageWriter();

                w.WriteTx(tx);
                w.Write(statement.Schema);
                w.Write(statement.PageSize);
                w.Write((long)statement.Timeout.TotalMilliseconds);
                w.WriteNil(); // Session timeout (unused, session is closed by the server immediately).

                w.WriteMapHeader(statement.Properties.Count);

                foreach (var (key, val) in statement.Properties)
                {
                    w.Write(key);
                    w.WriteObjectWithType(val);
                }

                w.Write(statement.Query);

                w.WriteObjectArrayWithTypes(args);

                w.Flush();
                return writer;
            }
        }

        /// <inheritdoc/>
        public Task<IResultSet<T>> ExecuteAsync<T>(ITransaction? transaction, SqlStatement statement, params object[] args)
            where T : class
        {
            // TODO: IGNITE-17333 SQL ResultSet object mapping
            throw new NotSupportedException();
        }
    }
}
