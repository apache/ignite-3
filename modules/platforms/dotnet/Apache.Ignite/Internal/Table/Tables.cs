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

namespace Apache.Ignite.Internal.Table
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Table;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Tables API.
    /// </summary>
    internal sealed class Tables : ITables
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** Cached tables. Caching here is required to retain schema and serializer caches in <see cref="Table"/>. */
        private readonly ConcurrentDictionary<Guid, Table> _tables = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="Tables"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public Tables(ClientFailoverSocket socket)
        {
            _socket = socket;
        }

        /// <inheritdoc/>
        public async Task<ITable?> GetTableAsync(string name)
        {
            return await GetTableInternalAsync(name).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<IList<ITable>> GetTablesAsync()
        {
            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TablesGet).ConfigureAwait(false);
            return Read(resBuf.GetReader());

            IList<ITable> Read(MessagePackReader r)
            {
                var len = r.ReadMapHeader();

                var res = new List<ITable>(len);

                for (var i = 0; i < len; i++)
                {
                    var id = r.ReadGuid();
                    var name = r.ReadString();

                    // ReSharper disable once LambdaExpressionMustBeStatic (not supported by .NET Core 3.1, TODO IGNITE-16994)
                    var table = _tables.GetOrAdd(
                        id,
                        (Guid id0, (string Name, ClientFailoverSocket Socket) arg) => new Table(arg.Name, id0, arg.Socket),
                        (name, _socket));

                    res.Add(table);
                }

                return res;
            }
        }

        /// <summary>
        /// Gets the table by name.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <returns>Table.</returns>
        internal async Task<Table?> GetTableInternalAsync(string name)
        {
            IgniteArgumentCheck.NotNull(name, nameof(name));

            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TableGet, writer).ConfigureAwait(false);
            return Read(resBuf.GetReader());

            void Write(MessagePackWriter w)
            {
                w.Write(name);
                w.Flush();
            }

            // ReSharper disable once LambdaExpressionMustBeStatic (requires .NET 5+)
            Table? Read(MessagePackReader r) =>
                r.NextMessagePackType == MessagePackType.Nil
                    ? null
                    : _tables.GetOrAdd(
                        r.ReadGuid(),
                        (Guid id, (string Name, ClientFailoverSocket Socket) arg) => new Table(arg.Name, id, arg.Socket),
                        (name, _socket));
        }
    }
}
