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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Common;
    using Ignite.Table;
    using Proto;
    using Proto.MsgPack;
    using Sql;

    /// <summary>
    /// Tables API.
    /// </summary>
    internal sealed class Tables : ITables
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** SQL. */
        private readonly Sql _sql;

        /** Cached tables. Caching here is required to retain schema and serializer caches in <see cref="Table"/>. */
        private readonly ConcurrentDictionary<int, Table> _cachedTables = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="Tables"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="sql">Sql.</param>
        public Tables(ClientFailoverSocket socket, Sql sql)
        {
            _socket = socket;
            _sql = sql;
        }

        /// <inheritdoc/>
        public async Task<ITable?> GetTableAsync(string name) =>
            await GetTableAsync(QualifiedName.Parse(name)).ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<ITable?> GetTableAsync(QualifiedName name) =>
            await GetTableInternalAsync(name).ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IList<ITable>> GetTablesAsync()
        {
            // TODO: Switch on features.
            return await _socket.DoWithRetryAsync(
                this,
                static (_, _) => ClientOp.TablesGet,
                async static (socket, tables) =>
                {
                    using var resBuf = await socket.DoOutInOpAsync(ClientOp.TablesGet).ConfigureAwait(false);
                    return Read(resBuf.GetReader(), tables);
                })
                .ConfigureAwait(false);

            static IList<ITable> Read(MsgPackReader r, Tables tables)
            {
                var len = r.ReadInt32();

                var res = new List<ITable>(len);

                for (var i = 0; i < len; i++)
                {
                    var id = r.ReadInt32();
                    var name = r.ReadString();

                    var table = tables._cachedTables.GetOrAdd(
                        id,
                        static (int id0, (string Name, Tables Tables) arg) =>
                            new Table(QualifiedName.Parse(arg.Name), id0, arg.Tables._socket, arg.Tables._sql),
                        (name, tables));

                    res.Add(table);
                }

                return res;
            }
        }

        /// <inheritdoc />
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .AppendList(_cachedTables.Values, "CachedTables")
                .Build();

        /// <summary>
        /// Gets the table by name.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <returns>Table.</returns>
        internal async Task<Table?> GetTableInternalAsync(QualifiedName name)
        {
            IgniteArgumentCheck.NotNull(name);

            // TODO: ClientOp.TABLE_GET_QUALIFIED
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(name.CanonicalName);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TableGet, writer).ConfigureAwait(false);
            return Read(resBuf.GetReader());

            Table? Read(MsgPackReader r)
            {
                if (r.TryReadNil())
                {
                    return null;
                }

                var tableId = r.ReadInt32();
                var actualName = r.ReadString();

                return _cachedTables.GetOrAdd(
                    tableId,
                    static (int id, (string ActualName, Tables Tables) arg) =>
                        new Table(QualifiedName.Parse(arg.ActualName), id, arg.Tables._socket, arg.Tables._sql),
                    (actualName, this));
            }
        }
    }
}
