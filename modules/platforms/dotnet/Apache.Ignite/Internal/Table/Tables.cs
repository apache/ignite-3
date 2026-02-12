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
            return await _socket.DoWithRetryAsync(
                this,
                static (socket, _) => Op(socket),
                async static (socket, tables) =>
                {
                    var op = Op(socket);
                    using var resBuf = await socket.DoOutInOpAsync(op).ConfigureAwait(false);
                    return Read(resBuf.GetReader(), tables, op);
                })
                .ConfigureAwait(false);

            static IList<ITable> Read(MsgPackReader r, Tables tables, ClientOp op)
            {
                var len = r.ReadInt32();

                var res = new List<ITable>(len);
                bool packedAsQualified = op == ClientOp.TablesGetQualified;

                for (var i = 0; i < len; i++)
                {
                    var id = r.ReadInt32();
                    var qualifiedName = UnpackQualifiedName(ref r, packedAsQualified);

                    var table = tables.GetOrCreateCachedTableInternal(id, qualifiedName);
                    res.Add(table);
                }

                return res;
            }

            static ClientOp Op(ClientSocket? socket) =>
                UseQualifiedNames(socket) ? ClientOp.TablesGetQualified : ClientOp.TablesGet;
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
            return await _socket.DoWithRetryAsync(
                    (Tables: this, Name: name),
                    static (socket, _) => Op(socket),
                    async static (socket, arg) =>
                    {
                        var op = Op(socket);

                        using var writer = ProtoCommon.GetMessageWriter();
                        Write(writer.MessageWriter, op, arg);

                        using var resBuf = await socket.DoOutInOpAsync(op, writer).ConfigureAwait(false);
                        return Read(resBuf.GetReader(), arg.Tables, op);
                    })
                .ConfigureAwait(false);

            static void Write(MsgPackWriter w, ClientOp op, (Tables Tables, QualifiedName Name) arg)
            {
                if (op == ClientOp.TableGetQualified)
                {
                    w.Write(arg.Name.SchemaName);
                    w.Write(arg.Name.ObjectName);
                }
                else
                {
                    w.Write(arg.Name.ObjectName);
                }
            }

            static Table? Read(MsgPackReader r, Tables tables, ClientOp op)
            {
                if (r.TryReadNil())
                {
                    return null;
                }

                var tableId = r.ReadInt32();
                var actualName = UnpackQualifiedName(ref r, op == ClientOp.TableGetQualified);

                return tables.GetOrCreateCachedTableInternal(tableId, actualName);
            }

            static ClientOp Op(ClientSocket? socket) =>
                UseQualifiedNames(socket) ? ClientOp.TableGetQualified : ClientOp.TableGet;
        }

        /// <summary>
        /// Gets or creates a cached table.
        /// </summary>
        /// <param name="id">Table id.</param>
        /// <param name="qualifiedName">Table name.</param>
        /// <returns>Table instance.</returns>
        internal Table GetOrCreateCachedTableInternal(int id, QualifiedName qualifiedName) =>
            _cachedTables.GetOrAdd(
                key: id,
                valueFactory: static (int id0, (QualifiedName QualifiedName, Tables Tables) arg) =>
                    new Table(arg.QualifiedName, id0, arg.Tables._socket, arg.Tables._sql),
                factoryArgument: (qualifiedName, this));

        private static QualifiedName UnpackQualifiedName(ref MsgPackReader r, bool packedAsQualified)
        {
            if (packedAsQualified)
            {
                var schemaName = r.ReadString();
                var objectName = r.ReadString();

                return QualifiedName.FromNormalizedInternal(schemaName, objectName);
            }

            var canonicalName = r.ReadString();

            return QualifiedName.Parse(canonicalName);
        }

        private static bool UseQualifiedNames(ClientSocket? socket) =>
            socket != null && socket.ConnectionContext.ServerHasFeature(ProtocolBitmaskFeature.TableReqsUseQualifiedName);
    }
}
