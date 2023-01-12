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
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Ignite.Table;
    using Ignite.Transactions;
    using Proto;
    using Proto.MsgPack;
    using Serialization;
    using Sql;

    /// <summary>
    /// Table API.
    /// </summary>
    internal sealed class Table : ITable
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** SQL. */
        private readonly Sql _sql;

        /** Schemas. */
        private readonly ConcurrentDictionary<int, Schema> _schemas = new();

        /** Cached record views. */
        private readonly ConcurrentDictionary<Type, object> _recordViews = new();

        /** */
        private readonly object _latestSchemaLock = new();

        /** */
        private readonly SemaphoreSlim _partitionAssignmentSemaphore = new(1);

        /** */
        private volatile int _latestSchemaVersion = -1;

        /** */
        private volatile int _partitionAssignmentVersion = -1;

        /** */
        private volatile string[]? _partitionAssignment;

        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class.
        /// </summary>
        /// <param name="name">Table name.</param>
        /// <param name="id">Table id.</param>
        /// <param name="socket">Socket.</param>
        /// <param name="sql">SQL.</param>
        public Table(string name, Guid id, ClientFailoverSocket socket, Sql sql)
        {
            _socket = socket;
            _sql = sql;

            Name = name;
            Id = id;

            RecordBinaryView = new RecordView<IIgniteTuple>(
                this,
                new RecordSerializer<IIgniteTuple>(this, TupleSerializerHandler.Instance),
                _sql);

            // RecordView and KeyValueView are symmetric and perform the same operations on the protocol level.
            // Only serialization is different - KeyValueView splits records into two parts.
            // Therefore, KeyValueView below simply delegates to RecordView<KvPair>,
            // and SerializerHandler writes KV pair as a single record and reads back record as two parts.
            var pairSerializer = new RecordSerializer<KvPair<IIgniteTuple, IIgniteTuple>>(this, TuplePairSerializerHandler.Instance);

            KeyValueBinaryView = new KeyValueView<IIgniteTuple, IIgniteTuple>(
                new RecordView<KvPair<IIgniteTuple, IIgniteTuple>>(this, pairSerializer, _sql));
        }

        /// <inheritdoc/>
        public string Name { get; }

        /// <inheritdoc/>
        public IRecordView<IIgniteTuple> RecordBinaryView { get; }

        /// <inheritdoc/>
        public IKeyValueView<IIgniteTuple, IIgniteTuple> KeyValueBinaryView { get; }

        /// <summary>
        /// Gets the associated socket.
        /// </summary>
        internal ClientFailoverSocket Socket => _socket;

        /// <summary>
        /// Gets the table id.
        /// </summary>
        internal Guid Id { get; }

        /// <inheritdoc/>
        public IRecordView<T> GetRecordView<T>()
            where T : notnull => GetRecordViewInternal<T>();

        /// <inheritdoc/>
        public IKeyValueView<TK, TV> GetKeyValueView<TK, TV>()
            where TK : notnull
            where TV : notnull =>
            new KeyValueView<TK, TV>(GetRecordViewInternal<KvPair<TK, TV>>());

        /// <summary>
        /// Gets the record view for the specified type.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <returns>Record view.</returns>
        internal RecordView<T> GetRecordViewInternal<T>()
            where T : notnull
        {
            // ReSharper disable once HeapView.CanAvoidClosure (generics prevent this)
            return (RecordView<T>)_recordViews.GetOrAdd(
                typeof(T),
                _ => new RecordView<T>(this, new RecordSerializer<T>(this, new ObjectSerializerHandler<T>()), _sql));
        }

        /// <summary>
        /// Reads the schema.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <returns>Schema or null.</returns>
        internal async ValueTask<Schema?> ReadSchemaAsync(PooledBuffer buf)
        {
            var ver = ReadSchemaVersion(buf);

            if (ver == null)
            {
                return null;
            }

            if (_schemas.TryGetValue(ver.Value, out var res))
            {
                return res;
            }

            return await LoadSchemaAsync(ver).ConfigureAwait(false);

            static int? ReadSchemaVersion(PooledBuffer buf)
            {
                var reader = buf.GetReader();

                return reader.ReadInt32Nullable();
            }
        }

        /// <summary>
        /// Gets the latest schema.
        /// </summary>
        /// <returns>Schema.</returns>
        internal async ValueTask<Schema> GetLatestSchemaAsync()
        {
            var latestSchemaVersion = _latestSchemaVersion;

            if (latestSchemaVersion >= 0)
            {
                return _schemas[latestSchemaVersion];
            }

            return await LoadSchemaAsync(null).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the preferred node by colocation hash.
        /// </summary>
        /// <param name="colocationHash">Colocation hash.</param>
        /// <param name="transaction">Transaction.</param>
        /// <returns>Preferred node.</returns>
        internal async ValueTask<PreferredNode> GetPreferredNode(int colocationHash, ITransaction? transaction)
        {
            if (transaction != null)
            {
                return default;
            }

            var assignment = await GetPartitionAssignmentAsync().ConfigureAwait(false);
            var partition = Math.Abs(colocationHash % assignment.Length);
            var nodeId = assignment[partition];

            return PreferredNode.FromId(nodeId);
        }

        private async ValueTask<string[]> GetPartitionAssignmentAsync()
        {
            var socketVer = _socket.PartitionAssignmentVersion;
            var assignment = _partitionAssignment;

            // Async double-checked locking. Assignment changes rarely, so we avoid the lock if possible.
            if (_partitionAssignmentVersion == socketVer && assignment != null)
            {
                return assignment;
            }

            await _partitionAssignmentSemaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                socketVer = _socket.PartitionAssignmentVersion;
                assignment = _partitionAssignment;

                if (_partitionAssignmentVersion == socketVer && assignment != null)
                {
                    return assignment;
                }

                assignment = await LoadPartitionAssignmentAsync().ConfigureAwait(false);

                _partitionAssignment = assignment;
                _partitionAssignmentVersion = socketVer;

                return assignment;
            }
            finally
            {
                _partitionAssignmentSemaphore.Release();
            }
        }

        /// <summary>
        /// Loads the schema.
        /// </summary>
        /// <param name="version">Version.</param>
        /// <returns>Schema.</returns>
        private async Task<Schema> LoadSchemaAsync(int? version)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.SchemasGet, writer).ConfigureAwait(false);
            return Read();

            void Write()
            {
                var w = writer.MessageWriter;
                w.Write(Id);

                if (version == null)
                {
                    w.WriteNil();
                }
                else
                {
                    w.WriteArrayHeader(1);
                    w.Write(version.Value);
                }

                w.Flush();
            }

            Schema Read()
            {
                var r = resBuf.GetReader();
                var schemaCount = r.ReadMapHeader();

                if (schemaCount == 0)
                {
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Schema not found: " + version);
                }

                Schema last = null!;

                for (var i = 0; i < schemaCount; i++)
                {
                    last = ReadSchema(ref r);
                }

                // Store all schemas in the map, and return last.
                return last;
            }
        }

        /// <summary>
        /// Reads the schema.
        /// </summary>
        /// <param name="r">Reader.</param>
        /// <returns>Schema.</returns>
        private Schema ReadSchema(ref MsgPackReader r)
        {
            var schemaVersion = r.ReadInt32();
            var columnCount = r.ReadArrayHeader();
            var keyColumnCount = 0;

            var columns = new Column[columnCount];

            for (var i = 0; i < columnCount; i++)
            {
                var propertyCount = r.ReadArrayHeader();
                const int expectedCount = 6;

                Debug.Assert(propertyCount >= expectedCount, "propertyCount >= " + expectedCount);

                var name = r.ReadString();
                var type = r.ReadInt32();
                var isKey = r.ReadBoolean();
                var isNullable = r.ReadBoolean();
                var isColocation = r.ReadBoolean(); // IsColocation.
                var scale = r.ReadInt32();

                r.Skip(propertyCount - expectedCount);

                var column = new Column(name, (ClientDataType)type, isNullable, isColocation, isKey, i, scale);

                columns[i] = column;

                if (isKey)
                {
                    keyColumnCount++;
                }
            }

            var schema = new Schema(schemaVersion, keyColumnCount, columns);

            _schemas[schemaVersion] = schema;

            lock (_latestSchemaLock)
            {
                if (schemaVersion > _latestSchemaVersion)
                {
                    _latestSchemaVersion = schemaVersion;
                }
            }

            return schema;
        }

        /// <summary>
        /// Loads the partition assignment.
        /// </summary>
        /// <returns>Partition assignment.</returns>
        private async Task<string[]> LoadPartitionAssignmentAsync()
        {
            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.PartitionAssignmentGet, writer).ConfigureAwait(false);
            return Read();

            void Write()
            {
                var w = writer.MessageWriter;
                w.Write(Id);
                w.Flush();
            }

            string[] Read()
            {
                var r = resBuf.GetReader();
                var count = r.ReadArrayHeader();
                var res = new string[count];

                for (int i = 0; i < count; i++)
                {
                    res[i] = r.ReadString();
                }

                return res;
            }
        }
    }
}
