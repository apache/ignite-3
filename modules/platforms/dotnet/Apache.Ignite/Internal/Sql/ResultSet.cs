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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Ignite.Sql;
    using Ignite.Table;
    using MessagePack;
    using Proto;

    /// <summary>
    /// SQL result set.
    /// </summary>
    internal sealed class ResultSet : IResultSet<IIgniteTuple>, IAsyncEnumerator<IIgniteTuple>
    {
        private record BufferHolder(PooledBuffer Buffer, int Offset);

        private readonly ClientSocket _socket;

        private readonly long? _resourceId;

        private volatile bool _hasMorePages;

        private volatile bool _closed;

        private volatile BufferHolder? _buffer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ResultSet"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="buf">Buffer to read initial data from.</param>
        public ResultSet(ClientSocket socket, PooledBuffer buf)
        {
            _socket = socket;

            var reader = buf.GetReader();

            _resourceId = reader.TryReadNil() ? null : reader.ReadInt64();

            HasRowSet = reader.ReadBoolean();
            _hasMorePages = reader.ReadBoolean();
            WasApplied = reader.ReadBoolean();
            AffectedRows = reader.ReadInt64();

            Metadata = ReadMeta(ref reader);

            if (HasRowSet)
            {
                _buffer = new BufferHolder(buf, reader.Position.GetInteger());
            }
            else
            {
                buf.Dispose();
                _closed = true;
            }
        }

        /// <inheritdoc/>
        public IResultSetMetadata? Metadata { get; }

        /// <inheritdoc/>
        public bool HasRowSet { get; }

        /// <inheritdoc/>
        public long AffectedRows { get; }

        /// <inheritdoc/>
        public bool WasApplied { get; }

        /// <inheritdoc/>
        public IIgniteTuple Current { get; }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (_closed)
            {
                return;
            }

            _buffer?.Buffer.Dispose();

            if (_resourceId != null)
            {
                using var writer = new PooledArrayBufferWriter();
                Write(writer.GetMessageWriter());

                await _socket.DoOutInOpAsync(ClientOp.SqlCursorClose, writer).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerator<IIgniteTuple> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            // TODO: Allowed to be called only once.
            // TODO: Throw when no row set.
            // TODO: Deserialize and set Current.
            return this;
        }

        /// <inheritdoc/>
        public ValueTask<bool> MoveNextAsync()
        {
            throw new NotImplementedException();
        }

        private static ResultSetMetadata ReadMeta(ref MessagePackReader reader)
        {
            var size = reader.ReadArrayHeader();

            var columns = new List<IColumnMetadata>(size);

            for (int i = 0; i < size; i++)
            {
                var name = reader.ReadString();
                var nullable = reader.ReadBoolean();
                var type = (SqlColumnType)reader.ReadInt32();
                var scale = reader.ReadInt32();
                var precision = reader.ReadInt32();

                var origin = reader.ReadBoolean()
                    ? new ColumnOrigin(
                        ColumnName: reader.TryReadNil() ? name : reader.ReadString(),
                        SchemaName: reader.TryReadInt(out var idx) ? columns[idx].Origin!.SchemaName : reader.ReadString(),
                        TableName: reader.TryReadInt(out idx) ? columns[idx].Origin!.TableName : reader.ReadString())
                    : null;

                columns.Add(new ColumnMetadata(name, type, precision, scale, nullable, origin));
            }

            return new ResultSetMetadata(columns);
        }

        private void Write(MessagePackWriter writer)
        {
            var resourceId = _resourceId;

            if (resourceId == null)
            {
                throw new InvalidOperationException("ResultSet does not have rows or is closed.");
            }

            writer.Write(_resourceId!.Value);
            writer.Flush();
        }
    }
}
