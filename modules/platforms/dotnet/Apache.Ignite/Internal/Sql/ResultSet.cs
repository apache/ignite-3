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
    internal sealed class ResultSet : IResultSet<IIgniteTuple>
    {
        private readonly ClientSocket _socket;

        private readonly long? _resourceId;

        private readonly PooledBuffer? _buffer;

        private readonly int _bufferOffset;

        private readonly bool _hasMorePages;

        private volatile bool _closed;

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

            Metadata = HasRowSet ? ReadMeta(ref reader) : null;

            if (HasRowSet)
            {
                _buffer = buf;
                _bufferOffset = (int)reader.Consumed;
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
        public async Task<List<IIgniteTuple>> GetAllAsync()
        {
            if (_buffer == null || Metadata == null)
            {
                throw NoResultSetException();
            }

            // TODO: Retrieve all pages.
            await Task.Yield();

            return ReadPage(_buffer.Value, _bufferOffset, Metadata!.Columns);
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (_closed)
            {
                return;
            }

            _buffer?.Dispose();

            if (_resourceId != null)
            {
                using var writer = new PooledArrayBufferWriter();
                WriteId(writer.GetMessageWriter());

                await _socket.DoOutInOpAsync(ClientOp.SqlCursorClose, writer).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerator<IIgniteTuple> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            EnumerateRows().GetAsyncEnumerator(cancellationToken);

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

        private static IEnumerable<IIgniteTuple> EnumeratePage(PooledBuffer buf, int offset, IReadOnlyList<IColumnMetadata> cols)
        {
            var reader = buf.GetReader(offset);
            var pageSize = reader.ReadArrayHeader();
            offset += (int)reader.Consumed;

            for (var rowIdx = 0; rowIdx < pageSize; rowIdx++)
            {
                yield return ReadRow();
            }

            IgniteTuple ReadRow()
            {
                // Can't use ref struct reader from above inside iterator block (CS4013).
                // Use a new reader for every row (stack allocated).
                var reader = buf.GetReader(offset);
                var row = new IgniteTuple(cols.Count);

                foreach (var col in cols)
                {
                    row[col.Name] = ReadValue(ref reader, col.Type);
                }

                offset += (int)reader.Consumed;
                return row;
            }
        }

        private static List<IIgniteTuple> ReadPage(PooledBuffer buf, int offset, IReadOnlyList<IColumnMetadata> cols)
        {
            var reader = buf.GetReader(offset);
            var pageSize = reader.ReadArrayHeader();
            var res = new List<IIgniteTuple>(pageSize);

            for (var rowIdx = 0; rowIdx < pageSize; rowIdx++)
            {
                var row = new IgniteTuple(cols.Count);

                foreach (var col in cols)
                {
                    row[col.Name] = ReadValue(ref reader, col.Type);
                }

                res.Add(row);
            }

            return res;
        }

        private static object? ReadValue(ref MessagePackReader reader, SqlColumnType type)
        {
            if (reader.TryReadNil())
            {
                return null;
            }

            switch (type)
            {
                case SqlColumnType.Boolean:
                    return reader.ReadBoolean();

                case SqlColumnType.Int8:
                    return reader.ReadSByte();

                case SqlColumnType.Int16:
                    return reader.ReadInt16();

                case SqlColumnType.Int32:
                    return reader.ReadInt32();

                case SqlColumnType.Int64:
                    return reader.ReadInt64();

                case SqlColumnType.Float:
                    return reader.ReadSingle();

                case SqlColumnType.Double:
                    return reader.ReadDouble();

                case SqlColumnType.Datetime:
                    return reader.ReadDateTime();

                case SqlColumnType.Uuid:
                    return reader.ReadGuid();

                case SqlColumnType.String:
                    return reader.ReadString();

                case SqlColumnType.ByteArray:
                case SqlColumnType.Period:
                case SqlColumnType.Duration:
                case SqlColumnType.Number:
                case SqlColumnType.Bitmask:
                case SqlColumnType.Date:
                case SqlColumnType.Time:
                case SqlColumnType.Decimal:
                case SqlColumnType.Timestamp:
                default:
                    // TODO: Support all types (IGNITE-15431).
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        private static IgniteClientException NoResultSetException() => new("Query has no result set.");

        private async IAsyncEnumerable<IIgniteTuple> EnumerateRows()
        {
            // TODO: Allowed to be called only once.
            // TODO: Throw when no row set.
            // TODO: Deserialize and set Current.
            if (_buffer == null || Metadata == null)
            {
                throw NoResultSetException();
            }

            // TODO: Fetch all pages.
            await Task.Delay(1).ConfigureAwait(false);

            foreach (var row in EnumeratePage(_buffer.Value, _bufferOffset, Metadata.Columns))
            {
                yield return row;
            }
        }

        private void WriteId(MessagePackWriter writer)
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
