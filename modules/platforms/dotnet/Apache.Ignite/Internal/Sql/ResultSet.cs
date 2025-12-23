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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Sql;
    using Proto;
    using Proto.BinaryTuple;
    using Proto.MsgPack;

    /// <summary>
    /// SQL result set.
    /// </summary>
    /// <typeparam name="T">Result type.</typeparam>
    internal sealed class ResultSet<T> : IResultSet<T>
    {
        private readonly ClientSocket _socket;

        private readonly long? _resourceId;

        private readonly PooledBuffer? _buffer;

        private readonly bool _hasMorePages;

        private readonly RowReader<T>? _rowReader;

        private readonly CancellationToken _cancellationToken;

        private bool _resourceClosed;

        private int _bufferReleased;

        private bool _iterated;

        /// <summary>
        /// Initializes a new instance of the <see cref="ResultSet{T}"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="buf">Buffer to read initial data from.</param>
        /// <param name="rowReaderFactory">Row reader factory.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ResultSet(ClientSocket socket, PooledBuffer buf, RowReaderFactory<T> rowReaderFactory, CancellationToken cancellationToken)
        {
            _socket = socket;
            _cancellationToken = cancellationToken;

            var reader = buf.GetReader();

            // ReSharper disable once RedundantCast (required on .NET Core 3.1).
            _resourceId = reader.TryReadNil() ? (long?)null : reader.ReadInt64();

            HasRowSet = reader.ReadBoolean();
            _hasMorePages = reader.ReadBoolean();
            WasApplied = reader.ReadBoolean();
            AffectedRows = reader.ReadInt64();

            ResultSetMetadata? meta = HasRowSet ? ReadMeta(ref reader) : null;
            Metadata = meta;
            _rowReader = meta != null ? rowReaderFactory(Metadata.Columns) : null;

            if (HasRowSet)
            {
                buf.Position += reader.Consumed;
                _buffer = buf;
                HasRows = reader.ReadInt32() > 0;
            }
            else
            {
                buf.Dispose();
                _bufferReleased = 1;
                _resourceClosed = true;
            }
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="ResultSet{T}"/> class.
        /// </summary>
        ~ResultSet()
        {
            Dispose();
        }

        /// <inheritdoc/>
        public IResultSetMetadata? Metadata { get; }

        /// <inheritdoc/>
        public bool HasRowSet { get; }

        /// <inheritdoc/>
        public long AffectedRows { get; }

        /// <inheritdoc/>
        public bool WasApplied { get; }

        /// <summary>
        /// Gets a value indicating whether this instance is disposed.
        /// </summary>
        internal bool IsDisposed => (_resourceId == null || _resourceClosed) && _bufferReleased > 0;

        /// <summary>
        /// Gets a value indicating whether this result set has any rows in it.
        /// </summary>
        internal bool HasRows { get; }

        /// <inheritdoc/>
        public async ValueTask<List<T>> ToListAsync() =>
            await CollectAsync(
                    constructor: static capacity => new List<T>(capacity),
                    accumulator: static (list, item) => list.Add(item))
                .ConfigureAwait(false);

        /// <inheritdoc/>
        [SuppressMessage("ReSharper", "InconsistentNaming", Justification = "Generics.")]
        public async ValueTask<Dictionary<TK, TV>> ToDictionaryAsync<TK, TV>(
            Func<T, TK> keySelector,
            Func<T, TV> valSelector,
            IEqualityComparer<TK>? comparer)
            where TK : notnull
        {
            IgniteArgumentCheck.NotNull(keySelector);
            IgniteArgumentCheck.NotNull(valSelector);

            return await CollectAsync(
                    constructor: capacity => new Dictionary<TK, TV>(capacity, comparer),
                    accumulator: (dictionary, item) => dictionary.Add(keySelector(item), valSelector(item)))
                .ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async ValueTask<TResult> CollectAsync<TResult>(Func<int, TResult> constructor, Action<TResult, T> accumulator)
        {
            IgniteArgumentCheck.NotNull(constructor);
            IgniteArgumentCheck.NotNull(accumulator);

            ValidateAndSetIteratorState();

            // First page is included in the initial response.
            var cols = Metadata!.Columns;
            var hasMore = _hasMorePages;
            TResult? res = default;

            ReadPage(_buffer!);
            ReleaseBuffer();

            while (hasMore)
            {
                using var pageBuf = await FetchNextPage().ConfigureAwait(false);
                ReadPage(pageBuf);
            }

            _resourceClosed = true;

            return res!;

            void ReadPage(PooledBuffer buf)
            {
                var reader = buf.GetReader();
                var pageSize = reader.ReadInt32();

                var capacity = hasMore ? pageSize * 2 : pageSize;
                res ??= constructor(capacity);

                for (var rowIdx = 0; rowIdx < pageSize; rowIdx++)
                {
                    var row = ReadRow(cols, ref reader);
                    accumulator(res, row);
                }

                if (!reader.End)
                {
                    hasMore = reader.ReadBoolean();
                }
            }
        }

        /// <inheritdoc/>
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly", Justification = "SuppressFinalize in DisposeAsync")]
        public void Dispose()
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Dispose should not throw.")]
        public async ValueTask DisposeAsync()
        {
            ReleaseBuffer();

            if (_resourceId != null && !_resourceClosed)
            {
                try
                {
                    using var writer = ProtoCommon.GetMessageWriter();
                    WriteId(writer.MessageWriter);

                    // Cursor close should never be cancelled.
                    using var buffer = await _socket.DoOutInOpAsync(
                        ClientOp.SqlCursorClose, writer, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Ignore.
                    // Socket might be disconnected.
                }

                _resourceClosed = true;
            }

            GC.SuppressFinalize(this);
        }

        /// <inheritdoc/>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            ValidateAndSetIteratorState();

            return EnumerateRows().GetAsyncEnumerator(cancellationToken);
        }

        /// <inheritdoc/>
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .Append(HasRowSet)
                .Append(AffectedRows)
                .Append(WasApplied)
                .Append(Metadata)
                .Build();

        /// <summary>
        /// Enumerates ResultSet pages.
        /// </summary>
        /// <returns>ResultSet pages.</returns>
        internal async IAsyncEnumerable<PooledBuffer> EnumeratePagesInternal()
        {
            ValidateAndSetIteratorState();

            yield return _buffer!;

            ReleaseBuffer();

            if (!_hasMorePages)
            {
                yield break;
            }

            while (true)
            {
                using var buffer = await FetchNextPage().ConfigureAwait(false);

                yield return buffer;

                if (!HasMore(buffer))
                {
                    break;
                }
            }

            static bool HasMore(PooledBuffer buf)
            {
                var reader = buf.GetReader();
                var rowCount = reader.ReadInt32();
                reader.Skip(rowCount);

                return !reader.End && reader.ReadBoolean();
            }
        }

        private static ResultSetMetadata ReadMeta(ref MsgPackReader reader)
        {
            var size = reader.ReadInt32();

            var columns = new List<IColumnMetadata>(size);

            for (int i = 0; i < size; i++)
            {
                var propertyCount = reader.ReadInt32();
                const int minCount = 6;

                Debug.Assert(propertyCount >= minCount, "propertyCount >= " + minCount);

                var name = reader.ReadString();
                var nullable = reader.ReadBoolean();
                var type = (ColumnType)reader.ReadInt32();
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

        private T ReadRow(IReadOnlyList<IColumnMetadata> cols, ref MsgPackReader reader)
        {
            var tupleReader = new BinaryTupleReader(reader.ReadBinary(), cols.Count);

            return _rowReader!(cols, ref tupleReader);
        }

        private async IAsyncEnumerable<T> EnumerateRows()
        {
            var hasMore = _hasMorePages;
            var cols = Metadata!.Columns;
            var offset = 0;

            // First page.
            foreach (var row in EnumeratePage(_buffer!))
            {
                yield return row;
            }

            ReleaseBuffer();

            // Next pages.
            while (hasMore)
            {
                using var buffer = await FetchNextPage().ConfigureAwait(false);
                offset = 0;

                foreach (var row in EnumeratePage(buffer))
                {
                    yield return row;
                }
            }

            _resourceClosed = true;

            IEnumerable<T> EnumeratePage(PooledBuffer buf)
            {
                // ReSharper disable AccessToModifiedClosure
                var reader = buf.GetReader(offset);
                var pageSize = reader.ReadInt32();
                offset += reader.Consumed;

                for (var rowIdx = 0; rowIdx < pageSize; rowIdx++)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    // Can't use ref struct reader from above inside iterator block (CS4013).
                    // Use a new reader for every row (stack allocated).
                    var rowReader = buf.GetReader(offset);
                    var row = ReadRow(cols, ref rowReader);

                    offset += rowReader.Consumed;
                    yield return row;
                }

                reader = buf.GetReader(offset);
                if (!reader.End)
                {
                    hasMore = reader.ReadBoolean();
                }
            }
        }

        private async Task<PooledBuffer> FetchNextPage()
        {
            using var writer = ProtoCommon.GetMessageWriter();
            WriteId(writer.MessageWriter);

            return await _socket.DoOutInOpAsync(ClientOp.SqlCursorNextPage, writer, cancellationToken: _cancellationToken)
                .ConfigureAwait(false);
        }

        private void WriteId(MsgPackWriter writer)
        {
            var resourceId = _resourceId;

            Debug.Assert(resourceId != null, "resourceId != null");

            ObjectDisposedException.ThrowIf(_resourceClosed, this);

            writer.Write(_resourceId!.Value);
        }

        private void ValidateAndSetIteratorState()
        {
            if (!HasRowSet)
            {
                throw new IgniteClientException(ErrorGroups.Sql.QueryNoResultSet, "Query has no result set.");
            }

            if (_iterated)
            {
                throw new IgniteClientException(
                    ErrorGroups.Common.CursorAlreadyClosed,
                    "Query result set can not be iterated more than once.");
            }

            _iterated = true;
        }

        private void ReleaseBuffer()
        {
            // ResultSet is not thread safe, so we don't need Interlocked with correct usage.
            // However, double release of pooled buffers is very dangerous, so we protect against that anyway.
            if (Interlocked.CompareExchange(ref _bufferReleased, 1, 0) == 0)
            {
                _buffer?.Dispose();
            }
        }
    }
}
