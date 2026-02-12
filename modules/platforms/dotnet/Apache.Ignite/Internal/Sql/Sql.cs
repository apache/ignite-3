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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Table.Mapper;
    using Ignite.Transactions;
    using Linq;
    using Proto;
    using Proto.BinaryTuple;
    using Proto.MsgPack;
    using Table.Serialization;
    using Transactions;

    using ClientTable = Table.Table;

    /// <summary>
    /// SQL API.
    /// </summary>
    internal sealed class Sql : ISql
    {
        private static readonly RowReader<IIgniteTuple> TupleReader =
            static (ResultSetMetadata metadata, ref BinaryTupleReader reader, object? _) => ReadTuple(metadata.Columns, ref reader);

        private static readonly RowReaderFactory<IIgniteTuple> TupleReaderFactory = static _ => TupleReader;

        /** Underlying connection. */
        private readonly ClientFailoverSocket _socket;

        /** Partition awareness mapping cache, keyed by (schema, query). */
        private readonly ConcurrentDictionary<(string? Schema, string Query), SqlPartitionMappingProvider> _paMappingCache = new();

        // TODO: Cache in Tables._cachedTables?
        /** Cached Table instances used for PA schema and partition assignment loading. */
        private readonly ConcurrentDictionary<int, ClientTable> _paTableCache = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="Sql"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public Sql(ClientFailoverSocket socket)
        {
            _socket = socket;
        }

        /// <inheritdoc/>
        public async Task<IResultSet<IIgniteTuple>> ExecuteAsync(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args) =>
            await ExecuteAsyncInternal(
                transaction,
                statement,
                TupleReaderFactory,
                rowReaderArg: null,
                args,
                cancellationToken)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
        public async Task<IResultSet<T>> ExecuteAsync<T>(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args) =>
            await ExecuteAsyncInternal(
                    transaction,
                    statement,
                    static meta => GetReaderFactory<T>(meta),
                    rowReaderArg: null,
                    args,
                    cancellationToken)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IResultSet<T>> ExecuteAsync<T>(
            ITransaction? transaction,
            IMapper<T> mapper,
            SqlStatement statement,
            CancellationToken cancellationToken,
            params object?[]? args)
        {
            IgniteArgumentCheck.NotNull(mapper);

            return await ExecuteAsyncInternal(
                    transaction,
                    statement,
                    RowReaderFactory,
                    rowReaderArg: mapper,
                    args,
                    cancellationToken)
                .ConfigureAwait(false);

            static RowReader<T> RowReaderFactory(ResultSetMetadata resultSetMetadata) =>
                static (ResultSetMetadata meta, ref BinaryTupleReader reader, object? arg) =>
                {
                    var mapperReader = new RowReader(ref reader, meta);
                    var mapper = (IMapper<T>)arg!;

                    return mapper.Read(ref mapperReader, meta);
                };
        }

        /// <inheritdoc/>
        public async Task<IgniteDbDataReader> ExecuteReaderAsync(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args)
        {
            var resultSet = await ExecuteAsyncInternal<object>(
                transaction,
                statement,
                static _ => null!,
                rowReaderArg: null,
                args,
                cancellationToken).ConfigureAwait(false);

            if (!resultSet.HasRowSet)
            {
                throw new InvalidOperationException($"{nameof(ExecuteReaderAsync)} does not support queries without row set (DDL, DML).");
            }

            return new IgniteDbDataReader(resultSet);
        }

        /// <inheritdoc/>
        public async Task ExecuteScriptAsync(SqlStatement script, params object?[]? args)
        {
            await ExecuteScriptAsync(script, CancellationToken.None, args).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task ExecuteScriptAsync(SqlStatement script, CancellationToken cancellationToken, params object?[]? args)
        {
            IgniteArgumentCheck.NotNull(script);

            using var bufferWriter = ProtoCommon.GetMessageWriter();
            WriteStatement(bufferWriter, script, args);

            try
            {
                using var buf = await _socket.DoOutInOpAsync(
                    ClientOp.SqlExecScript, bufferWriter, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (SqlException e)
            {
                ConvertExceptionAndThrow(e, script, cancellationToken);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task<long[]> ExecuteBatchAsync(
            ITransaction? transaction,
            SqlStatement statement,
            IEnumerable<IEnumerable<object?>> args,
            CancellationToken cancellationToken = default)
        {
            IgniteArgumentCheck.NotNull(statement);
            IgniteArgumentCheck.NotNull(args);

            cancellationToken.ThrowIfCancellationRequested();
            Transaction? tx = await LazyTransaction.EnsureStartedAsync(transaction, _socket, default).ConfigureAwait(false);

            using var bufferWriter = ProtoCommon.GetMessageWriter();

            WriteStatement(bufferWriter, statement, tx, writeTx: true);
            WriteBatchArgs(bufferWriter, args);
            bufferWriter.MessageWriter.Write(_socket.ObservableTimestamp);

            try
            {
                var (buf, _) = await _socket.DoOutInOpAndGetSocketAsync(
                    ClientOp.SqlExecBatch, tx, bufferWriter, cancellationToken: cancellationToken).ConfigureAwait(false);

                using (buf)
                {
                    return Read(buf);
                }
            }
            catch (SqlBatchException e)
            {
                ConvertExceptionAndThrow(e, statement, cancellationToken);

                throw;
            }

            static long[] Read(PooledBuffer resBuf)
            {
                var r = resBuf.GetReader();
                r.Skip(4); // Unused values: resourceId, rowSet, morePages, wasApplied

                int count = r.ReadInt32();
                var affectedRows = new long[count];

                for (var i = 0; i < count; i++)
                {
                    affectedRows[i] = r.ReadInt64();
                }

                return affectedRows;
            }
        }

        /// <inheritdoc/>
        public override string ToString() => IgniteToStringBuilder.Build(GetType());

        /// <summary>
        /// Reads column value.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="col">Column.</param>
        /// <param name="idx">Index.</param>
        /// <returns>Value.</returns>
        internal static object? ReadColumnValue(ref BinaryTupleReader reader, IColumnMetadata col, int idx)
        {
            if (reader.IsNull(idx))
            {
                return null;
            }

            return col.Type switch
            {
                ColumnType.Null => null,
                ColumnType.Boolean => reader.GetBool(idx),
                ColumnType.Int8 => reader.GetByte(idx),
                ColumnType.Int16 => reader.GetShort(idx),
                ColumnType.Int32 => reader.GetInt(idx),
                ColumnType.Int64 => reader.GetLong(idx),
                ColumnType.Float => reader.GetFloat(idx),
                ColumnType.Double => reader.GetDouble(idx),
                ColumnType.Decimal => reader.GetBigDecimal(idx, col.Scale),
                ColumnType.Date => reader.GetDate(idx),
                ColumnType.Time => reader.GetTime(idx),
                ColumnType.Datetime => reader.GetDateTime(idx),
                ColumnType.Timestamp => reader.GetTimestamp(idx),
                ColumnType.Uuid => reader.GetGuid(idx),
                ColumnType.String => reader.GetString(idx),
                ColumnType.ByteArray => reader.GetBytes(idx),
                ColumnType.Period => reader.GetPeriod(idx),
                ColumnType.Duration => reader.GetDuration(idx),
                _ => throw new InvalidOperationException("Unknown SQL column type: " + col.Type)
            };
        }

        /// <summary>
        /// Executes single SQL statement and returns rows deserialized with the provided <paramref name="rowReaderFactory"/>.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="rowReaderFactory">Row reader factory.</param>
        /// <param name="rowReaderArg">Row reader arg.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <typeparam name="T">Row type.</typeparam>
        /// <returns>SQL result set.</returns>
        internal async Task<ResultSet<T>> ExecuteAsyncInternal<T>(
            ITransaction? transaction,
            SqlStatement statement,
            RowReaderFactory<T> rowReaderFactory,
            object? rowReaderArg,
            ICollection<object?>? args,
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNull(statement);

            cancellationToken.ThrowIfCancellationRequested();
            Transaction? tx = await LazyTransaction.EnsureStartedAsync(transaction, _socket, default).ConfigureAwait(false);

            using var bufferWriter = ProtoCommon.GetMessageWriter();
            WriteStatement(bufferWriter, statement, args, tx, writeTx: true);

            // Look up cached PA mapping to route the query to the preferred node.
            var paKey = (statement.Schema, statement.Query);
            PreferredNode preferredNode = default;

            if (_paMappingCache.TryGetValue(paKey, out var mappingProvider))
            {
                preferredNode = mappingProvider.GetPreferredNode(args);
            }

            PooledBuffer? buf = null;

            try
            {
                (buf, var socket) = await _socket.DoOutInOpAndGetSocketAsync(
                    ClientOp.SqlExec, tx, bufferWriter, preferredNode: preferredNode, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                // ResultSet will dispose the pooled buffer.
                var resultSet = new ResultSet<T>(socket, buf, rowReaderFactory, rowReaderArg, cancellationToken, readPaMetadata: socket.ConnectionContext.ServerHasFeature(ProtocolBitmaskFeature.SqlPartitionAwarenessTableName));

                // Cache PA metadata for subsequent queries.
                var paMeta = resultSet.PartitionAwarenessMetadata;
                if (paMeta != null)
                {
                    // TODO: We don't have the table name here, which makes it difficult
                    // Reusing existing table cache in Tables.
                    // Should we add the table name to the response?
                    var table = _paTableCache.GetOrAdd(
                        paMeta.TableId,
                        static (id, state) => new ClientTable(
                            QualifiedName.Of("DUMMY", $"DUMMY_{id}"),
                            id,
                            state.Socket,
                            state.Sql),
                        (Socket: _socket, Sql: this));

                    _paMappingCache[paKey] = new SqlPartitionMappingProvider(table, paMeta);
                }

                return resultSet;
            }
            catch (SqlException e)
            {
                buf?.Dispose();

                ConvertExceptionAndThrow(e, statement, cancellationToken);

                throw;
            }
            catch (Exception)
            {
                buf?.Dispose();

                throw;
            }
        }

        private static void ConvertExceptionAndThrow(IgniteException e, SqlStatement statement, CancellationToken token)
        {
            switch (e.Code)
            {
                case ErrorGroups.Sql.StmtParse:
                    throw new SqlException(
                        e.TraceId,
                        ErrorGroups.Sql.StmtValidation,
                        "Invalid query, check inner exceptions for details: " + statement,
                        e);

                case ErrorGroups.Sql.ExecutionCancelled:
                    var cancelledToken = token.IsCancellationRequested ? token : CancellationToken.None;

                    throw new OperationCanceledException(e.Message, e, cancelledToken);
            }
        }

        private static void WriteProperties(SqlStatement statement, ref MsgPackWriter w)
        {
            var props = statement.Properties;
            w.Write(props.Count);
            using var propTuple = new BinaryTupleBuilder(props.Count * 4);

            foreach (var (key, val) in props)
            {
                propTuple.AppendString(key);
                propTuple.AppendObjectWithType(val);
            }

            w.Write(propTuple.Build().Span);
        }

        private static IgniteTuple ReadTuple(IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader tupleReader)
        {
            var row = new IgniteTuple(cols.Count);

            for (var i = 0; i < cols.Count; i++)
            {
                var col = cols[i];
                row[col.Name] = ReadColumnValue(ref tupleReader, col, i);
            }

            return row;
        }

        [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
        private static RowReader<T> GetReaderFactory<T>(ResultSetMetadata metadata) =>
            ResultSelector.Get<T>(metadata, selectorExpression: null, ResultSelectorOptions.None);

        private static void WriteBatchArgs(PooledArrayBuffer writer, IEnumerable<IEnumerable<object?>> args)
        {
            int rowSize = -1;
            int rowCountPos = -1;
            int rowCount = 0;

            var w = writer.MessageWriter;

            foreach (var arg in args)
            {
                IgniteArgumentCheck.NotNull(arg);
                IEnumerable<object?> row = arg;
                rowCount++;

                if (rowSize < 0)
                {
                    // First row, write header.
                    if (!row.TryGetNonEnumeratedCount(out rowSize))
                    {
                        var list = row.ToList();
                        rowSize = list.Count;
                        row = list;
                    }

                    IgniteArgumentCheck.Ensure(rowSize > 0, nameof(args), "Batch arguments must not contain empty rows.");

                    w.Write(rowSize);
                    rowCountPos = writer.ReserveMsgPackInt32();
                    w.Write(false); // Paged args.
                }

                w.WriteObjectEnumerableAsBinaryTuple(row, expectedCount: rowSize, errorPrefix: "Inconsistent batch argument size: ");
            }

            IgniteArgumentCheck.Ensure(rowCount > 0, nameof(args), "Batch arguments must not be empty.");

            writer.WriteMsgPackInt32(rowCount, rowCountPos);
        }

        private static void WriteStatement(
            PooledArrayBuffer writer,
            SqlStatement statement,
            Transaction? tx = null,
            bool writeTx = false)
        {
            var w = writer.MessageWriter;

            if (writeTx)
            {
                w.WriteTx(tx?.Id);
            }

            w.Write(statement.Schema);
            w.Write(statement.PageSize);
            w.Write((long)statement.Timeout.TotalMilliseconds);
            w.WriteNil(); // Session timeout (unused, session is closed by the server immediately).
            w.Write(statement.TimeZoneId);

            WriteProperties(statement, ref w);
            w.Write(statement.Query);
        }

        private void WriteStatement(
            PooledArrayBuffer writer,
            SqlStatement statement,
            ICollection<object?>? args,
            Transaction? tx = null,
            bool writeTx = false)
        {
            var w = writer.MessageWriter;

            WriteStatement(writer, statement, tx, writeTx);

            w.WriteObjectCollectionWithCountAsBinaryTuple(args);
            w.Write(_socket.ObservableTimestamp);

            // Request partition awareness metadata from the server.
            w.Write(true);
        }
    }
}
