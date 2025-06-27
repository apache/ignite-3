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
    using Common;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Transactions;
    using Linq;
    using Proto;
    using Proto.BinaryTuple;
    using Proto.MsgPack;
    using Transactions;

    /// <summary>
    /// SQL API.
    /// </summary>
    internal sealed class Sql : ISql
    {
        private static readonly RowReader<IIgniteTuple> TupleReader =
            static (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) => ReadTuple(cols, ref reader);

        private static readonly RowReaderFactory<IIgniteTuple> TupleReaderFactory = static _ => TupleReader;

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
        public async Task<IResultSet<IIgniteTuple>> ExecuteAsync(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args) =>
            await ExecuteAsyncInternal(transaction, statement, TupleReaderFactory, args, cancellationToken).ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IResultSet<T>> ExecuteAsync<T>(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args) =>
            await ExecuteAsyncInternal(
                    transaction,
                    statement,
                    static cols => GetReaderFactory<T>(cols),
                    args,
                    cancellationToken)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IgniteDbDataReader> ExecuteReaderAsync(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args)
        {
            var resultSet = await ExecuteAsyncInternal<object>(
                transaction, statement, _ => null!, args, cancellationToken).ConfigureAwait(false);

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
        /// <param name="args">Arguments for the statement.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <typeparam name="T">Row type.</typeparam>
        /// <returns>SQL result set.</returns>
        internal async Task<ResultSet<T>> ExecuteAsyncInternal<T>(
            ITransaction? transaction,
            SqlStatement statement,
            RowReaderFactory<T> rowReaderFactory,
            ICollection<object?>? args,
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNull(statement);

            cancellationToken.ThrowIfCancellationRequested();
            Transaction? tx = await LazyTransaction.EnsureStartedAsync(transaction, _socket, default).ConfigureAwait(false);

            using var bufferWriter = ProtoCommon.GetMessageWriter();
            WriteStatement(bufferWriter, statement, args, tx, writeTx: true);

            PooledBuffer? buf = null;

            try
            {
                (buf, var socket) = await _socket.DoOutInOpAndGetSocketAsync(
                    ClientOp.SqlExec, tx, bufferWriter, cancellationToken: cancellationToken).ConfigureAwait(false);

                // ResultSet will dispose the pooled buffer.
                return new ResultSet<T>(socket, buf, rowReaderFactory, cancellationToken);
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

        private static void ConvertExceptionAndThrow(SqlException e, SqlStatement statement, CancellationToken token)
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

        private static RowReader<T> GetReaderFactory<T>(IReadOnlyList<IColumnMetadata> cols) =>
            ResultSelector.Get<T>(cols, selectorExpression: null, ResultSelectorOptions.None);

        private void WriteStatement(
            PooledArrayBuffer writer,
            SqlStatement statement,
            ICollection<object?>? args,
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
            w.WriteObjectCollectionAsBinaryTuple(args);
            w.Write(_socket.ObservableTimestamp);
        }
    }
}
