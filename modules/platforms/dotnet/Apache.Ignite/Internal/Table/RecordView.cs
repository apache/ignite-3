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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Compute;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Transactions;
    using Linq;
    using Microsoft.Extensions.Logging;
    using Proto;
    using Serialization;
    using Sql;
    using Transactions;

    /// <summary>
    /// Generic record view.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal sealed class RecordView<T> : IRecordView<T>
        where T : notnull
    {
        /** Table. */
        private readonly Table _table;

        /** Serializer. */
        private readonly RecordSerializer<T> _ser;

        /** SQL. */
        private readonly Sql _sql;

        private readonly ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordView{T}"/> class.
        /// </summary>
        /// <param name="table">Table.</param>
        /// <param name="ser">Serializer.</param>
        /// <param name="sql">SQL.</param>
        public RecordView(Table table, RecordSerializer<T> ser, Sql sql)
        {
            _table = table;
            _ser = ser;
            _sql = sql;
            _logger = table.Socket.Configuration.LoggerFactory.CreateLogger<RecordView<T>>();
        }

        /// <summary>
        /// Gets the record serializer.
        /// </summary>
        public RecordSerializer<T> RecordSerializer => _ser;

        /// <summary>
        /// Gets the table.
        /// </summary>
        public Table Table => _table;

        /// <summary>
        /// Gets the SQL.
        /// </summary>
        public Sql Sql => _sql;

        /// <inheritdoc/>
        public IQueryable<T> AsQueryable(ITransaction? transaction = null, QueryableOptions? options = null)
        {
            var executor = new IgniteQueryExecutor(_sql, transaction, options, Table.Socket.Configuration);
            var provider = new IgniteQueryProvider(IgniteQueryParser.Instance, executor, _table.Name);

            if (typeof(T).IsKeyValuePair())
            {
                throw new NotSupportedException(
                    $"Can't use {typeof(KeyValuePair<,>)} for LINQ queries: " +
                    $"it is reserved for {typeof(IKeyValueView<,>)}.{nameof(IKeyValueView<int, int>.AsQueryable)}. " +
                    "Use a custom type instead.");
            }

            return new IgniteQueryable<T>(provider);
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGet, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<bool> ContainsKeyAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleContainsKey, transaction, key, keyOnly: true).ConfigureAwait(false);
            return ReadSchemaAndBoolean(resBuf);
        }

        /// <inheritdoc/>
        public async Task<IList<Option<T>>> GetAllAsync(ITransaction? transaction, IEnumerable<T> keys) =>
            await GetAllAsync(
                transaction: transaction,
                keys: keys,
                resultFactory: static count => count == 0
                    ? (IList<Option<T>>)Array.Empty<Option<T>>()
                    : new List<Option<T>>(count),
                addAction: static (res, item) => res.Add(item))
                .ConfigureAwait(false);

        /// <summary>
        /// Gets multiple records by keys.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="keys">Collection of records with key columns set.</param>
        /// <param name="resultFactory">Result factory.</param>
        /// <param name="addAction">Add action.</param>
        /// <typeparam name="TRes">Result type.</typeparam>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains matching records with all columns filled from the table. The order of collection
        /// elements is guaranteed to be the same as the order of <paramref name="keys"/>. If a record does not exist,
        /// the element at the corresponding index of the resulting collection will be empty <see cref="Option{T}"/>.
        /// </returns>
        public async Task<TRes> GetAllAsync<TRes>(
            ITransaction? transaction,
            IEnumerable<T> keys,
            Func<int, TRes> resultFactory,
            Action<TRes, Option<T>> addAction)
        {
            IgniteArgumentCheck.NotNull(keys);

            using var resBuf = await DoMultiRecordOutOpAsync(ClientOp.TupleGetAll, transaction, keys, true).ConfigureAwait(false);
            if (resBuf == null)
            {
                return resultFactory(0);
            }

            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadMultipleNullable(resBuf, resSchema, resultFactory, addAction);
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleUpsert, transaction, record).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task UpsertAllAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            IgniteArgumentCheck.NotNull(records);

            using var resBuf = await DoMultiRecordOutOpAsync(ClientOp.TupleUpsertAll, transaction, records).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAndUpsertAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGetAndUpsert, transaction, record).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<bool> InsertAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleInsert, transaction, record).ConfigureAwait(false);
            return ReadSchemaAndBoolean(resBuf);
        }

        /// <inheritdoc/>
        public async Task<IList<T>> InsertAllAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            IgniteArgumentCheck.NotNull(records);

            using var resBuf = await DoMultiRecordOutOpAsync(ClientOp.TupleInsertAll, transaction, records).ConfigureAwait(false);
            if (resBuf == null)
            {
                return Array.Empty<T>();
            }

            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadMultiple(
                buf: resBuf,
                schema: resSchema,
                keyOnly: false,
                resultFactory: static count => count == 0
                    ? (IList<T>)Array.Empty<T>()
                    : new List<T>(count),
                addAction: static (res, item) => res.Add(item));
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleReplace, transaction, record).ConfigureAwait(false);
            return ReadSchemaAndBoolean(resBuf);
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, T record, T newRecord)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoTwoRecordOutOpAsync(ClientOp.TupleReplaceExact, transaction, record, newRecord)
                .ConfigureAwait(false);

            return ReadSchemaAndBoolean(resBuf);
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAndReplaceAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGetAndReplace, transaction, record).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            return ReadSchemaAndBoolean(resBuf);
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteExactAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleDeleteExact, transaction, record).ConfigureAwait(false);
            return ReadSchemaAndBoolean(resBuf);
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAndDeleteAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key);

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGetAndDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<IList<T>> DeleteAllAsync(ITransaction? transaction, IEnumerable<T> keys) =>
            await DeleteAllAsync(transaction, keys, exact: false).ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IList<T>> DeleteAllExactAsync(ITransaction? transaction, IEnumerable<T> records) =>
            await DeleteAllAsync(transaction, records, exact: true).ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task StreamDataAsync(
            IAsyncEnumerable<DataStreamerItem<T>> data,
            DataStreamerOptions? options = null,
            CancellationToken cancellationToken = default) =>
            await DataStreamer.StreamDataAsync(
                data,
                _table,
                writer: _ser.Handler,
                options ?? DataStreamerOptions.Default,
                cancellationToken).ConfigureAwait(false);

        /// <inheritdoc/>
        public IAsyncEnumerable<TResult> StreamDataAsync<TSource, TPayload, TResult>(
            IAsyncEnumerable<TSource> data,
            DataStreamerOptions? options,
            Func<TSource, T> keySelector,
            Func<TSource, TPayload> payloadSelector,
            IEnumerable<DeploymentUnit> units,
            string receiverClassName,
            object[]? receiverArgs,
            CancellationToken cancellationToken = default) =>
            DataStreamerWithReceiver.StreamDataAsync<TSource, T, TPayload, TResult>(
                data,
                _table,
                keySelector,
                payloadSelector,
                keyWriter: _ser.Handler,
                options ?? DataStreamerOptions.Default,
                expectResults: true,
                receiverArgs,
                cancellationToken);

        /// <inheritdoc/>
        public async Task StreamDataAsync<TSource, TPayload>(
            IAsyncEnumerable<TSource> data,
            DataStreamerOptions? options,
            Func<TSource, T> keySelector,
            Func<TSource, TPayload> payloadSelector,
            IEnumerable<DeploymentUnit> units,
            string receiverClassName,
            object[]? receiverArgs,
            CancellationToken cancellationToken = default)
        {
            IAsyncEnumerable<object> results = DataStreamerWithReceiver.StreamDataAsync<TSource, T, TPayload, object>(
                data,
                _table,
                keySelector,
                payloadSelector,
                keyWriter: _ser.Handler,
                options ?? DataStreamerOptions.Default,
                expectResults: false,
                receiverArgs,
                cancellationToken);

            // Await streaming completion.
            await foreach (var unused in results)
            {
                Debug.Fail("Got results with expectResults=false: " + unused);
            }
        }

        /// <inheritdoc/>
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .Append(Table)
                .Build();

        /// <summary>
        /// Deletes multiple records. If one or more keys do not exist, other records are still deleted.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Record keys to delete.</param>
        /// <param name="exact">Whether to match on both key and value.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains records from <paramref name="records"/> that did not exist.
        /// </returns>
        internal async Task<IList<T>> DeleteAllAsync(ITransaction? transaction, IEnumerable<T> records, bool exact) =>
            await DeleteAllAsync(
                transaction,
                records,
                resultFactory: static count => count == 0
                    ? (IList<T>)Array.Empty<T>()
                    : new List<T>(count),
                addAction: static (res, item) => res.Add(item),
                exact: exact).ConfigureAwait(false);

        /// <summary>
        /// Deletes multiple records. If one or more keys do not exist, other records are still deleted.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Record keys to delete.</param>
        /// <param name="resultFactory">Result factory.</param>
        /// <param name="addAction">Add action.</param>
        /// <param name="exact">Whether to match on both key and value.</param>
        /// <typeparam name="TRes">Result type.</typeparam>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains records from <paramref name="records"/> that did not exist.
        /// </returns>
        internal async Task<TRes> DeleteAllAsync<TRes>(
            ITransaction? transaction,
            IEnumerable<T> records,
            Func<int, TRes> resultFactory,
            Action<TRes, T> addAction,
            bool exact)
        {
            IgniteArgumentCheck.NotNull(records);

            var clientOp = exact ? ClientOp.TupleDeleteAllExact : ClientOp.TupleDeleteAll;
            using var resBuf = await DoMultiRecordOutOpAsync(clientOp, transaction, records, keyOnly: !exact).ConfigureAwait(false);
            if (resBuf == null)
            {
                return resultFactory(0);
            }

            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadMultiple(
                buf: resBuf,
                schema: resSchema,
                keyOnly: !exact,
                resultFactory: resultFactory,
                addAction: addAction);
        }

        private static bool ReadSchemaAndBoolean(PooledBuffer buf)
        {
            var reader = buf.GetReader();

            _ = reader.ReadInt32();

            return reader.ReadBoolean();
        }

        private async Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            Transaction? tx,
            PooledArrayBuffer? request = null,
            PreferredNode preferredNode = default,
            IRetryPolicy? retryPolicyOverride = null)
        {
            var (buf, _) = await _table.Socket.DoOutInOpAndGetSocketAsync(clientOp, tx, request, preferredNode, retryPolicyOverride)
                .ConfigureAwait(false);

            return buf;
        }

        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive.")]
        private async Task<PooledBuffer> DoRecordOutOpAsync(
            ClientOp op,
            ITransaction? transaction,
            T record,
            bool keyOnly = false,
            int? schemaVersionOverride = null)
        {
            Schema? schema = null;

            try
            {
                schema = await _table.GetSchemaAsync(schemaVersionOverride).ConfigureAwait(false);

                LazyTransaction? lazyTx = LazyTransaction.Get(transaction);
                var txId = lazyTx?.Id;

                using var writer = ProtoCommon.GetMessageWriter();
                var (colocationHash, txIdPos) = _ser.Write(writer, txId, schema, record, keyOnly);
                var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

                var tx = await LazyTransaction.EnsureStartedAsync(transaction, _table.Socket, preferredNode)
                    .ConfigureAwait(false);

                if (tx != null && txId == LazyTransaction.TxIdPlaceholder)
                {
                    writer.WriteLongBigEndian(tx.Id, txIdPos + 1);
                }

                return await DoOutInOpAsync(op, tx, writer, preferredNode).ConfigureAwait(false);
            }
            catch (IgniteException e) when (e.Code == ErrorGroups.Table.SchemaVersionMismatch &&
                                            schemaVersionOverride != e.GetExpectedSchemaVersion())
            {
                schemaVersionOverride = e.GetExpectedSchemaVersion();

                _logger.LogRetryingSchemaVersionMismatchErrorDebug(_table.Id, schema?.Version, schemaVersionOverride);

                return await DoRecordOutOpAsync(op, transaction, record, keyOnly, schemaVersionOverride).ConfigureAwait(false);
            }
            catch (Exception e) when (e.CausedByUnmappedColumns() &&
                                      schemaVersionOverride == null)
            {
                _logger.LogRetryingUnmappedColumnsErrorDebug(_table.Id, schema?.Version, e.Message);

                schemaVersionOverride = Table.SchemaVersionForceLatest;
                return await DoRecordOutOpAsync(op, transaction, record, keyOnly, schemaVersionOverride).ConfigureAwait(false);
            }
        }

        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive.")]
        private async Task<PooledBuffer> DoTwoRecordOutOpAsync(
            ClientOp op,
            ITransaction? transaction,
            T record,
            T record2,
            bool keyOnly = false,
            int? schemaVersionOverride = null)
        {
            Schema? schema = null;

            try
            {
                schema = await _table.GetSchemaAsync(schemaVersionOverride).ConfigureAwait(false);

                LazyTransaction? lazyTx = LazyTransaction.Get(transaction);
                var txId = lazyTx?.Id;

                using var writer = ProtoCommon.GetMessageWriter();
                var (colocationHash, txIdPos) = _ser.WriteTwo(writer, txId, schema, record, record2, keyOnly);
                var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

                var tx = await LazyTransaction.EnsureStartedAsync(transaction, _table.Socket, preferredNode)
                    .ConfigureAwait(false);

                if (tx != null && txId == LazyTransaction.TxIdPlaceholder)
                {
                    writer.WriteLongBigEndian(tx.Id, txIdPos + 1);
                }

                return await DoOutInOpAsync(op, tx, writer, preferredNode).ConfigureAwait(false);
            }
            catch (IgniteException e) when (e.Code == ErrorGroups.Table.SchemaVersionMismatch &&
                                            schemaVersionOverride != e.GetExpectedSchemaVersion())
            {
                schemaVersionOverride = e.GetExpectedSchemaVersion();

                _logger.LogRetryingSchemaVersionMismatchErrorDebug(_table.Id, schema?.Version, schemaVersionOverride);

                return await DoTwoRecordOutOpAsync(op, transaction, record, record2, keyOnly, schemaVersionOverride).ConfigureAwait(false);
            }
            catch (Exception e) when (e.CausedByUnmappedColumns() &&
                                      schemaVersionOverride == null)
            {
                _logger.LogRetryingUnmappedColumnsErrorDebug(_table.Id, schema?.Version, e.Message);

                schemaVersionOverride = Table.SchemaVersionForceLatest;
                return await DoTwoRecordOutOpAsync(op, transaction, record, record2, keyOnly, schemaVersionOverride).ConfigureAwait(false);
            }
        }

        private async Task<PooledBuffer?> DoMultiRecordOutOpAsync(
            ClientOp op,
            ITransaction? transaction,
            IEnumerable<T> recs,
            bool keyOnly = false,
            int? schemaVersionOverride = null)
        {
            // ReSharper disable once PossibleMultipleEnumeration (we may have to retry, but this is very rare)
            using var iterator = recs.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return null;
            }

            Schema? schema = null;

            try
            {
                schema = await _table.GetSchemaAsync(schemaVersionOverride).ConfigureAwait(false);

                LazyTransaction? lazyTx = LazyTransaction.Get(transaction);
                var txId = lazyTx?.Id;

                using var writer = ProtoCommon.GetMessageWriter();
                var (colocationHash, txIdPos) = _ser.WriteMultiple(writer, txId, schema, iterator, keyOnly);
                var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

                var tx = await LazyTransaction.EnsureStartedAsync(transaction, _table.Socket, preferredNode)
                    .ConfigureAwait(false);

                if (tx != null && txId == LazyTransaction.TxIdPlaceholder)
                {
                    writer.WriteLongBigEndian(tx.Id, txIdPos + 1);
                }

                return await DoOutInOpAsync(op, tx, writer, preferredNode).ConfigureAwait(false);
            }
            catch (IgniteException e) when (e.Code == ErrorGroups.Table.SchemaVersionMismatch &&
                                            schemaVersionOverride != e.GetExpectedSchemaVersion())
            {
                schemaVersionOverride = e.GetExpectedSchemaVersion();

                _logger.LogRetryingSchemaVersionMismatchErrorDebug(_table.Id, schema?.Version, schemaVersionOverride);

                // ReSharper disable once PossibleMultipleEnumeration (we have to retry, but this is very rare)
                return await DoMultiRecordOutOpAsync(op, transaction, recs, keyOnly, schemaVersionOverride).ConfigureAwait(false);
            }
            catch (Exception e) when (e.CausedByUnmappedColumns() &&
                                      schemaVersionOverride == null)
            {
                _logger.LogRetryingUnmappedColumnsErrorDebug(_table.Id, schema?.Version, e.Message);

                schemaVersionOverride = Table.SchemaVersionForceLatest;

                // ReSharper disable once PossibleMultipleEnumeration (we have to retry, but this is very rare)
                return await DoMultiRecordOutOpAsync(op, transaction, recs, keyOnly, schemaVersionOverride).ConfigureAwait(false);
            }
        }
    }
}
