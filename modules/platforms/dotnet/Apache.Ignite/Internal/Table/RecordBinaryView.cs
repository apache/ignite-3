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
    using System.Threading.Tasks;
    using System.Transactions;
    using Buffers;
    using Common;
    using Ignite.Table;
    using Ignite.Transactions;
    using Proto;

    /// <summary>
    /// Table API.
    /// </summary>
    internal class RecordBinaryView : IRecordView<IIgniteTuple>
    {
        /** Table. */
        private readonly Table _table;

        private readonly RecordSerializer<IIgniteTuple> _ser;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordBinaryView"/> class.
        /// </summary>
        /// <param name="table">Table.</param>
        public RecordBinaryView(Table table)
        {
            _table = table;
            _ser = new RecordSerializer<IIgniteTuple>(table, null!);
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAsync(ITransaction? transaction, IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGet, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValueTuple(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple?>> GetAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> keys)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuples(writer, tx, schema, iterator, keyOnly: true);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleGetAll, tx, writer).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadTuplesNullable(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleUpsert, transaction, record).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task UpsertAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return;
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuples(writer, tx, schema, iterator);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleUpsertAll, tx, writer).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndUpsertAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGetAndUpsert, transaction, record).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValueTuple(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> InsertAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleInsert, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> InsertAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuples(writer, tx, schema, iterator);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleInsertAll, tx, writer).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadTuples(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleReplace, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, IIgniteTuple record, IIgniteTuple newRecord)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuples(writer, tx, schema, record, newRecord);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleReplaceExact, tx, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndReplaceAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGetAndReplace, transaction, record).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValueTuple(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteAsync(ITransaction? transaction, IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteExactAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleDeleteExact, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndDeleteAsync(ITransaction? transaction, IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGetAndDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValueTuple(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> DeleteAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> keys)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuples(writer, tx, schema, iterator, keyOnly: true);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleDeleteAll, tx, writer).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadTuples(resBuf, resSchema, keyOnly: true);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> DeleteAllExactAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuples(writer, tx, schema, iterator);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleDeleteAllExact, tx, writer).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadTuples(resBuf, resSchema);
        }

        private static Transactions.Transaction? GetTx(ITransaction? tx)
        {
            if (tx == null)
            {
                return null;
            }

            if (tx is Transactions.Transaction t)
            {
                return t;
            }

            throw new TransactionException("Unsupported transaction implementation: " + tx.GetType());
        }

        private ValueTask<ClientSocket> GetSocket(Transactions.Transaction? tx)
        {
            if (tx == null)
            {
                return _table.Socket.GetSocketAsync();
            }

            if (tx.FailoverSocket != _table.Socket)
            {
                throw new IgniteClientException("Specified transaction belongs to a different IgniteClient instance.");
            }

            return new ValueTask<ClientSocket>(tx.Socket);
        }

        private async Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            Transactions.Transaction? tx,
            PooledArrayBufferWriter? request = null)
        {
            var socket = await GetSocket(tx).ConfigureAwait(false);

            return await socket.DoOutInOpAsync(clientOp, request).ConfigureAwait(false);
        }

        private async Task<PooledBuffer> DoTupleOutOpAsync(
            ClientOp op,
            ITransaction? transaction,
            IIgniteTuple tuple,
            bool keyOnly = false)
        {
            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            _ser.WriteTuple(writer, tx, schema, tuple, keyOnly);

            return await DoOutInOpAsync(op, tx, writer).ConfigureAwait(false);
        }
    }
}
