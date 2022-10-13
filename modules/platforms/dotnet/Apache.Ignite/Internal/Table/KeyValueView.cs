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

namespace Apache.Ignite.Internal.Table;

using System.Threading.Tasks;
using Apache.Ignite.Transactions;
using Buffers;
using Common;
using Ignite.Table;
using Proto;
using Serialization;
using Transactions;

/// <summary>
/// Generic key-value view.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
internal sealed class KeyValueView<TK, TV> : IKeyValueView<TK, TV>
    where TK : class
    where TV : class
{
    /** Table. */
    private readonly Table _table;

    /** Key serializer. */
    private readonly RecordSerializer<TK> _keySer;

    /** Value serializer. */
    private readonly RecordSerializer<TV> _valSer;

    /// <summary>
    /// Initializes a new instance of the <see cref="KeyValueView{TK, TV}"/> class.
    /// </summary>
    /// <param name="table">Table.</param>
    /// <param name="keySer">Key serializer.</param>
    /// <param name="valSer">Value serializer.</param>
    public KeyValueView(Table table, RecordSerializer<TK> keySer, RecordSerializer<TV> valSer)
    {
        _table = table;
        _keySer = keySer;
        _valSer = valSer;
    }

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAsync(ITransaction? transaction, TK key)
    {
        IgniteArgumentCheck.NotNull(key, nameof(key));

        using var resBuf = await DoKeyOutOpAsync(ClientOp.TupleGet, transaction, key).ConfigureAwait(false);
        var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

        // TODO IGNITE-16226
        return _valSer.ReadValue(resBuf, resSchema, default!);
    }

    /// <inheritdoc/>
    public async Task PutAsync(ITransaction? transaction, TK key, TV? val)
    {
        // TODO IGNITE-16226 use a special known type that combines Key and Val - some kind of Pair?
        // In case of tuples we can create a combined tuple.
        var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
        var tx = transaction.ToInternal();

        using var writer = ProtoCommon.GetMessageWriter();

        // TODO IGNITE-16226 We should write key and val into the same BinaryTuple.
        // _keySer.Write(writer, tx, schema.SliceKey(), key);
        // TODO IGNITE-16226 What if val is null?
        // _valSer.Write(writer, tx, schema.SliceVal(), val!);
        await DoOutInOpAsync(ClientOp.TupleUpsert, tx, writer).ConfigureAwait(false);
    }

    private async Task<PooledBuffer> DoKeyOutOpAsync(
        ClientOp op,
        ITransaction? transaction,
        TK key)
    {
        var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
        var tx = transaction.ToInternal();

        using var writer = ProtoCommon.GetMessageWriter();
        _keySer.Write(writer, tx, schema, key, keyOnly: true);

        return await DoOutInOpAsync(op, tx, writer).ConfigureAwait(false);
    }

    private async Task<PooledBuffer> DoOutInOpAsync(
        ClientOp clientOp,
        Transaction? tx,
        PooledArrayBufferWriter? request = null)
    {
        return await _table.Socket.DoOutInOpAsync(clientOp, tx, request).ConfigureAwait(false);
    }
}
