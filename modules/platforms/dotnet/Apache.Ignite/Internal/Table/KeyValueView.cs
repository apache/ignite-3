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
    private readonly RecordSerializer<KvPair<TK, TV>> _ser;

    /// <summary>
    /// Initializes a new instance of the <see cref="KeyValueView{TK, TV}"/> class.
    /// </summary>
    /// <param name="table">Table.</param>
    /// <param name="ser">Serializer.</param>
    public KeyValueView(Table table, RecordSerializer<KvPair<TK, TV>> ser)
    {
        _table = table;
        _ser = ser;
    }

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAsync(ITransaction? transaction, TK key)
    {
        // TODO: Delegate to RecordView to cut the code size! Most APIs should be covered.
        IgniteArgumentCheck.NotNull(key, nameof(key));

        using var resBuf = await DoKeyOutOpAsync(ClientOp.TupleGet, transaction, key).ConfigureAwait(false);
        var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

        return _ser.ReadValue(resBuf, resSchema, new(key)).Map(static x => x.Val);
    }

    /// <inheritdoc/>
    public async Task PutAsync(ITransaction? transaction, TK key, TV val)
    {
        var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
        var tx = transaction.ToInternal();

        using var writer = ProtoCommon.GetMessageWriter();

        _ser.Write(writer, tx, schema, new(key, val));
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
        _ser.Write(writer, tx, schema, new(key), keyOnly: true);

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
