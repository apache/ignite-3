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

using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Ignite.Transactions;
using Ignite.Table;
using Serialization;

/// <summary>
/// Generic key-value view.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
internal sealed class KeyValueView<TK, TV> : IKeyValueView<TK, TV>
{
    /** Record view. */
    private readonly RecordView<KvPair<TK, TV>> _recordView;

    /// <summary>
    /// Initializes a new instance of the <see cref="KeyValueView{TK, TV}"/> class.
    /// </summary>
    /// <param name="recordView">Record view.</param>
    public KeyValueView(RecordView<KvPair<TK, TV>> recordView)
    {
        _recordView = recordView;
    }

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAsync(ITransaction? transaction, TK key) =>
        (await _recordView.GetAsync(transaction, new(key))).Map(static x => x.Val);

    /// <inheritdoc/>
    public Task<IDictionary<TK, TV>> GetAllAsync(ITransaction? transaction, IEnumerable<TK> keys)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public async Task<bool> ContainsAsync(ITransaction? transaction, TK key) =>
        await _recordView.ContainsKey(transaction, new(key));

    /// <inheritdoc/>
    public async Task PutAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.UpsertAsync(transaction, new(key, val));

    /// <inheritdoc/>
    public Task PutAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndPutAsync(ITransaction? transaction, TK key, TV val) =>
        (await _recordView.GetAndUpsertAsync(transaction, new KvPair<TK, TV>(key, val))).Map(static x => x.Val);

    /// <inheritdoc/>
    public Task<bool> PutIfAbsentAsync(ITransaction? transaction, TK key, TV val)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<bool> RemoveAsync(ITransaction? transaction, TK key)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<bool> RemoveAsync(ITransaction? transaction, TK key, TV val)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<TK> keys)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<Option<TV>> GetAndRemoveAsync(ITransaction? transaction, TK key)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV val)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV oldVal, TV newVal)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<Option<TV>> GetAndReplaceAsync(ITransaction? transaction, TK key, TV val)
    {
        throw new System.NotImplementedException();
    }
}
