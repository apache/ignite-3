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
using System.Linq;
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
    where TK : notnull
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
        (await _recordView.GetAsync(transaction, key)).Map(static x => x.Val);

    /// <inheritdoc/>
    public async Task<IDictionary<TK, TV>> GetAllAsync(ITransaction? transaction, IEnumerable<TK> keys)
    {
        // TODO: Avoid list allocation, read directly into dict.
        var list = await _recordView.GetAllAsync(transaction, keys.Select(static k => ToKv(k)));
        var res = new Dictionary<TK, TV>(list.Count);

        foreach (var ((key, val), hasVal) in list)
        {
            if (hasVal)
            {
                res[key] = val;
            }
        }

        return res;
    }

    /// <inheritdoc/>
    public async Task<bool> ContainsAsync(ITransaction? transaction, TK key) =>
        await _recordView.ContainsKey(transaction, key);

    /// <inheritdoc/>
    public async Task PutAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.UpsertAsync(transaction, new(key, val));

    /// <inheritdoc/>
    public async Task PutAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs) =>
        await _recordView.UpsertAllAsync(transaction, pairs.Select(static x => ToKv(x)));

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndPutAsync(ITransaction? transaction, TK key, TV val) =>
        (await _recordView.GetAndUpsertAsync(transaction, new KvPair<TK, TV>(key, val))).Map(static x => x.Val);

    /// <inheritdoc/>
    public async Task<bool> PutIfAbsentAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.InsertAsync(transaction, new(key, val));

    /// <inheritdoc/>
    public async Task<bool> RemoveAsync(ITransaction? transaction, TK key) =>
        await _recordView.DeleteAsync(transaction, key);

    /// <inheritdoc/>
    public async Task<bool> RemoveAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.DeleteAsync(transaction, new(key, val));

    /// <inheritdoc/>
    public async Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<TK> keys)
    {
        // TODO: Avoid LINQ and list allocation, read into final list.
        var skippedRecs = await _recordView.DeleteAllAsync(transaction, keys.Select(static k => ToKv(k)));

        return skippedRecs.Select(x => x.Key).ToList();
    }

    /// <inheritdoc/>
    public async Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs)
    {
        // TODO: Avoid LINQ and list allocation, read into final list.
        var skippedRecs = await _recordView.DeleteAllExactAsync(transaction, pairs.Select(static x => ToKv(x)));

        return skippedRecs.Select(x => x.Key).ToList();
    }

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndRemoveAsync(ITransaction? transaction, TK key) =>
        (await _recordView.GetAndDeleteAsync(transaction, key)).Map(static x => x.Val);

    /// <inheritdoc/>
    public async Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.ReplaceAsync(transaction, new(key, val));

    /// <inheritdoc/>
    public async Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV oldVal, TV newVal) =>
        await _recordView.ReplaceAsync(transaction, new(key, oldVal), new(key, newVal));

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndReplaceAsync(ITransaction? transaction, TK key, TV val) =>
        (await _recordView.GetAndReplaceAsync(transaction, new(key, val))).Map(static x => x.Val);

    private static KvPair<TK, TV> ToKv(KeyValuePair<TK, TV> x) => new(x.Key, x.Value);

    private static KvPair<TK, TV> ToKv(TK k) => new(k);
}
