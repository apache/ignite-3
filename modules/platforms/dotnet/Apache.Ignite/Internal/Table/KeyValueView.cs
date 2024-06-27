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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Transactions;
using Common;
using Ignite.Compute;
using Ignite.Sql;
using Ignite.Table;
using Linq;
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
        (await _recordView.GetAsync(transaction, ToKv(key)).ConfigureAwait(false))
        .Select(static x => x.Val);

    /// <inheritdoc/>
    public async Task<IDictionary<TK, TV>> GetAllAsync(ITransaction? transaction, IEnumerable<TK> keys) =>
        await _recordView.GetAllAsync(
            transaction,
            keys.Select(static k => ToKv(k)),
            count => new Dictionary<TK, TV>(count),
            (dict, item) =>
            {
                var ((key, val), hasVal) = item;

                if (hasVal)
                {
                    dict[key] = val;
                }
            }).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<bool> ContainsAsync(ITransaction? transaction, TK key) =>
        await _recordView.ContainsKeyAsync(transaction, ToKv(key)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task PutAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.UpsertAsync(transaction, ToKv(key, val)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task PutAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs) =>
        await _recordView.UpsertAllAsync(transaction, pairs.Select(static x => ToKv(x))).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndPutAsync(ITransaction? transaction, TK key, TV val) =>
        (await _recordView.GetAndUpsertAsync(transaction, new KvPair<TK, TV>(key, val)).ConfigureAwait(false))
        .Select(static x => x.Val);

    /// <inheritdoc/>
    public async Task<bool> PutIfAbsentAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.InsertAsync(transaction, ToKv(key, val)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<bool> RemoveAsync(ITransaction? transaction, TK key) =>
        await _recordView.DeleteAsync(transaction, ToKv(key)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<bool> RemoveAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.DeleteExactAsync(transaction, ToKv(key, val)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<TK> keys)
    {
        IgniteArgumentCheck.NotNull(keys);

        return await _recordView.DeleteAllAsync(
            transaction,
            keys.Select(static k => ToKv(k)),
            resultFactory: static count => count == 0
                ? (IList<TK>)Array.Empty<TK>()
                : new List<TK>(count),
            addAction: static (res, item) => res.Add(item.Key),
            exact: false).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs)
    {
        IgniteArgumentCheck.NotNull(pairs);

        return await _recordView.DeleteAllAsync(
            transaction,
            pairs.Select(static k => ToKv(k)),
            resultFactory: static count => count == 0
                ? (IList<TK>)Array.Empty<TK>()
                : new List<TK>(count),
            addAction: static (res, item) => res.Add(item.Key),
            exact: true).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndRemoveAsync(ITransaction? transaction, TK key) =>
        (await _recordView.GetAndDeleteAsync(transaction, ToKv(key)).ConfigureAwait(false))
        .Select(static x => x.Val);

    /// <inheritdoc/>
    public async Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV val) =>
        await _recordView.ReplaceAsync(transaction, ToKv(key, val)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV oldVal, TV newVal) =>
        await _recordView.ReplaceAsync(transaction, ToKv(key, oldVal), ToKv(key, newVal)).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<Option<TV>> GetAndReplaceAsync(ITransaction? transaction, TK key, TV val) =>
        (await _recordView.GetAndReplaceAsync(transaction, ToKv(key, val)).ConfigureAwait(false))
        .Select(static x => x.Val);

    /// <inheritdoc/>
    public IQueryable<KeyValuePair<TK, TV>> AsQueryable(ITransaction? transaction = null, QueryableOptions? options = null)
    {
        var executor = new IgniteQueryExecutor(_recordView.Sql, transaction, options, _recordView.Table.Socket.Configuration);
        var provider = new IgniteQueryProvider(IgniteQueryParser.Instance, executor, _recordView.Table.Name);

        return new IgniteQueryable<KeyValuePair<TK, TV>>(provider);
    }

    /// <inheritdoc/>
    public async Task StreamDataAsync(
        IAsyncEnumerable<DataStreamerItem<KeyValuePair<TK, TV>>> data,
        DataStreamerOptions? options = null,
        CancellationToken cancellationToken = default) =>
        await _recordView.StreamDataAsync(ToKv(data), options, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public IAsyncEnumerable<TResult> StreamDataAsync<TSource, TPayload, TResult>(
        IAsyncEnumerable<TSource> data,
        Func<TSource, KeyValuePair<TK, TV>> keySelector,
        Func<TSource, TPayload> payloadSelector,
        ReceiverDescriptor<TResult> receiver,
        ICollection<object>? receiverArgs,
        DataStreamerOptions? options,
        CancellationToken cancellationToken = default)
        where TPayload : notnull =>
        _recordView.StreamDataAsync(
            data,
            src => ToKv(keySelector(src)),
            payloadSelector,
            receiver,
            receiverArgs,
            options,
            cancellationToken);

    /// <inheritdoc/>
    public Task StreamDataAsync<TSource, TPayload>(
        IAsyncEnumerable<TSource> data,
        Func<TSource, KeyValuePair<TK, TV>> keySelector,
        Func<TSource, TPayload> payloadSelector,
        IEnumerable<DeploymentUnit> units,
        string receiverClassName,
        ICollection<object>? receiverArgs,
        DataStreamerOptions? options,
        CancellationToken cancellationToken = default)
        where TPayload : notnull =>
        _recordView.StreamDataAsync(
            data,
            src => ToKv(keySelector(src)),
            payloadSelector,
            units,
            receiverClassName,
            receiverArgs,
            options,
            cancellationToken);

    /// <inheritdoc/>
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(_recordView.Table, "Table")
            .Build();

    private static KvPair<TK, TV> ToKv(KeyValuePair<TK, TV> x)
    {
        IgniteArgumentCheck.NotNull(x.Key);

        return new(x.Key, x.Value);
    }

    private static KvPair<TK, TV> ToKv(TK key)
    {
        IgniteArgumentCheck.NotNull(key);

        return new(key);
    }

    private static KvPair<TK, TV> ToKv(TK key, TV val)
    {
        IgniteArgumentCheck.NotNull(key);

        return new(key, val);
    }

    private static async IAsyncEnumerable<DataStreamerItem<KvPair<TK, TV>>> ToKv(
        IAsyncEnumerable<DataStreamerItem<KeyValuePair<TK, TV>>> pairs)
    {
        await foreach (var pair in pairs)
        {
            yield return DataStreamerItem.Create(ToKv(pair.Data), pair.OperationType);
        }
    }
}
