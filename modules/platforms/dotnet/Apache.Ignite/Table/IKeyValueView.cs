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

namespace Apache.Ignite.Table;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sql;
using Transactions;

/// <summary>
/// Key-value view provides access to table records in form of separate key and value parts.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
public interface IKeyValueView<TK, TV> : IDataStreamerTarget<KeyValuePair<TK, TV>>
    where TK : notnull
{
    /// <summary>
    /// Gets a value associated with the given key.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains the value for the specified key, or an <see cref="Option{T}"/> instance without a value
    /// when specified key is not present in the table.
    /// </returns>
    Task<Option<TV>> GetAsync(ITransaction? transaction, TK key);

    /// <summary>
    /// Gets multiple records by keys.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="keys">Keys.</param>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains a dictionary with specified keys and their values. If a record for a particular key does not exist,
    /// it will not be present in the resulting dictionary.
    /// </returns>
    Task<IDictionary<TK, TV>> GetAllAsync(ITransaction? transaction, IEnumerable<TK> keys);

    /// <summary>
    /// Determines if the table contains an entry for the specified key.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result is <c>true</c> if a value exists for the specified key, and <c>false</c> otherwise.
    /// </returns>
    Task<bool> ContainsAsync(ITransaction? transaction, TK key);

    /// <summary>
    /// Puts a value with a given key.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="val">Value.</param>
    /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation.</returns>
    Task PutAsync(ITransaction? transaction, TK key, TV val);

    /// <summary>
    /// Puts multiple key-value pairs.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="pairs">Pairs.</param>
    /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation.</returns>
    Task PutAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs);

    /// <summary>
    /// Puts a value with a given key and returns previous value for that key.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="val">Value.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains the value for the specified key, or an <see cref="Option{T}"/> instance without a value
    /// when specified key is not present in the table.
    /// </returns>
    Task<Option<TV>> GetAndPutAsync(ITransaction? transaction, TK key, TV val);

    /// <summary>
    /// Puts a value with a given key if the specified key is not present in the table.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="val">Value.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains a value indicating whether the value was added to the table.
    /// </returns>
    Task<bool> PutIfAbsentAsync(ITransaction? transaction, TK key, TV val);

    /// <summary>
    /// Removes a value with a given key from the table.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains a value indicating whether the key was removed from the table.
    /// </returns>
    Task<bool> RemoveAsync(ITransaction? transaction, TK key);

    /// <summary>
    /// Removes a value with a given key from the table only if it is equal to the specified value.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="val">Val.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains a value indicating whether the key was removed from the table.
    /// </returns>
    Task<bool> RemoveAsync(ITransaction? transaction, TK key, TV val);

    /// <summary>
    /// Removes values with given keys from the table.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="keys">Keys.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains skipped keys.
    /// </returns>
    Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<TK> keys);

    /// <summary>
    /// Removes records with given keys and values from the table.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="pairs">Keys.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains skipped keys.
    /// </returns>
    Task<IList<TK>> RemoveAllAsync(ITransaction? transaction, IEnumerable<KeyValuePair<TK, TV>> pairs);

    /// <summary>
    /// Gets and removes a value associated with the given key.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains a value indicating whether the key was removed from the table.
    /// </returns>
    Task<Option<TV>> GetAndRemoveAsync(ITransaction? transaction, TK key);

    /// <summary>
    /// Replaces a record with the same key columns if it exists, otherwise does nothing.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="val">Value.</param>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains a value indicating whether a record with the specified key was replaced.
    /// </returns>
    Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV val);

    /// <summary>
    /// Replaces a record with a new one only if all existing columns have the same values
    /// as the specified <paramref name="oldVal"/>.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="oldVal">Old value.</param>
    /// <param name="newVal">New value.</param>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains a value indicating whether a record was replaced.
    /// </returns>
    Task<bool> ReplaceAsync(ITransaction? transaction, TK key, TV oldVal, TV newVal);

    /// <summary>
    /// Replaces a record with the same key columns if it exists.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">Key.</param>
    /// <param name="val">Value.</param>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the previous value for the given key, or empty <see cref="Option{T}"/> if it did not exist.
    /// </returns>
    Task<Option<TV>> GetAndReplaceAsync(ITransaction? transaction, TK key, TV val);

    /// <summary>
    /// Gets a <see cref="IQueryable{T}"/> to perform Ignite SQL queries using LINQ
    /// (see <see href="https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/" />).
    /// <para />
    /// Use <see cref="IgniteQueryableExtensions.ToResultSetAsync{T}"/> to materialize query results asynchronously.
    /// </summary>
    /// <param name="transaction">Optional transaction.</param>
    /// <param name="options">Options.</param>
    /// <returns><see cref="IQueryable{T}"/>.</returns>
    IQueryable<KeyValuePair<TK, TV>> AsQueryable(ITransaction? transaction = null, QueryableOptions? options = null);
}
