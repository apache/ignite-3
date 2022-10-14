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

namespace Apache.Ignite.Table
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Transactions;

    /// <summary>
    /// Record view interface provides methods to access table records.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    public interface IRecordView<T>
    {
        /// <summary>
        /// Gets a record by key.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="key">A record with key columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a record with all columns.
        /// </returns>
        Task<Option<T>> GetAsync(ITransaction? transaction, T key);

        /// <summary>
        /// Gets multiple records by keys.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="keys">Collection of records with key columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains matching records with all columns filled from the table. The order of collection
        /// elements is guaranteed to be the same as the order of <paramref name="keys"/>. If a record does not exist,
        /// the element at the corresponding index of the resulting collection will be <c>null</c>.
        /// </returns>
        Task<IList<Option<T>>> GetAllAsync(ITransaction? transaction, IEnumerable<T> keys);

        /// <summary>
        /// Inserts a record into the table if it does not exist or replaces the existing one.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">Record to upsert.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task UpsertAsync(ITransaction? transaction, T record);

        /// <summary>
        /// Inserts multiple records into the table, replacing existing ones.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Records to upsert.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task UpsertAllAsync(ITransaction? transaction, IEnumerable<T> records);

        /// <summary>
        /// Inserts a record into the table if it does not exist or replaces the existing one.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">Record to upsert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains replaced record or null if it did not exist.
        /// </returns>
        Task<Option<T>> GetAndUpsertAsync(ITransaction? transaction, T record);

        /// <summary>
        /// Inserts a record into the table if it does not exist.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">Record to insert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether the record was inserted. Returns <c>false</c> if a
        /// record with the same key already exists.
        /// </returns>
        Task<bool> InsertAsync(ITransaction? transaction, T record);

        /// <summary>
        /// Inserts multiple records into the table, skipping existing ones.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Records to insert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains skipped records.
        /// </returns>
        Task<IList<T>> InsertAllAsync(ITransaction? transaction, IEnumerable<T> records);

        /// <summary>
        /// Replaces a record with the same key columns if it exists, otherwise does nothing.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">Record to insert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether a record with the specified key was replaced.
        /// </returns>
        Task<bool> ReplaceAsync(ITransaction? transaction, T record);

        /// <summary>
        /// Replaces a record with a new one only if all existing columns have the same values
        /// as the specified <paramref name="record"/>.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">Record to replace.</param>
        /// <param name="newRecord">Record to replace with.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether a record was replaced.
        /// </returns>
        Task<bool> ReplaceAsync(ITransaction? transaction, T record, T newRecord);

        /// <summary>
        /// Replaces a record with the same key columns if it exists.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">Record to insert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains the previous value for the given key, or <c>null</c> if it did not exist.
        /// </returns>
        Task<Option<T>> GetAndReplaceAsync(ITransaction? transaction, T record);

        /// <summary>
        /// Deletes a record with the specified key.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="key">A record with key columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether a record with the specified key was deleted.
        /// </returns>
        Task<bool> DeleteAsync(ITransaction? transaction, T key);

        /// <summary>
        /// Deletes a record only if all existing columns have the same values as the specified <paramref name="record"/>.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="record">A record with all columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether a record was deleted.
        /// </returns>
        Task<bool> DeleteExactAsync(ITransaction? transaction, T record);

        /// <summary>
        /// Gets and deletes a record with the specified key.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="key">A record with key columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains deleted record or <c>null</c> if it did not exist.
        /// </returns>
        Task<Option<T>> GetAndDeleteAsync(ITransaction? transaction, T key);

        /// <summary>
        /// Deletes multiple records. If one or more keys do not exist, other records are still deleted.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="keys">Record keys to delete.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains records from <paramref name="keys"/> that did not exist.
        /// </returns>
        Task<IList<T>> DeleteAllAsync(ITransaction? transaction, IEnumerable<T> keys);

        /// <summary>
        /// Deletes multiple exactly matching records.  If one or more records do not exist,
        /// other records are still deleted.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Records to delete.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains records from <paramref name="records"/> that did not exist.
        /// </returns>
        Task<IList<T>> DeleteAllExactAsync(ITransaction? transaction, IEnumerable<T> records);
    }
}
