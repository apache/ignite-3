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

    /// <summary>
    /// Table view interface provides methods to access table records.
    /// <para />
    /// TODO: All APIs (IGNITE-15430).
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    public interface ITableView<T>
        where T : class
    {
        /// <summary>
        /// Gets a record by key.
        /// </summary>
        /// <param name="key">A record with key columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a record with all columns.
        /// </returns>
        Task<T?> GetAsync(T key);

        /// <summary>
        /// Gets multiple records by keys.
        /// </summary>
        /// <param name="keys">Collection of records with key columns set.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains matching records with all columns.
        /// </returns>
        Task<IList<T>> GetAllAsync(IEnumerable<T> keys);

        /// <summary>
        /// Inserts a record into the table if it does not exist or replaces the existing one.
        /// </summary>
        /// <param name="record">Record to upsert.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task UpsertAsync(T record);

        /// <summary>
        /// Inserts multiple records into the table, replacing existing ones.
        /// </summary>
        /// <param name="records">Records to upsert.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task UpsertAllAsync(IEnumerable<T> records);

        /// <summary>
        /// Inserts a record into the table if it does not exist or replaces the existing one.
        /// </summary>
        /// <param name="record">Record to upsert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains replaced record or null if it did not exist.
        /// </returns>
        Task<T?> GetAndUpsertAsync(T record);

        /// <summary>
        /// Inserts a record into the table if it does not exist.
        /// </summary>
        /// <param name="record">Record to insert.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether the record was inserted. Returns <c>false</c> if a
        /// record with the same key already exists.
        /// </returns>
        Task InsertAsync(T record);
    }
}
