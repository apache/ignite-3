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

namespace Apache.Ignite.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Query result set.
    /// <para />
    /// Can not be enumerated more than once. The implementation is lazy and retrieves data pages on demand
    /// (while iterating with <see cref="IAsyncEnumerable{T}"/> or when <see cref="GetAllAsync"/> is called).
    /// Page size is defined in <see cref="SqlStatement.PageSize"/>.
    /// </summary>
    /// <typeparam name="T">Row type.</typeparam>
    public interface IResultSet<T> : IAsyncEnumerable<T>, IAsyncDisposable, IDisposable
        where T : class // TODO: Remove class constraint (IGNITE-16355)
    {
        /// <summary>
        /// Gets result set metadata when <see cref="HasRowSet"/> is <c>true</c>, otherwise <c>null</c>.
        /// </summary>
        IResultSetMetadata? Metadata { get; }

        /// <summary>
        /// Gets a value indicating whether this result set contains a collection of rows.
        /// </summary>
        bool HasRowSet { get; }

        /// <summary>
        /// Gets the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.),
        /// or 0 if the statement returns nothing (such as "ALTER TABLE", etc), or -1 if not applicable.
        /// </summary>
        long AffectedRows { get; }

        /// <summary>
        /// Gets a value indicating whether a conditional query (such as "CREATE TABLE IF NOT EXISTS") was applied successfully.
        /// </summary>
        bool WasApplied { get; }

        /// <summary>
        /// Gets all result set rows as list.
        /// <para />
        /// Can not be called multiple times - the underlying server-side result set is closed as soon
        /// as the last page of data is retrieved, and client-side buffer is also released to reduce memory usage.
        /// </summary>
        /// <returns>All result set rows as list.</returns>
        ValueTask<List<T>> GetAllAsync(); // TODO: Measure against IAsyncEnumerable approach.
    }
}
