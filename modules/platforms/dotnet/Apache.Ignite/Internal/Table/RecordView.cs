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
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Ignite.Table;
    using Ignite.Transactions;

    /// <summary>
    /// Record view.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal class RecordView<T> : IRecordView<T>
        where T : class
    {
        /** Table. */
        private readonly Table _table;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordView{T}"/> class.
        /// </summary>
        /// <param name="table">Table.</param>
        public RecordView(Table table)
        {
            _table = table;
        }

        /// <inheritdoc/>
        public Task<T?> GetAsync(ITransaction? transaction, T key)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<IList<T?>> GetAllAsync(ITransaction? transaction, IEnumerable<T> keys)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task UpsertAsync(ITransaction? transaction, T record)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task UpsertAllAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<T?> GetAndUpsertAsync(ITransaction? transaction, T record)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<bool> InsertAsync(ITransaction? transaction, T record)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<IList<T>> InsertAllAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<bool> ReplaceAsync(ITransaction? transaction, T record)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<bool> ReplaceAsync(ITransaction? transaction, T record, T newRecord)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<T?> GetAndReplaceAsync(ITransaction? transaction, T record)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<bool> DeleteAsync(ITransaction? transaction, T key)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<bool> DeleteExactAsync(ITransaction? transaction, T record)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<T?> GetAndDeleteAsync(ITransaction? transaction, T key)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<IList<T>> DeleteAllAsync(ITransaction? transaction, IEnumerable<T> keys)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<IList<T>> DeleteAllExactAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            throw new System.NotImplementedException();
        }
    }
}
