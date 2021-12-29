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

namespace Apache.Ignite.Internal.Transactions
{
    using System.Threading.Tasks;
    using Ignite.Transactions;

    /// <summary>
    /// Ignite transaction.
    /// </summary>
    internal class Transaction : ITransaction
    {
        /** Transaction id. */
        private readonly long _id;

        /** Underlying connection. */
        private readonly ClientSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction"/> class.
        /// </summary>
        /// <param name="id">Transaction id.</param>
        /// <param name="socket">Associated connection.</param>
        public Transaction(long id, ClientSocket socket)
        {
            _id = id;
            _socket = socket;
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task CommitAsync()
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public Task RollbackAsync()
        {
            throw new System.NotImplementedException();
        }
    }
}
