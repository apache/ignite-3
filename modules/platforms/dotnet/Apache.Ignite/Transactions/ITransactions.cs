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

namespace Apache.Ignite.Transactions
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;

    /// <summary>
    /// Ignite transactions API.
    /// </summary>
    public interface ITransactions
    {
        /// <summary>
        /// Starts a new transaction.
        /// </summary>
        /// <param name="options">Transaction options.</param>
        /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
        ValueTask<ITransaction> BeginAsync(TransactionOptions options);

        /// <summary>
        /// Starts a new transaction.
        /// </summary>
        /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
        ValueTask<ITransaction> BeginAsync() => BeginAsync(default);

        /// <summary>
        /// Runs the specified function within a transaction. The transaction is committed automatically.
        /// <para />
        /// The transaction is rolled back if the function throws an exception.
        /// <example>
        /// <code>
        /// const decimal amount = 1000;
        /// IKeyValueView&lt;int, Account&gt; view = Table.GetKeyValueView&lt;int, Account&gt;();
        /// await Client.Transactions.RunInTransactionAsync(async tx =>
        /// {
        ///     var acc1 = await view.GetAsync(tx, 1);
        ///     var acc2 = await view.GetAsync(tx, 2);
        ///     acc1.Value.Amount += amount;
        ///     acc2.Value.Amount -= amount;
        ///     await view.PutAsync(tx, 1, acc1.Value);
        ///     await view.PutAsync(tx, 2, acc2.Value);
        /// });
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="func">Function.</param>
        /// <param name="options">Transaction options.</param>
        /// <typeparam name="T">Result type.</typeparam>
        /// <returns>Function result.</returns>
        [SuppressMessage(
            "Reliability",
            "CA2007:Consider calling ConfigureAwait on the awaited task",
            Justification = "False positive, ConfigureAwait is present.")]
        async Task<T> RunInTransactionAsync<T>(
            Func<ITransaction, Task<T>> func,
            TransactionOptions options = default)
        {
            // TODO IGNITE-25401 Retries.
            await using var tx = await BeginAsync(options).ConfigureAwait(false);
            var res = await func(tx).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);

            return res;
        }

        /// <summary>
        /// Runs the specified function within a transaction. The transaction is committed automatically.
        /// <para />
        /// The transaction is rolled back if the function throws an exception.
        /// <example>
        /// <code>
        /// const decimal amount = 1000;
        /// IKeyValueView&lt;int, Account&gt; view = Table.GetKeyValueView&lt;int, Account&gt;();
        /// decimal resultAmount = await Client.Transactions.RunInTransactionAsync(async tx =>
        /// {
        ///     var acc1 = await view.GetAsync(tx, 1);
        ///     var acc2 = await view.GetAsync(tx, 2);
        ///     acc1.Value.Amount += amount;
        ///     acc2.Value.Amount -= amount;
        ///     await view.PutAsync(tx, 1, acc1.Value);
        ///     await view.PutAsync(tx, 2, acc2.Value);
        ///     return acc1.Value.Amount;
        /// });
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="func">Function.</param>
        /// <param name="options">Transaction options.</param>
        /// <returns>Function result.</returns>
        [SuppressMessage(
            "Reliability",
            "CA2007:Consider calling ConfigureAwait on the awaited task",
            Justification = "False positive, ConfigureAwait is present.")]
        async Task RunInTransactionAsync(
            Func<ITransaction, Task> func,
            TransactionOptions options = default)
        {
            // TODO IGNITE-25401 Retries.
            await using var tx = await BeginAsync(options).ConfigureAwait(false);
            await func(tx).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
        }
    }
}
