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
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Internal.Table.Serialization;
    using Table;
    using Table.Mapper;
    using Transactions;

    /// <summary>
    /// Ignite SQL query facade.
    /// </summary>
    public interface ISql
    {
        /// <summary>
        /// Executes a single SQL statement and returns rows as tuples (<see cref="IIgniteTuple"/>).
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>SQL result set.</returns>
        Task<IResultSet<IIgniteTuple>> ExecuteAsync(ITransaction? transaction, SqlStatement statement, params object?[]? args)
            => ExecuteAsync(transaction, statement, CancellationToken.None, args);

        /// <summary>
        /// Executes a single SQL statement and returns rows as tuples (<see cref="IIgniteTuple"/>).
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>SQL result set.</returns>
        Task<IResultSet<IIgniteTuple>> ExecuteAsync(ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args);

        /// <summary>
        /// Executes a single SQL statement and returns rows deserialized into the specified user type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <typeparam name="T">Row type.</typeparam>
        /// <returns>SQL result set.</returns>
        [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
        Task<IResultSet<T>> ExecuteAsync<T>(
            ITransaction? transaction, SqlStatement statement, params object?[]? args)
            => ExecuteAsync<T>(transaction, statement, CancellationToken.None, args);

        /// <summary>
        /// Executes a single SQL statement and returns rows deserialized into the specified user type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <typeparam name="T">Row type.</typeparam>
        /// <returns>SQL result set.</returns>
        [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
        Task<IResultSet<T>> ExecuteAsync<T>(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args);

        /// <summary>
        /// Executes a single SQL statement and returns rows deserialized into the specified user type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="mapper">Mapper for the result type.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <typeparam name="T">Row type.</typeparam>
        /// <returns>SQL result set.</returns>
        Task<IResultSet<T>> ExecuteAsync<T>(
            ITransaction? transaction,
            IMapper<T> mapper,
            SqlStatement statement,
            CancellationToken cancellationToken,
            params object?[]? args);

        /// <summary>
        /// Executes a single SQL statement and returns a <see cref="DbDataReader"/> to consume them in an efficient, forward-only way.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>Data reader.</returns>
        Task<IgniteDbDataReader> ExecuteReaderAsync(
            ITransaction? transaction, SqlStatement statement, params object?[]? args)
            => ExecuteReaderAsync(transaction, statement, CancellationToken.None, args);

        /// <summary>
        /// Executes a single SQL statement and returns a <see cref="DbDataReader"/> to consume them in an efficient, forward-only way.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>Data reader.</returns>
        Task<IgniteDbDataReader> ExecuteReaderAsync(
            ITransaction? transaction, SqlStatement statement, CancellationToken cancellationToken, params object?[]? args);

        /// <summary>
        /// Executes a multi-statement SQL query.
        /// </summary>
        /// <param name="script">Script.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task ExecuteScriptAsync(SqlStatement script, params object?[]? args)
            => ExecuteScriptAsync(script, CancellationToken.None, args);

        /// <summary>
        /// Executes a multi-statement SQL query.
        /// </summary>
        /// <param name="script">Script.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task ExecuteScriptAsync(SqlStatement script, CancellationToken cancellationToken, params object?[]? args);

        /// <summary>
        /// Executes an SQL statement for every set of arguments and returns the number of affected rows for each statement execution.
        /// <para />
        /// Only DML statements (INSERT, UPDATE, DELETE) are supported.
        /// <para />
        /// <example>
        /// <code>
        /// long[] res = await sql.ExecuteBatchAsync(
        ///     transaction: null,
        ///     "INSERT INTO Person (Id, Name) VALUES (?, ?)",
        ///     [[1, "Alice"], [2, "Bob"], [3, "Charlie"]]);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="transaction">Transaction.</param>
        /// <param name="statement">Statement to execute once for every entry in <paramref name="args"/>.</param>
        /// <param name="args">Batched arguments. The specified statement will be executed once for each entry in this collection. Cannot be empty or contain empty rows.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The number of affected rows for each set of arguments. The size of the returned array will match the size of <paramref name="args"/>.</returns>
        Task<long[]> ExecuteBatchAsync(
            ITransaction? transaction,
            SqlStatement statement,
            IEnumerable<IEnumerable<object?>> args,
            CancellationToken cancellationToken = default);
    }
}
