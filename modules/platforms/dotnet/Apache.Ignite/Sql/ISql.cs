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
    using System.Data.Common;
    using System.Threading.Tasks;
    using Table;
    using Transactions;

    /// <summary>
    /// Ignite SQL query facade.
    /// </summary>
    public interface ISql
    {
        /// <summary>
        /// Executes single SQL statement and returns rows as tuples (<see cref="IIgniteTuple"/>).
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>SQL result set.</returns>
        Task<IResultSet<IIgniteTuple>> ExecuteAsync(ITransaction? transaction, SqlStatement statement, params object?[]? args);

        /// <summary>
        /// Executes single SQL statement and returns rows deserialized into the specified user type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <typeparam name="T">Row type.</typeparam>
        /// <returns>SQL result set.</returns>
        Task<IResultSet<T>> ExecuteAsync<T>(ITransaction? transaction, SqlStatement statement, params object?[]? args);

        /// <summary>
        /// Executes single SQL statement and returns a <see cref="DbDataReader"/> to consume them in an efficient, forward-only way.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>Data reader.</returns>
        Task<IgniteDbDataReader> ExecuteReaderAsync(ITransaction? transaction, SqlStatement statement, params object?[]? args);

        /// <summary>
        /// Executes a multi-statement SQL query.
        /// </summary>
        /// <param name="script">Script.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task ExecuteScriptAsync(SqlStatement script, params object?[]? args);
    }
}
