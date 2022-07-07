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
    using System.Threading.Tasks;
    using Transactions;

    /// <summary>
    /// SQL Session executes queries and holds default query settings.
    /// </summary>
    public interface ISqlSession : IAsyncDisposable // TODO: Rename to ISession, along with other things?
    {
        /// <summary>
        /// Executes single SQL statement.
        /// </summary>
        /// <param name="transaction">Optional transaction.</param>
        /// <param name="statement">Statement to execute.</param>
        /// <param name="args">Arguments for the statement.</param>
        /// <returns>SQL result set.</returns>
        Task<IResultSet> ExecuteAsync(ITransaction? transaction, SqlStatement statement, params object[] args);
    }
}
