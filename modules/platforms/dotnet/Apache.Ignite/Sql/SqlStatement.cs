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

// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
namespace Apache.Ignite.Sql
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// SQL statement.
    /// </summary>
    public sealed record SqlStatement
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlStatement"/> class.
        /// </summary>
        /// <param name="query">Query text.</param>
        /// <param name="timeout">Timeout.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="pageSize">Page size.</param>
        /// <param name="properties">Properties.</param>
        public SqlStatement(
            string query,
            TimeSpan? timeout = null,
            string? schema = null,
            int? pageSize = null,
            IReadOnlyDictionary<string, object?>? properties = null)
        {
            // TODO:
            // 1. Get rid of sessions FULLY - do not make sense for now
            // 2. Put defaults in this class
            // 3. Implicit conversion from string.
            Query = query;
            Timeout = timeout;
            Schema = schema;
            PageSize = pageSize;
            Properties = properties == null ? new Dictionary<string, object?>() : new(properties);
        }

        /// <summary>
        /// Converts a query string to an instance of <see cref="SqlStatement"/>.
        /// </summary>
        /// <param name="query">Query string.</param>
        /// <returns>Statement.</returns>
        public static implicit operator SqlStatement(string query) => ToSqlStatement(query);

        /// <summary>
        /// Gets the query text.
        /// </summary>
        public string Query { get; init; }

        /// <summary>
        /// Gets the query timeout (zero means no timeout).
        /// </summary>
        public TimeSpan? Timeout { get; init; }

        /// <summary>
        /// Gets the SQL schema name.
        /// </summary>
        public string? Schema { get; init; }

        /// <summary>
        /// Gets the number of rows per data page.
        /// </summary>
        public int? PageSize { get; init; }

        /// <summary>
        /// Gets the property bag.
        /// </summary>
        public IReadOnlyDictionary<string, object?> Properties { get; init; }

        /// <summary>
        /// Converts a query string to an instance of <see cref="SqlStatement"/>.
        /// </summary>
        /// <param name="query">Query string.</param>
        /// <returns>Statement.</returns>
        public static SqlStatement ToSqlStatement(string query) => new(query);
    }
}
