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

    /// <summary>
    /// SQL session configuration.
    /// </summary>
    public sealed record SqlSessionConfiguration
    {
        /// <summary>
        /// Default SQL schema name.
        /// </summary>
        public const string DefaultSchema = "PUBLIC";

        /// <summary>
        /// Default number of rows per data page.
        /// </summary>
        public const int DefaultPageSize = 1024;

        /// <summary>
        /// Default query timeout (zero means no timeout).
        /// </summary>
        public static readonly TimeSpan DefaultTimeout = TimeSpan.Zero;

        /// <summary>
        /// Gets or sets the query timeout (zero means no timeout).
        /// </summary>
        public TimeSpan Timeout { get; set; } = DefaultTimeout;

        /// <summary>
        /// Gets or sets the SQL schema name.
        /// </summary>
        public string Schema { get; set; } = DefaultSchema;

        /// <summary>
        /// Gets or sets the number of rows per data page.
        /// </summary>
        public int PageSize { get; set; } = DefaultPageSize;

        /// <summary>
        /// Gets the property bag.
        /// </summary>
        public Dictionary<string, object?> Properties { get; } = new();
    }
}
