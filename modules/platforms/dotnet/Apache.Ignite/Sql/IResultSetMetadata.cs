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

    /// <summary>
    /// SQL result set metadata.
    /// </summary>
    public interface IResultSetMetadata
    {
        /// <summary>
        /// Gets the columns in the same order as they appear in the result set data.
        /// </summary>
        public IReadOnlyList<IColumnMetadata> Columns { get; }

        /// <summary>
        /// Gets the index of the specified column, or -1 when there is no column with the specified name.
        /// </summary>
        /// <param name="columnName">Column name.</param>
        /// <returns>Column index.</returns>
        public int IndexOf(string columnName);
    }
}
