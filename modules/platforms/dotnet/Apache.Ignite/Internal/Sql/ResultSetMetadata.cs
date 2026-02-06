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

namespace Apache.Ignite.Internal.Sql
{
    using System.Collections.Generic;
    using Common;
    using Ignite.Sql;
    using Ignite.Table.Mapper;

    /// <summary>
    /// Result set metadata.
    /// </summary>
    internal sealed record ResultSetMetadata : IResultSetMetadata, IMapperSchema
    {
        private readonly ColumnMetadata[] _columns;

        /** Column index by name. Initialized on first access. */
        private Dictionary<string, int>? _indices;

        /// <summary>
        /// Initializes a new instance of the <see cref="ResultSetMetadata"/> class.
        /// </summary>
        /// <param name="columns">Columns.</param>
        internal ResultSetMetadata(ColumnMetadata[] columns)
        {
            _columns = columns;
        }

        /// <inheritdoc/>
        public IReadOnlyList<IColumnMetadata> Columns => _columns;

        /// <inheritdoc/>
        IReadOnlyList<IMapperColumn> IMapperSchema.Columns => _columns;

        /// <inheritdoc/>
        public int IndexOf(string columnName)
        {
            var indices = _indices;

            if (indices == null)
            {
                indices = new Dictionary<string, int>(_columns.Length);

                for (var i = 0; i < _columns.Length; i++)
                {
                    indices[_columns[i].Name] = i;
                }

                _indices = indices;
            }

            return indices.GetValueOrDefault(columnName, -1);
        }

        /// <inheritdoc/>
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .AppendList(Columns)
                .Build();
    }
}
