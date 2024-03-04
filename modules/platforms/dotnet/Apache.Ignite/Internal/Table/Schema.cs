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
    using System.Linq;
    using Proto.BinaryTuple;

    /// <summary>
    /// Schema.
    /// </summary>
    /// <param name="Version">Version.</param>
    /// <param name="TableId">Table id.</param>
    /// <param name="HashedColumnCount">Hashed column count.</param>
    /// <param name="Columns">Columns in schema order.</param>
    /// <param name="KeyColumns">Key part columns.</param>
    /// <param name="ValColumns">Val part columns.</param>
    internal sealed record Schema(
        int Version,
        int TableId,
        int HashedColumnCount,
        IReadOnlyList<Column> Columns,
        IReadOnlyList<Column> KeyColumns,
        IReadOnlyList<Column> ValColumns) : IHashedColumnIndexProvider
    {
        /// <inheritdoc/>
        public int HashedColumnOrder(int index) => Columns[index].ColocationIndex;

        /// <summary>
        /// Create schema instance.
        /// </summary>
        /// <param name="version">Version.</param>
        /// <param name="tableId">Table ID.</param>
        /// <param name="columns">Columns.</param>
        /// <returns>Schema.</returns>
        public static Schema CreateInstance(int version, int tableId, IReadOnlyList<Column> columns)
        {
            var keyColumnCount = columns.Count(static x => x.IsKey);
            var keyColumns = new Column[keyColumnCount];
            var valColumns = new List<Column>();
            int hashedColumnCount = 0;

            foreach (var column in columns)
            {
                // TODO: Add assertions for column indexes - see Java code.
                if (column.IsKey)
                {
                    keyColumns[column.KeyIndex] = column;
                }
                else
                {
                    valColumns.Add(column);
                }

                if (column.IsColocation)
                {
                    hashedColumnCount++;
                }
            }

            return new Schema(version, tableId, hashedColumnCount, columns, keyColumns, valColumns);
        }

        /// <summary>
        /// Gets columns.
        /// </summary>
        /// <param name="keyOnly">Key only flag.</param>
        /// <returns>Columns according to the key flag.</returns>
        public IReadOnlyList<Column> GetColumnsFor(bool keyOnly) => keyOnly ? KeyColumns : Columns;
    }
}
