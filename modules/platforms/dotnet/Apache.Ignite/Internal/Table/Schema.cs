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
    using System.Diagnostics;
    using System.Linq;
    using Proto.BinaryTuple;

    /// <summary>
    /// Schema.
    /// </summary>
    /// <param name="Version">Version.</param>
    /// <param name="TableId">Table id.</param>
    /// <param name="ColocationColumnCount">Colocation column count.</param>
    /// <param name="Columns">Columns in schema order.</param>
    /// <param name="KeyColumns">Key part columns.</param>
    /// <param name="ValColumns">Val part columns.</param>
    /// <param name="ColumnsByName">Column name map.</param>
    internal sealed record Schema(
        int Version,
        int TableId,
        int ColocationColumnCount,
        IReadOnlyList<Column> Columns,
        IReadOnlyList<Column> KeyColumns,
        IReadOnlyList<Column> ValColumns,
        IReadOnlyDictionary<string, Column> ColumnsByName)
    {
        /// <summary>
        /// Gets column by name.
        /// </summary>
        /// <param name="name">Column name.</param>
        /// <returns>Column or null.</returns>
        public Column? GetColumn(string name) => ColumnsByName!.GetValueOrDefault(IgniteTupleCommon.ParseColumnName(name), null);

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
            int colocationColumnCount = 0;

            foreach (var column in columns)
            {
                if (column.IsKey)
                {
                    Debug.Assert(keyColumns[column.KeyIndex] == null, "Duplicate key index: " + column);
                    keyColumns[column.KeyIndex] = column;
                }
                else
                {
                    valColumns.Add(column);
                }

                if (column.IsColocation)
                {
                    colocationColumnCount++;
                }
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            Debug.Assert(keyColumns.All(x => x != null), "Some key columns are missing");
            Debug.Assert(columns.Count == 0 || colocationColumnCount > 0, "No hashed columns");

            var columnMap = new Dictionary<string, Column>(columns.Count);
            foreach (var column in columns)
            {
                columnMap[IgniteTupleCommon.ParseColumnName(column.Name)] = column;
            }

            return new Schema(version, tableId, colocationColumnCount, columns, keyColumns, valColumns, columnMap);
        }

        /// <summary>
        /// Gets columns.
        /// </summary>
        /// <param name="keyOnly">Key only flag.</param>
        /// <returns>Columns according to the key flag.</returns>
        public IReadOnlyList<Column> GetColumnsFor(bool keyOnly) => keyOnly ? KeyColumns : Columns;
    }
}
