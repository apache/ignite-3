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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Ignite.Table.Mapper;
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
    /// <param name="ColocationColumns">Colocation columns.</param>
    /// <param name="ColumnsByName">Column name map.</param>
    /// <param name="HashedColumnIndexProvider">Hashed column index provider.</param>
    /// <param name="KeyOnlyHashedColumnIndexProvider">Hashed column index provider for key-only mode.</param>
    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Reviewed.")]
    internal sealed record Schema(
        int Version,
        int TableId,
        int ColocationColumnCount,
        Column[] Columns,
        Column[] KeyColumns,
        Column[] ValColumns,
        Column[] ColocationColumns,
        IReadOnlyDictionary<string, Column> ColumnsByName,
        IHashedColumnIndexProvider HashedColumnIndexProvider,
        IHashedColumnIndexProvider KeyOnlyHashedColumnIndexProvider)
    {
        private readonly Lazy<IMapperSchema> _mapperSchema =
            new(() => new MapperSchema(Columns.Cast<IMapperColumn>().ToArray()));

        private readonly Lazy<IMapperSchema> _mapperSchemaKeyOnly =
            new(() => new MapperSchema(KeyColumns.Cast<IMapperColumn>().ToArray()));

        /// <summary>
        /// Gets the mapper schema.
        /// </summary>
        /// <param name="keyOnly">Whether to get a key-only schema.</param>
        /// <returns>Mapper schema.</returns>
        public IMapperSchema GetMapperSchema(bool keyOnly) =>
            keyOnly ? _mapperSchemaKeyOnly.Value : _mapperSchema.Value;

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
        public static Schema CreateInstance(int version, int tableId, Column[] columns)
        {
            var keyColumnCount = columns.Count(static x => x.IsKey);
            var keyColumns = new Column[keyColumnCount];
            var valColumns = new Column[columns.Length - keyColumnCount];
            int colocationColumnCount = 0;
            int valIdx = 0;

            foreach (var column in columns)
            {
                if (column.IsKey)
                {
                    Debug.Assert(keyColumns[column.KeyIndex] == null, "Duplicate key index: " + column);
                    keyColumns[column.KeyIndex] = column;
                }
                else
                {
                    valColumns[valIdx++] = column;
                }

                if (column.IsColocation)
                {
                    colocationColumnCount++;
                }
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            Debug.Assert(keyColumns.All(x => x != null), "Some key columns are missing");
            Debug.Assert(columns.Length == 0 || colocationColumnCount > 0, "No hashed columns");

            var columnMap = new Dictionary<string, Column>(columns.Length);
            var colocationColumns = colocationColumnCount > 0 ? new Column[colocationColumnCount] : keyColumns;

            foreach (var column in columns)
            {
                columnMap[IgniteTupleCommon.ParseColumnName(column.Name)] = column;

                if (column.ColocationIndex >= 0)
                {
                    Debug.Assert(
                        column.ColocationIndex < colocationColumnCount,
                        $"Invalid colocation index: {column}, schema={columns[column.ColocationIndex]}");

                    Debug.Assert(
                        colocationColumns[column.ColocationIndex] == null!,
                        $"Duplicate colocation index: {column}, {colocationColumns[column.ColocationIndex]}");

                    colocationColumns[column.ColocationIndex] = column;
                }
            }

            return new Schema(
                version,
                tableId,
                colocationColumnCount,
                columns,
                keyColumns,
                valColumns,
                colocationColumns,
                columnMap,
                new HashedColumnIndexProvider(columns, colocationColumnCount),
                new HashedColumnIndexProvider(keyColumns, colocationColumnCount));
        }

        /// <summary>
        /// Gets columns.
        /// </summary>
        /// <param name="keyOnly">Key only flag.</param>
        /// <returns>Columns according to the key flag.</returns>
        public Column[] GetColumnsFor(bool keyOnly) =>
            keyOnly ? KeyColumns : Columns;

        /// <summary>
        /// Gets the hashed column index provider for the specified key-only flag.
        /// </summary>
        /// <param name="keyOnly">Key only flag.</param>
        /// <returns>Hashed column index provider.</returns>
        public IHashedColumnIndexProvider GetHashedColumnIndexProviderFor(bool keyOnly) =>
            keyOnly ? KeyOnlyHashedColumnIndexProvider : HashedColumnIndexProvider;
    }
}
