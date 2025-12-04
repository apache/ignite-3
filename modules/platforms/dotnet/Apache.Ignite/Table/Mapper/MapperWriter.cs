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

namespace Apache.Ignite.Table.Mapper;

using System;
using Internal.Proto.BinaryTuple;
using Internal.Table;
using Internal.Table.Serialization;
using Sql;

/// <summary>
/// Mapper writer.
/// </summary>
public ref struct MapperWriter
{
    private readonly Schema _schema;

    private readonly Span<byte> _noValueSet;

    private BinaryTupleBuilder _builder;

    private int _position;

    /// <summary>
    /// Initializes a new instance of the <see cref="MapperWriter"/> struct.
    /// </summary>
    /// <param name="builder">Builder.</param>
    /// <param name="schema">Schema.</param>
    /// <param name="noValueSet">No-value set.</param>
    internal MapperWriter(ref BinaryTupleBuilder builder, Schema schema, Span<byte> noValueSet)
    {
        _builder = builder;
        _schema = schema; // TODO: Pass schema subset.
        _noValueSet = noValueSet;
    }

    /// <summary>
    /// Gets the builder.
    /// </summary>
    internal readonly BinaryTupleBuilder Builder => _builder;

    /// <summary>
    /// Writes the specified object to an Ignite table row.
    /// This method must be called for every column in the schema, in the order of the columns (see <see cref="IMapper{T}.Write"/>).
    /// </summary>
    /// <param name="obj">Object.</param>
    /// <typeparam name="T">Object type.</typeparam>
    public void Write<T>(T? obj)
    {
        // TODO: Allow writing in arbitrary order by column name. If the order matches schema order, we can avoid lookups.
        // Otherwise, put values into a map or a separate BinaryTuple.
        var pos = _position++;

        if (pos >= _schema.Columns.Length)
        {
            throw new InvalidOperationException($"No more columns to write. Total columns: {_schema.Columns.Length}.");
        }

        Column col = _schema.Columns[pos];
        ColumnType actualType = _builder.AppendObjectAndGetType(obj);

        if (col.Type != actualType)
        {
            throw new InvalidOperationException(
                $"Column type mismatch for {col.Name}. Expected: {col.Type}, actual: {actualType}, value: '{obj}'.");
        }
    }

    /// <summary>
    /// Skips writing the next column (marks as not set, so that the default column value can be applied by the server).
    /// </summary>
    public void Skip()
    {
        _builder.AppendNoValue(_noValueSet);
    }
}
