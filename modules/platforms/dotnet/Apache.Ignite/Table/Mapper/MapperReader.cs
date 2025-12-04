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

/// <summary>
/// Mapper reader.
/// </summary>
public ref struct MapperReader
{
    private readonly BinaryTupleReader _reader;

    private readonly Column[] _schema;

    private int _position;

    /// <summary>
    /// Initializes a new instance of the <see cref="MapperReader"/> struct.
    /// </summary>
    /// <param name="reader">Reader.</param>
    /// <param name="schema">Schema.</param>
    internal MapperReader(ref BinaryTupleReader reader, Column[] schema)
    {
        _reader = reader;
        _schema = schema;
    }

    /// <summary>
    /// Returns the value of the next column.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <returns>Column value.</returns>
    public T? Read<T>()
    {
        var pos = _position++;

        if (pos >= _schema.Length)
        {
            throw new InvalidOperationException($"No more columns to read. Total columns: {_schema.Length}.");
        }

        var col = _schema[pos];

        // TODO: Use KeyIndex/SchemaIndex.
        return (T?)_reader.GetObject(pos, col.Type, col.Scale);
    }

    /// <summary>
    /// Skips the current column.
    /// </summary>
    public void Skip() => _position++;

    // TODO: Add read by name?
}
