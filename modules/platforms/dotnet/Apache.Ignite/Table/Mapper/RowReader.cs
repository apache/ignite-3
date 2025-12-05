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

/// <summary>
/// Row reader for mappers. Reads columns in the order defined by the schema.
/// </summary>
public ref struct RowReader
{
    private readonly BinaryTupleReader _reader;

    private int _position;

    /// <summary>
    /// Initializes a new instance of the <see cref="RowReader"/> struct.
    /// </summary>
    /// <param name="reader">Reader.</param>
    internal RowReader(ref BinaryTupleReader reader)
    {
        _reader = reader;
    }

    /// <summary>
    /// Reads the next column as a GUID.
    /// </summary>
    /// <returns>Column value.</returns>
    public Guid? ReadGuid() => _reader.GetGuidNullable(_position++);

    /// <summary>
    /// Reads the next column as a string.
    /// </summary>
    /// <returns>Column value.</returns>
    public string? ReadString() => _reader.GetStringNullable(_position++);

    /// <summary>
    /// Reads the next column as an int.
    /// </summary>
    /// <returns>Column value.</returns>
    public int? ReadInt() => _reader.GetByteNullable(_position++);

    /// <summary>
    /// Skips the current column.
    /// </summary>
    public void Skip() => _position++;
}
