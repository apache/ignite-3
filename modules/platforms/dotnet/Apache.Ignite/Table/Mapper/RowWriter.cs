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
using Internal.Table.Serialization;

/// <summary>
/// Row writer for mappers. Writes columns in the order defined by the schema.
/// </summary>
public ref struct RowWriter
{
    private readonly Span<byte> _noValueSet;

    private BinaryTupleBuilder _builder;

    /// <summary>
    /// Initializes a new instance of the <see cref="RowWriter"/> struct.
    /// </summary>
    /// <param name="builder">Builder.</param>
    /// <param name="noValueSet">No-value set.</param>
    internal RowWriter(ref BinaryTupleBuilder builder, Span<byte> noValueSet)
    {
        _builder = builder;
        _noValueSet = noValueSet;
    }

    /// <summary>
    /// Gets the builder.
    /// </summary>
    internal readonly BinaryTupleBuilder Builder => _builder;

    /// <summary>
    /// Writes an integer value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteInt(int? value) => _builder.AppendIntNullable(value);

    /// <summary>
    /// Writes a long value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteLong(long? value) => _builder.AppendLongNullable(value);

    /// <summary>
    /// Writes a string value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteString(string? value) => _builder.AppendStringNullable(value);

    /// <summary>
    /// Writes a string value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteGuid(Guid? value) => _builder.AppendGuidNullable(value);

    /// <summary>
    /// Skips writing the next column (marks as not set, so that the default column value can be applied by the server).
    /// </summary>
    public void Skip() => _builder.AppendNoValue(_noValueSet);
}
