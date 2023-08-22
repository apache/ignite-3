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

namespace Apache.Ignite.Internal.Proto.BinaryTuple;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Ignite.Table;
using Table;
using Table.Serialization;

/// <summary>
/// Adapts <see cref="BinaryTuple"/> to <see cref="IIgniteTuple"/>, so that we can avoid extra copying and allocations when
/// reading data from the server.
/// </summary>
internal sealed class BinaryTupleIgniteTupleAdapter : IIgniteTuple, IEquatable<BinaryTupleIgniteTupleAdapter>, IEquatable<IIgniteTuple>
{
    // TODO: Comprehensive tests to ensure consistency with IgniteTuple
    private readonly int _schemaFieldCount; // Key-only tuples have less fields than schema.

    private Memory<byte> _data;

    private Schema? _schema;

    private Dictionary<string, int>? _indexes;

    private IgniteTuple? _tuple;

    /// <summary>
    /// Initializes a new instance of the <see cref="BinaryTupleIgniteTupleAdapter"/> class.
    /// </summary>
    /// <param name="data">Binary tuple data.</param>
    /// <param name="schema">Schema.</param>
    /// <param name="fieldCount">Field count.</param>
    public BinaryTupleIgniteTupleAdapter(Memory<byte> data, Schema schema, int fieldCount)
    {
        Debug.Assert(fieldCount <= schema.Columns.Count, "fieldCount <= schema.Columns.Count");

        _data = data;
        _schema = schema;
        _schemaFieldCount = fieldCount;
    }

    /// <inheritdoc/>
    public int FieldCount => _tuple?.FieldCount ?? _schemaFieldCount;

    /// <inheritdoc/>
    public object? this[int ordinal]
    {
        get => _tuple != null
            ? _tuple[ordinal]
            : TupleSerializerHandler.ReadObject(_data.Span, _schema!, _schemaFieldCount, ordinal);

        set => InitTuple()[ordinal] = value;
    }

    /// <inheritdoc/>
    public object? this[string name]
    {
        get => this[GetOrdinal(name)];
        set => InitTuple()[name] = value;
    }

    /// <inheritdoc/>
    public string GetName(int ordinal) => _schema != null
        ? _schema.Columns[ordinal].Name
        : _tuple!.GetName(ordinal);

    /// <inheritdoc/>
    public int GetOrdinal(string name)
    {
        if (_tuple != null)
        {
            return _tuple.GetOrdinal(name);
        }

        if (_indexes == null)
        {
            _indexes = new Dictionary<string, int>(_schema!.Columns.Count);

            for (var i = 0; i < _schema.Columns.Count; i++)
            {
                _indexes[IgniteTupleCommon.ParseColumnName(_schema.Columns[i].Name)] = i;
            }
        }

        return _indexes.TryGetValue(IgniteTupleCommon.ParseColumnName(name), out var index) ? index : -1;
    }

    /// <inheritdoc/>
    public override string ToString() => IgniteTupleCommon.ToString(this);

    /// <inheritdoc />
    public bool Equals(BinaryTupleIgniteTupleAdapter? other) => IIgniteTuple.Equals(this, other);

    /// <inheritdoc />
    public bool Equals(IIgniteTuple? other) => IIgniteTuple.Equals(this, other);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is IIgniteTuple other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => IIgniteTuple.GetHashCode(this);

    private IIgniteTuple InitTuple()
    {
        if (_tuple != null)
        {
            return _tuple;
        }

        Debug.Assert(_schema != null, "_schema != null");

        // Copy data to a mutable IgniteTuple.
        _tuple = TupleSerializerHandler.ReadTuple(_data.Span, _schema, _schemaFieldCount);

        // Release schema and data.
        _schema = default;
        _indexes = default;
        _data = default;

        return _tuple;
    }
}
