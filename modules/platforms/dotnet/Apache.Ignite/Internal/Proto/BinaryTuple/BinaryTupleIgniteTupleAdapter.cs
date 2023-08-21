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
using System.Diagnostics;
using Ignite.Table;
using Table;

/// <summary>
/// Adapts <see cref="BinaryTuple"/> to <see cref="IIgniteTuple"/>, so that we can avoid extra copying and allocations when
/// reading data from the server.
/// </summary>
internal sealed class BinaryTupleIgniteTupleAdapter : IIgniteTuple
{
    // TODO: Copy on write.
    private readonly Memory<byte> _data;

    private readonly Schema _schema;

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
        FieldCount = fieldCount;
    }

    /// <inheritdoc/>
    public int FieldCount { get; }

    /// <inheritdoc/>
    public object? this[int ordinal]
    {
        get => throw new System.NotImplementedException();
        set => throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public object? this[string name]
    {
        get => throw new System.NotImplementedException();
        set => throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public string GetName(int ordinal)
    {
        // TODO: Range checks.
        return _schema.Columns[ordinal].Name;
    }

    /// <inheritdoc/>
    public int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }
}
