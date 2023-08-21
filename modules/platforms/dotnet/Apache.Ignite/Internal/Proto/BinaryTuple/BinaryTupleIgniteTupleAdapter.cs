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
using Ignite.Table;
using Table;

/// <summary>
/// Adapts <see cref="BinaryTuple"/> to <see cref="IIgniteTuple"/>, so that we can avoid extra copying and allocations when
/// reading data from the server.
/// </summary>
internal sealed class BinaryTupleIgniteTupleAdapter : IIgniteTuple
{
    private readonly Memory<byte> _data;

    private readonly Schema _schema;

    public BinaryTupleIgniteTupleAdapter(Memory<byte> data, Schema schema, int fieldCount)
    {
        _data = data;
        _schema = schema;
        FieldCount = fieldCount;
    }

    public int FieldCount { get; }

    public object? this[int ordinal]
    {
        get => throw new System.NotImplementedException();
        set => throw new System.NotImplementedException();
    }

    public object? this[string name]
    {
        get => throw new System.NotImplementedException();
        set => throw new System.NotImplementedException();
    }

    public string GetName(int ordinal)
    {
        throw new System.NotImplementedException();
    }

    public int GetOrdinal(string name)
    {
        throw new System.NotImplementedException();
    }
}
