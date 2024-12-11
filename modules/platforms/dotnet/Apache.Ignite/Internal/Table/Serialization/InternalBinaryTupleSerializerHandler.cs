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

namespace Apache.Ignite.Internal.Table.Serialization;

using System;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Internal serializer handler for direct access to binary tuple data.
/// </summary>
internal sealed class InternalBinaryTupleSerializerHandler : IRecordSerializerHandler<BinaryTupleAccessor>
{
    /// <inheritdoc/>
    public BinaryTupleAccessor Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
    {
        var binaryTupleData = reader.ReadBinary();
        var binaryTupleReader = new BinaryTupleReader(binaryTupleData, schema.GetColumnsFor(keyOnly).Length);

        // TODO: Get column index by name?
        var resultSpan = binaryTupleReader.GetBytesSpan(1);

        // TODO: How to copy this to the provided buffer?
    }

    /// <inheritdoc/>
    public void Write(ref BinaryTupleBuilder tupleBuilder, BinaryTupleAccessor record, Schema schema, bool keyOnly, Span<byte> noValueSet)
    {
        throw new NotImplementedException();
    }
}
