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
using Common;
using Ignite.Table.Mapper;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Mapper-based record serializer handler.
/// </summary>
/// <typeparam name="T">Object type.</typeparam>
internal sealed class MapperSerializerHandler<T> : IRecordSerializerHandler<T>
{
    private readonly IMapper<T> _mapper;

    /// <summary>
    /// Initializes a new instance of the <see cref="MapperSerializerHandler{T}"/> class.
    /// </summary>
    /// <param name="mapper">Mapper.</param>
    internal MapperSerializerHandler(IMapper<T> mapper)
    {
        IgniteArgumentCheck.NotNull(mapper);

        _mapper = mapper;
    }

    /// <inheritdoc/>
    public T Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
    {
        // TODO: keyOnly support.
        var binaryTupleReader = new BinaryTupleReader(reader.ReadBinary(), schema.Columns.Length);
        var mapperReader = new MapperReader(ref binaryTupleReader, schema);

        return _mapper.Read(ref mapperReader, schema);
    }

    /// <inheritdoc/>
    public void Write(ref BinaryTupleBuilder tupleBuilder, T record, Schema schema, bool keyOnly, Span<byte> noValueSet)
    {
        // TODO: keyOnly support.
        var mapperWriter = new MapperWriter(ref tupleBuilder, schema, noValueSet);
        _mapper.Write(record, ref mapperWriter, schema);

        // NOTE: MapperWriter constructor makes a copy of the builder, but this is ok since the underlying buffer is shared.
        if (mapperWriter.Builder.ElementIndex < mapperWriter.Builder.NumElements)
        {
            throw new InvalidOperationException("Not all columns were written by the mapper. " +
                $"Expected: {tupleBuilder.NumElements}, written: {tupleBuilder.ElementIndex}.");
        }
    }
}
