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
using System.Runtime.InteropServices;
using Common;
using Ignite.Table.Mapper;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Mapper-based record serializer handler.
/// <para />
/// IMapper design considerations:
/// - Source generators are the primary use case: performance is more important than ease of use.
/// - Object creation is handled by the mapper, no restrictions on constructors.
/// - If the mapper knows the column order, it can avoid name lookups.
/// - If name lookups are needed, a regular switch block is as fast as it gets (compiler uses hash codes and other tricks).
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
        Column[] columns = schema.GetColumnsFor(keyOnly);
        var binaryTupleReader = new BinaryTupleReader(reader.ReadBinary(), columns.Length);

        var mapperReader = new RowReader(ref binaryTupleReader, columns);
        IMapperSchema mapperSchema = schema.GetMapperSchema(keyOnly);

        return _mapper.Read(ref mapperReader, mapperSchema);
    }

    /// <inheritdoc/>
    public void Write(ref BinaryTupleBuilder tupleBuilder, T record, Schema schema, bool keyOnly, scoped Span<byte> noValueSet)
    {
        // Use MemoryMarshal to overcome the 'scoped' restriction.
        // We know that noValueSet will not outlive this method but cannot express this to the compiler.
        Span<byte> noValueSetRef = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(noValueSet), noValueSet.Length);
        var mapperWriter = new RowWriter(ref tupleBuilder, noValueSetRef, schema.GetColumnsFor(keyOnly));

        _mapper.Write(record, ref mapperWriter, schema.GetMapperSchema(keyOnly));

        // NOTE: MapperWriter constructor makes a copy of the builder, but this is ok since the underlying buffer is shared.
        if (mapperWriter.Builder.ElementIndex < mapperWriter.Builder.NumElements)
        {
            var message = $"Not all columns were written by the mapper. " +
                          $"Expected: {tupleBuilder.NumElements}, written: {mapperWriter.Builder.ElementIndex}.";

            throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
        }
    }
}
