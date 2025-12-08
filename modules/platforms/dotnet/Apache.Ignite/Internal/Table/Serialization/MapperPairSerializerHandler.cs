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
using Ignite.Table.Mapper;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Mapper-based KV pair serializer handler. See <see cref="MapperSerializerHandler{T}"/> for details.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
internal sealed class MapperPairSerializerHandler<TK, TV> : IRecordSerializerHandler<KvPair<TK, TV>>
{
    private readonly IMapper<TK> _keyMapper;

    private readonly IMapper<TV> _valMapper;

    /// <summary>
    /// Initializes a new instance of the <see cref="MapperPairSerializerHandler{TK, TV}"/> class.
    /// </summary>
    /// <param name="keyMapper">Key mapper.</param>
    /// <param name="valMapper">Val mapper.</param>
    public MapperPairSerializerHandler(IMapper<TK> keyMapper, IMapper<TV> valMapper)
    {
        _keyMapper = keyMapper;
        _valMapper = valMapper;
    }

    /// <inheritdoc/>
    public KvPair<TK, TV> Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Write(ref BinaryTupleBuilder tupleBuilder, KvPair<TK, TV> record, Schema schema, bool keyOnly, scoped Span<byte> noValueSet)
    {
        throw new NotImplementedException();
    }
}
