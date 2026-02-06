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
using System.Collections.Generic;
using Ignite.Table.Mapper;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Mapper-based KV pair serializer handler.
/// Converts between internal <see cref="KvPair{TK,TV}"/> and public <see cref="KeyValuePair{TKey,TValue}"/>.
/// See <see cref="MapperSerializerHandler{T}"/> for details.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
internal sealed class MapperPairSerializerHandler<TK, TV> : IRecordSerializerHandler<KvPair<TK, TV>>
{
    private readonly MapperSerializerHandler<KeyValuePair<TK, TV>> _pairHandler;

    /// <summary>
    /// Initializes a new instance of the <see cref="MapperPairSerializerHandler{TK, TV}"/> class.
    /// </summary>
    /// <param name="mapper">Mapper.</param>
    public MapperPairSerializerHandler(IMapper<KeyValuePair<TK, TV>> mapper) =>
        _pairHandler = new MapperSerializerHandler<KeyValuePair<TK, TV>>(mapper);

    /// <inheritdoc/>
    public KvPair<TK, TV> Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
    {
        KeyValuePair<TK, TV> pair = _pairHandler.Read(ref reader, schema, keyOnly);
        return new KvPair<TK, TV>(pair.Key, pair.Value);
    }

    /// <inheritdoc/>
    public void Write(ref BinaryTupleBuilder tupleBuilder, KvPair<TK, TV> record, Schema schema, bool keyOnly, scoped Span<byte> noValueSet)
    {
        KeyValuePair<TK, TV> pair = new KeyValuePair<TK, TV>(record.Key, record.Val);
        _pairHandler.Write(ref tupleBuilder, pair, schema, keyOnly, noValueSet);
    }
}
