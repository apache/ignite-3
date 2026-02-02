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

namespace Apache.Ignite.Internal.Table.Serialization.Mappers;

using System.Collections.Generic;
using Apache.Ignite.Table.Mapper;

/// <summary>
/// Composite mapper for key-value pairs.
/// </summary>
/// <typeparam name="TKey">Key type.</typeparam>
/// <typeparam name="TValue">Value type.</typeparam>
internal sealed record KeyValuePairCompositeMapper<TKey, TValue>(OneColumnMapper<TKey> KeyMapper, OneColumnMapper<TValue> ValMapper)
    : IMapper<KeyValuePair<TKey, TValue>>
{
    /// <inheritdoc />
    public void Write(KeyValuePair<TKey, TValue> obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        bool keyWritten = false;
        bool valueWritten = false;

        foreach (var column in schema.Columns)
        {
            if (!keyWritten && column is Column { IsKey: true })
            {
                KeyMapper.Writer(obj.Key, ref rowWriter, schema);
                keyWritten = true;
            }
            else if (!valueWritten)
            {
                ValMapper.Writer(obj.Value, ref rowWriter, schema);
                valueWritten = true;
            }

            if (keyWritten && valueWritten)
            {
                break;
            }
        }
    }

    /// <inheritdoc />
    public KeyValuePair<TKey, TValue> Read(ref RowReader rowReader, IMapperSchema schema)
    {
        TKey key = default!;
        TValue value = default!;

        bool keyRead = false;
        bool valueRead = false;

        foreach (var column in schema.Columns)
        {
            if (!keyRead && column is Column { IsKey: true })
            {
                key = KeyMapper.Reader(ref rowReader, schema);
                keyRead = true;
            }
            else if (!valueRead)
            {
                value = ValMapper.Reader(ref rowReader, schema);
                valueRead = true;
            }

            if (keyRead && valueRead)
            {
                break;
            }
        }

        return KeyValuePair.Create(key, value);
    }
}
