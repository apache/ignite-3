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

namespace Apache.Ignite.Internal.Table;

using System;
using Ignite.Table.Mapper;

/// <summary>
/// Mappers for common key types.
/// <para />
/// An optimization to avoid generating mappers via reflection for basic types.
/// </summary>
internal static class BasicMappers
{
    /// <summary>
    /// Gets a mapper for a basic type if available, null otherwise.
    /// </summary>
    /// <typeparam name="T">Type.</typeparam>
    /// <returns>Mapper or null.</returns>
    internal static IMapper<T>? TryGet<T>()
    {
        if (typeof(T) == typeof(Guid))
        {
            return (IMapper<T>)(object)GuidMapper.Instance;
        }

        if (typeof(T) == typeof(string))
        {
            return (IMapper<T>)(object)StringMapper.Instance;
        }

        return null;
    }

    private sealed class GuidMapper : IMapper<Guid>
    {
        public static readonly GuidMapper Instance = new();

        public void Write(Guid obj, ref RowWriter rowWriter, IMapperSchema schema) => rowWriter.WriteGuid(obj);

        public Guid Read(ref RowReader rowReader, IMapperSchema schema) => rowReader.ReadGuid()!.Value;
    }

    private sealed class StringMapper : IMapper<string>
    {
        public static readonly StringMapper Instance = new();

        public void Write(string obj, ref RowWriter rowWriter, IMapperSchema schema) => rowWriter.WriteString(obj);

        public string Read(ref RowReader rowReader, IMapperSchema schema) => rowReader.ReadString()!;
    }
}
