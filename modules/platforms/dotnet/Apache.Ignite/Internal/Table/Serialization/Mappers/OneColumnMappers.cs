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

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Ignite.Table.Mapper;

/// <summary>
/// Primitive mapper helper.
/// </summary>
internal static class OneColumnMappers
{
    private static readonly OneColumnMapper<int> IntMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadInt()!.Value,
        (int obj, ref RowWriter writer, IMapperSchema _) => writer.WriteInt(obj));

    private static readonly OneColumnMapper<int?> IntNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadInt(),
        (int? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteInt(obj));

    private static readonly OneColumnMapper<string?> StringMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadString(),
        (string? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteString(obj));

    private static readonly FrozenDictionary<Type, object> Mappers = new Dictionary<Type, object>
    {
        { typeof(int), IntMapper },
        { typeof(int?), IntNullableMapper },
        { typeof(string), StringMapper }
    }.ToFrozenDictionary();

    /// <summary>
    /// Creates a primitive mapper for the specified type if supported; otherwise, returns null.
    /// </summary>
    /// <typeparam name="T">Type.</typeparam>
    /// <returns>Mapper or null.</returns>
    public static OneColumnMapper<T>? TryCreate<T>() => Mappers.GetValueOrDefault(typeof(T)) as OneColumnMapper<T>;
}
