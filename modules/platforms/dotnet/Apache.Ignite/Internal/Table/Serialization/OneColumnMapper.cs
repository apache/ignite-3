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
using System.Diagnostics.CodeAnalysis;
using Ignite.Table.Mapper;

/// <summary>
/// Primitive mapper helper.
/// </summary>
internal static class OneColumnMapper
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

    /// <summary>
    /// Writer delegate.
    /// </summary>
    /// <param name="obj">Object.</param>
    /// <param name="rowWriter">Writer.</param>
    /// <param name="schema">Schema.</param>
    /// <typeparam name="T">Type.</typeparam>
    public delegate void Writer<in T>(T obj, ref RowWriter rowWriter, IMapperSchema schema);

    /// <summary>
    /// Reader delegate.
    /// </summary>
    /// <param name="rowReader">Reader.</param>
    /// <param name="schema">Schema.</param>
    /// <typeparam name="T">Type.</typeparam>
    /// <returns>Result.</returns>
    public delegate T Reader<out T>(ref RowReader rowReader, IMapperSchema schema);

    /// <summary>
    /// Creates a primitive mapper for the specified type if supported; otherwise, returns null.
    /// </summary>
    /// <typeparam name="T">Type.</typeparam>
    /// <returns>Mapper or null.</returns>
    public static OneColumnMapper<T>? TryCreate<T>()
    {
        if (typeof(T) == typeof(int))
        {
            return (OneColumnMapper<T>)(object)IntMapper;
        }

        if (typeof(T) == typeof(int?))
        {
            return (OneColumnMapper<T>)(object)IntNullableMapper;
        }

        if (typeof(T) == typeof(string))
        {
            return (OneColumnMapper<T>)(object)StringMapper;
        }

        return null;
    }
}

/// <summary>
/// Primitive mapper.
/// </summary>
/// <typeparam name="T">Type.</typeparam>
[SuppressMessage("MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
internal sealed record OneColumnMapper<T>(OneColumnMapper.Reader<T> Reader, OneColumnMapper.Writer<T> Writer) : IMapper<T>
{
    /// <inheritdoc/>
    public void Write(T obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        ValidateSchema(schema);
        Writer(obj, ref rowWriter, schema);
    }

    /// <inheritdoc/>
    public T Read(ref RowReader rowReader, IMapperSchema schema)
    {
        ValidateSchema(schema);
        return Reader(ref rowReader, schema);
    }

    private static void ValidateSchema(IMapperSchema schema)
    {
        if (schema.Columns.Count > 1)
        {
            // TODO: Is this consistent with auto-generated mapper?
            throw new InvalidOperationException(
                $"Primitive mapper can only be used with single-column schemas, but schema has {schema.Columns.Count} columns.");
        }
    }
}
