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

namespace Apache.Ignite.Marshalling;

using System;
using System.Buffers;
using System.Text.Json;
using Internal.Common;

/// <summary>
/// JSON marshaller. Uses <see cref="System.Text.Json.JsonSerializer"/>.
/// </summary>
/// <typeparam name="T">Object type.</typeparam>
public sealed class JsonMarshaller<T> : IMarshaller<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JsonMarshaller{T}"/> class.
    /// </summary>
    /// <param name="options">Options.</param>
    public JsonMarshaller(JsonSerializerOptions? options = null)
    {
        Options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            WriteIndented = false
        };
    }

    /// <summary>
    /// Gets the options.
    /// </summary>
    public JsonSerializerOptions Options { get; }

    /// <inheritdoc />
    public void Marshal(T obj, IBufferWriter<byte> writer)
    {
        IgniteArgumentCheck.NotNull(obj);
        IgniteArgumentCheck.NotNull(writer);

        using var utf8JsonWriter = new Utf8JsonWriter(writer);
        JsonSerializer.Serialize(utf8JsonWriter, obj, Options);
    }

    /// <inheritdoc />
    public T Unmarshal(ReadOnlySpan<byte> bytes) =>
        JsonSerializer.Deserialize<T>(bytes, Options)!;
}
