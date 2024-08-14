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

/// <summary>
/// Ignite marshaller (serializer / deserializer).
/// <para />
/// Marshaller is used in APIs that require transferring custom objects over the wire, such as Compute arguments and results.
/// </summary>
/// <typeparam name="T">Object type.</typeparam>
public interface IMarshaller<T>
{
    /// <summary>
    /// Marshals (serializes) the specified object into the provided writer.
    /// </summary>
    /// <param name="obj">Object. Not null - Ignite handles nulls separately and does not invoke the marshaller.</param>
    /// <param name="writer">Writer.</param>
    void Marshal(T obj, IBufferWriter<byte> writer);

    /// <summary>
    /// Unmarshals (deserializes) an object from the provided data.
    /// </summary>
    /// <param name="bytes">Serialized data.</param>
    /// <returns>Deserialized object.</returns>
    T Unmarshal(ReadOnlySpan<byte> bytes);
}
