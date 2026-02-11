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

namespace Apache.Ignite.Internal.Proto;

using System;
using System.Buffers.Binary;

/// <summary>
/// Extensions for <see cref="ProtocolBitmaskFeature"/>.
/// </summary>
internal static class ProtocolBitmaskFeatureExtensions
{
    /// <summary>
    /// Gets the feature flags as bytes.
    /// </summary>
    /// <param name="feature">Flags.</param>
    /// <returns>Bytes.</returns>
    public static byte[] ToBytes(this ProtocolBitmaskFeature feature)
    {
        // BitSet.valueOf on the server is little-endian.
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, (int) feature);

        return buffer.ToArray();
    }

    /// <summary>
    /// Gets the feature flags from bytes.
    /// </summary>
    /// <param name="bytes">Bytes.</param>
    /// <returns>Flags.</returns>
    public static ProtocolBitmaskFeature FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length > 4)
        {
            throw new InvalidOperationException("Invalid bitmask feature length: " + bytes.Length);
        }

        if (bytes.Length < 4)
        {
            // Pad with zeros if less than 4 bytes.
            Span<byte> buffer = stackalloc byte[4];
            buffer.Clear();
            bytes.CopyTo(buffer);

            return (ProtocolBitmaskFeature) BinaryPrimitives.ReadInt32LittleEndian(buffer);
        }

        // BitSet.valueOf on the server is little-endian.
        return (ProtocolBitmaskFeature) BinaryPrimitives.ReadInt32LittleEndian(bytes);
    }
}
