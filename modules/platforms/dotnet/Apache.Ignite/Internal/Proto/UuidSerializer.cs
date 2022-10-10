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

namespace Apache.Ignite.Internal.Proto
{
    using System;
    using System.Buffers.Binary;
    using System.Diagnostics;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Serializes and deserializes Guids in Java-specific UUID format.
    /// </summary>
    internal static class UuidSerializer
    {
        /// <summary>
        /// Writes Guid as Java UUID.
        /// </summary>
        /// <param name="guid">Guid.</param>
        /// <param name="span">Target span.</param>
        public static void Write(Guid guid, Span<byte> span)
        {
            var written = guid.TryWriteBytes(span);
            Debug.Assert(written, "written");

            // Reverse first part order: abc -> cba. Parts are little-endian on any system.
            var a = MemoryMarshal.Read<int>(span);
            var b = MemoryMarshal.Read<short>(span[4..]);
            var c = MemoryMarshal.Read<short>(span[6..]);

            MemoryMarshal.Write(span[4..8], ref a);
            MemoryMarshal.Write(span[2..4], ref b);
            MemoryMarshal.Write(span[..2], ref c);

            // Reverse second part order: defghijk -> kjihgfed.
            span[8..16].Reverse();
        }

        /// <summary>
        /// Reads Java UUID as Guid.
        /// </summary>
        /// <param name="span">Span.</param>
        /// <returns>Guid.</returns>
        public static Guid Read(ReadOnlySpan<byte> span)
        {
            // Hoist bounds checks.
            var k = span[15];
            var a = BinaryPrimitives.ReverseEndianness(MemoryMarshal.Read<int>(span));
            var b = BinaryPrimitives.ReverseEndianness(MemoryMarshal.Read<short>(span[4..]));
            var c = BinaryPrimitives.ReverseEndianness(MemoryMarshal.Read<short>(span[6..]));
            var d = span[8];
            var e = span[9];
            var f = span[10];
            var g = span[11];
            var h = span[12];
            var i = span[13];
            var j = span[14];

            return new Guid(a, b, c, d, e, f, g, h, i, j, k);
        }
    }
}
