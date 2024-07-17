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
        /// <param name="val">Guid.</param>
        /// <param name="span">Target span.</param>
        public static void Write(Guid val, Span<byte> span)
        {
            var written = val.TryWriteBytes(span);
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
            var d = span[15];
            var e = span[14];
            var f = span[13];
            var g = span[12];
            var h = span[11];
            var i = span[10];
            var j = span[9];
            var k = span[8];

            var a = BinaryPrimitives.ReadInt32LittleEndian(span[4..8]);
            var b = BinaryPrimitives.ReadInt16LittleEndian(span[2..4]);
            var c = BinaryPrimitives.ReadInt16LittleEndian(span[..2]);

            return new Guid(a, b, c, d, e, f, g, h, i, j, k);
        }
    }
}
