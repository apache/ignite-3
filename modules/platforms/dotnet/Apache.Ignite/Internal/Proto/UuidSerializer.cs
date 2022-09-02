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
    using System.Xml;

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
            var bytes = guid.ToByteArray();
            bytes.CopyTo(span);
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
            var a = BinaryPrimitives.ReadInt32BigEndian(span);
            var b = BinaryPrimitives.ReadInt16BigEndian(span[4..]);
            var c = BinaryPrimitives.ReadInt16BigEndian(span[6..]);
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
