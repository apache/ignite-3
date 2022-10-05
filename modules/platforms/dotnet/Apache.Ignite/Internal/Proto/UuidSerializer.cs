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
        public static void WriteBigEndian(Guid guid, Span<byte> span)
        {
            var written = guid.TryWriteBytes(span); // Always little-endian.
            Debug.Assert(written, "written");

            span[..4].Reverse();
            span[4..6].Reverse();
            span[6..8].Reverse();
        }

        /// <summary>
        /// Writes Guid as Java UUID.
        /// </summary>
        /// <param name="guid">Guid.</param>
        /// <param name="span">Target span.</param>
        public static void WriteLittleEndian(Guid guid, Span<byte> span)
        {
            var written = guid.TryWriteBytes(span); // Always little-endian.
            Debug.Assert(written, "written");
        }

        /// <summary>
        /// Reads Java UUID as Guid.
        /// </summary>
        /// <param name="span">Span.</param>
        /// <returns>Guid.</returns>
        public static Guid ReadBigEndian(ReadOnlySpan<byte> span)
        {
            Span<byte> leSpan = stackalloc byte[16];
            span.CopyTo(leSpan);

            leSpan[..4].Reverse();
            leSpan[4..6].Reverse();
            leSpan[6..8].Reverse();

            return new Guid(leSpan);
        }

        /// <summary>
        /// Reads Java UUID as Guid.
        /// </summary>
        /// <param name="span">Span.</param>
        /// <returns>Guid.</returns>
        public static Guid ReadLittleEndian(ReadOnlySpan<byte> span) => new(span);
    }
}
