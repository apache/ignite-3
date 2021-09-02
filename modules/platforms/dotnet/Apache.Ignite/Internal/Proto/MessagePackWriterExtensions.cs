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
    using System.Diagnostics;
    using MessagePack;

    /// <summary>
    /// Extension methods for <see cref="MessagePackWriter"/>.
    /// </summary>
    internal static class MessagePackWriterExtensions
    {
        /// <summary>
        /// Writes a string.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="str">String.</param>
        public static void Write(this ref MessagePackWriter writer, string? str)
        {
            if (str == null)
            {
                writer.WriteNil();
                return;
            }

            var count = ProtoCommon.Encoding.GetByteCount(str);

            writer.WriteStringHeader(count);

            var targetSpan = writer.GetSpan(count);

            ProtoCommon.Encoding.GetBytes(str, targetSpan);

            writer.Advance(count);
        }

        /// <summary>
        /// Writes a Guid.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="guid">Guid.</param>
        public static void Write(this ref MessagePackWriter writer, Guid guid)
        {
            writer.WriteExtensionFormatHeader(new ExtensionHeader((sbyte)ClientMessagePackType.Uuid, 16));

            Span<byte> bytes = stackalloc byte[16];
            Span<byte> jBytes = writer.GetSpan(16);

            var written = guid.TryWriteBytes(bytes);
            Debug.Assert(written, "written");

            // Hoist range checks.
            jBytes[8] = bytes[15]; // k
            jBytes[15] = bytes[8]; // d

            if (BitConverter.IsLittleEndian)
            {
                jBytes[0] = bytes[7]; // c1
                jBytes[1] = bytes[6]; // c2

                jBytes[2] = bytes[5]; // b1
                jBytes[3] = bytes[4]; // b2

                jBytes[4] = bytes[3]; // a1
                jBytes[5] = bytes[2]; // a2
                jBytes[6] = bytes[1]; // a3
                jBytes[7] = bytes[0]; // a4
            }
            else
            {
                jBytes[0] = bytes[6]; // c1
                jBytes[1] = bytes[7]; // c2

                jBytes[2] = bytes[4]; // b1
                jBytes[3] = bytes[5]; // b2

                jBytes[4] = bytes[0]; // a1
                jBytes[5] = bytes[1]; // a2
                jBytes[6] = bytes[2]; // a3
                jBytes[7] = bytes[3]; // a4
            }

            jBytes[9] = bytes[14]; // j
            jBytes[10] = bytes[13]; // i
            jBytes[11] = bytes[12]; // h
            jBytes[12] = bytes[11]; // g
            jBytes[13] = bytes[10]; // f
            jBytes[14] = bytes[9]; // e

            writer.Advance(16);
        }

        /// <summary>
        /// Writes an object.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="obj">Object.</param>
        public static void WriteObject(this ref MessagePackWriter writer, object? obj)
        {
            // TODO: Support all types (IGNITE-15430).
            switch (obj)
            {
                case null:
                    writer.WriteNil();
                    return;

                case string str:
                    writer.Write(str);
                    return;

                case Guid g:
                    writer.Write(g);
                    return;

                case byte b:
                    writer.Write(b);
                    return;

                case sbyte sb:
                    writer.Write(sb);
                    return;

                case short s:
                    writer.Write(s);
                    return;

                case ushort us:
                    writer.Write(us);
                    return;

                case int i:
                    writer.Write(i);
                    return;

                case uint ui:
                    writer.Write(ui);
                    return;

                case long l:
                    writer.Write(l);
                    return;

                case ulong ul:
                    writer.Write(ul);
                    return;

                case char ch:
                    writer.Write(ch);
                    return;

                case float f:
                    writer.Write(f);
                    return;

                case double d:
                    writer.Write(d);
                    return;
            }

            throw new IgniteClientException("Unsupported type: " + obj.GetType());
        }
    }
}
