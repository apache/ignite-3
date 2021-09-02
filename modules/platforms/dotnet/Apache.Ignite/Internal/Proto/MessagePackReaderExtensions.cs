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
    using System.Buffers;
    using System.Buffers.Binary;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using MessagePack;

    /// <summary>
    /// Extension methods for <see cref="MessagePackReader"/>.
    /// </summary>
    internal static class MessagePackReaderExtensions
    {
        /// <summary>
        /// Reads an object with specified type.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="type">Type.</param>
        /// <returns>Resulting object.</returns>
        public static object? ReadObject(this ref MessagePackReader reader, ClientDataType type)
        {
            switch (type)
            {
                case ClientDataType.Int8:
                    return reader.ReadByte();

                case ClientDataType.Int16:
                    return reader.ReadInt16();

                case ClientDataType.Int32:
                    return reader.ReadInt32();

                case ClientDataType.Int64:
                    return reader.ReadInt64();

                case ClientDataType.Float:
                    return reader.ReadSingle();

                case ClientDataType.Double:
                    return reader.ReadDouble();

                case ClientDataType.Uuid:
                    return reader.ReadGuid();

                case ClientDataType.String:
                    return reader.ReadString();

                default:
                    throw new IgniteClientException("Unsupported type: " + type);
            }
        }

        /// <summary>
        /// Skips multiple elements.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="count">Element count to skip.</param>
        public static void Skip(this ref MessagePackReader reader, int count)
        {
            for (var i = 0; i < count; i++)
            {
                reader.Skip();
            }
        }

        /// <summary>
        /// Reads a Guid value.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Guid.</returns>
        public static Guid ReadGuid(this ref MessagePackReader reader)
        {
            const int guidSize = 16;

            ValidateExtensionType(ref reader, ClientMessagePackType.Uuid, guidSize);

            ReadOnlySequence<byte> seq = reader.ReadRaw(guidSize);
            ReadOnlySpan<byte> jBytes = seq.FirstSpan;

            Debug.Assert(jBytes.Length == guidSize, "jBytes.Length == 16");

            // Hoist range checks.
            byte d = jBytes[15];
            byte e = jBytes[14];
            byte f = jBytes[13];
            byte g = jBytes[12];
            byte h = jBytes[11];
            byte i = jBytes[10];
            byte j = jBytes[9];
            byte k = jBytes[8];

            int a = BinaryPrimitives.ReadInt32BigEndian(jBytes[4..]);
            short b = BinaryPrimitives.ReadInt16BigEndian(jBytes[2..]);
            short c = BinaryPrimitives.ReadInt16BigEndian(jBytes);

            return new Guid(a, b, c, d, e, f, g, h, i, j, k);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ValidateExtensionType(
            ref MessagePackReader reader,
            ClientMessagePackType expectedType,
            int expectedLength)
        {
            ExtensionHeader hdr = reader.ReadExtensionFormatHeader();

            if (hdr.TypeCode != (int)expectedType)
            {
                throw new IgniteClientException(
                    $"Expected {expectedType} extension ({(int)expectedType}), but got {hdr.TypeCode}.");
            }

            if (hdr.Length != expectedLength)
            {
                throw new IgniteClientException(
                    $"Expected {expectedLength} bytes for {expectedType} extension, but got {hdr.Length}.");
            }
        }
    }
}
