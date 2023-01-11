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
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using BinaryTuple;
    using MessagePack;

    /// <summary>
    /// Extension methods for <see cref="MessagePackReader"/>.
    /// </summary>
    internal static class MessagePackReaderExtensions
    {
        /// <summary>
        /// Reads <see cref="ClientDataType"/> and value.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Value.</returns>
        public static object? ReadObjectFromBinaryTuple(this ref MessagePackReader reader)
        {
            if (reader.TryReadNil())
            {
                return null;
            }

            var tuple = new BinaryTupleReader(reader.ReadBytesAsSpan(), 3);

            return tuple.GetObject(0);
        }

        /// <summary>
        /// Reads nullable integer.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Nullable int.</returns>
        public static int? ReadInt32Nullable(this ref MessagePackReader reader)
        {
            // ReSharper disable RedundantCast (does not build on older SDKs)
            return reader.IsNil ? (int?)null : reader.ReadInt32();
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
        /// Reads an UUID (RFC #4122) as <see cref="Guid"/>.
        /// <para />
        /// <see cref="Guid"/> uses a mixed-endian format which differs from UUID,
        /// see https://en.wikipedia.org/wiki/Universally_unique_identifier#Encoding.
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

            return UuidSerializer.Read(jBytes);
        }

        /// <summary>
        /// Reads an int value if it is the next token..
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="res">result.</param>
        /// <returns><c>true</c> if could read and integer value; <c>false</c> otherwise.</returns>
        public static bool TryReadInt(this ref MessagePackReader reader, out int res)
        {
            if (reader.NextMessagePackType == MessagePackType.Integer)
            {
                res = reader.ReadInt32();
                return true;
            }

            res = 0;
            return false;
        }

        /// <summary>
        /// Reads binary value as <see cref="ReadOnlyMemory{T}"/>.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Binary value.</returns>
        public static ReadOnlySpan<byte> ReadBytesAsSpan(this ref MessagePackReader reader)
        {
            ReadOnlySequence<byte> tupleSeq = reader.ReadBytes()!.Value;

            Debug.Assert(tupleSeq.IsSingleSegment, "tupleSeq.IsSingleSegment");

            return tupleSeq.First.Span;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ValidateExtensionType(
            ref MessagePackReader reader,
            ClientMessagePackType expectedType,
            int? expectedLength = null)
        {
            ExtensionHeader hdr = reader.ReadExtensionFormatHeader();

            if (hdr.TypeCode != (int)expectedType)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Protocol,
                    $"Expected {expectedType} extension ({(int)expectedType}), but got {hdr.TypeCode}.");
            }

            if (expectedLength != null && hdr.Length != expectedLength)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Protocol,
                    $"Expected {expectedLength} bytes for {expectedType} extension, but got {hdr.Length}.");
            }
        }
    }
}
