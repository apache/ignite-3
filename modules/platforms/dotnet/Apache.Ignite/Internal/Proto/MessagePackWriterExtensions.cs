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
    using BinaryTuple;
    using MessagePack;
    using Transactions;

    /// <summary>
    /// Extension methods for <see cref="MessagePackWriter"/>.
    /// </summary>
    internal static class MessagePackWriterExtensions
    {
        /// <summary>
        /// Writes a <see cref="Guid"/> as UUID (RFC #4122).
        /// <para />
        /// <see cref="Guid"/> uses a mixed-endian format which differs from UUID,
        /// see https://en.wikipedia.org/wiki/Universally_unique_identifier#Encoding.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="guid">Guid.</param>
        public static void Write(this ref MessagePackWriter writer, Guid guid)
        {
            writer.WriteExtensionFormatHeader(new ExtensionHeader((sbyte)ClientMessagePackType.Uuid, 16));

            var jBytes = writer.GetSpan(16);

            UuidSerializer.Write(guid, jBytes);

            writer.Advance(16);
        }

        /// <summary>
        /// Writes BitSet header and reserves space for bits, returns a span to write bits to.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="bitCount">Bit count.</param>
        /// <returns>Span to write bits to.</returns>
        public static Span<byte> WriteBitSet(this ref MessagePackWriter writer, int bitCount)
        {
            var byteCount = bitCount / 8 + 1;
            writer.WriteExtensionFormatHeader(new ExtensionHeader((sbyte)ClientMessagePackType.Bitmask, byteCount));

            var span = writer.GetSpan(byteCount)[..byteCount];
            span.Clear();
            writer.Advance(byteCount);

            return span;
        }

        /// <summary>
        /// Writes an array of objects with type codes.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="arr">Array.</param>
        public static void WriteObjectArrayAsBinaryTuple(this ref MessagePackWriter writer, object?[]? arr)
        {
            if (arr == null)
            {
                writer.WriteNil();

                return;
            }

            writer.Write(arr.Length);

            using var builder = new BinaryTupleBuilder(arr.Length * 3);

            foreach (var obj in arr)
            {
                builder.AppendObjectWithType(obj);
            }

            writer.Write(builder.Build().Span);
        }

        /// <summary>
        /// Writes an object with type code.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="obj">Object.</param>
        public static void WriteObjectAsBinaryTuple(this ref MessagePackWriter writer, object? obj)
        {
            if (obj == null)
            {
                writer.WriteNil();

                return;
            }

            using var builder = new BinaryTupleBuilder(3);
            builder.AppendObjectWithType(obj);

            writer.Write(builder.Build().Span);
        }

        /// <summary>
        /// Writes a transaction.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="tx">Transaction.</param>
        public static void WriteTx(this ref MessagePackWriter writer, Transaction? tx)
        {
            if (tx == null)
            {
                writer.WriteNil();
            }
            else
            {
                writer.Write(tx.Id);
            }
        }
    }
}
