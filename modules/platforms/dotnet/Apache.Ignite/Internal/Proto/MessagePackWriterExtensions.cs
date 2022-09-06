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
        /// Writes Ignite UUID.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public static void WriteNoValue(this ref MessagePackWriter writer)
        {
            writer.WriteExtensionFormatHeader(
                new ExtensionHeader((sbyte)ClientMessagePackType.NoValue, 1));

            writer.Advance(1);
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
        /// Writes an object.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="obj">Object.</param>
        public static void WriteObject(this ref MessagePackWriter writer, object? obj)
        {
            // TODO: Support all types (IGNITE-15431).
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

        /// <summary>
        /// Writes an object with type code.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="obj">Object.</param>
        public static void WriteObjectWithType(this ref MessagePackWriter writer, object? obj)
        {
            // TODO: Support all types (IGNITE-15431).
            switch (obj)
            {
                case null:
                    writer.WriteNil();
                    return;

                case string str:
                    writer.Write((int)ClientDataType.String);
                    writer.Write(str);
                    return;

                case Guid g:
                    writer.Write((int)ClientDataType.Uuid);
                    writer.Write(g);
                    return;

                case byte b:
                    writer.Write((int)ClientDataType.Int8);
                    writer.Write(b);
                    return;

                case sbyte sb:
                    writer.Write((int)ClientDataType.Int8);
                    writer.Write(sb);
                    return;

                case short s:
                    writer.Write((int)ClientDataType.Int16);
                    writer.Write(s);
                    return;

                case ushort us:
                    writer.Write((int)ClientDataType.Int16);
                    writer.Write(us);
                    return;

                case int i:
                    writer.Write((int)ClientDataType.Int32);
                    writer.Write(i);
                    return;

                case uint ui:
                    writer.Write((int)ClientDataType.Int32);
                    writer.Write(ui);
                    return;

                case long l:
                    writer.Write((int)ClientDataType.Int64);
                    writer.Write(l);
                    return;

                case ulong ul:
                    writer.Write((int)ClientDataType.Int64);
                    writer.Write(ul);
                    return;

                case float f:
                    writer.Write((int)ClientDataType.Float);
                    writer.Write(f);
                    return;

                case double d:
                    writer.Write((int)ClientDataType.Double);
                    writer.Write(d);
                    return;
            }

            throw new IgniteClientException("Unsupported type: " + obj.GetType());
        }

        /// <summary>
        /// Writes an array of objects with type codes.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="arr">Array.</param>
        public static void WriteObjectArrayWithTypes(this ref MessagePackWriter writer, object[]? arr)
        {
            if (arr == null)
            {
                writer.WriteNil();

                return;
            }

            writer.WriteArrayHeader(arr.Length);

            foreach (var obj in arr)
            {
                writer.WriteObjectWithType(obj);
            }
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
