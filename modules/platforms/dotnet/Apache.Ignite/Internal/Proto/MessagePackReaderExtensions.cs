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
    using System.Runtime.CompilerServices;
    using MessagePack;

    /// <summary>
    /// Extension methods for <see cref="MessagePackReader"/>.
    /// </summary>
    internal static class MessagePackReaderExtensions
    {
        /// <summary>
        /// Skips multiple elements.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="count">Element count to skip.</param>
        public static void Skip(this MessagePackReader reader, int count)
        {
            for (var i = 0; i < count; i++)
            {
                reader.Skip();
            }
        }

        public static Guid ReadGuid(this ref MessagePackReader reader)
        {
            ValidateExtensionType(ref reader, ClientMessagePackType.Uuid, 16);

            return new Guid()
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ExtensionHeader ValidateExtensionType(
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

            return hdr;
        }
    }
}
