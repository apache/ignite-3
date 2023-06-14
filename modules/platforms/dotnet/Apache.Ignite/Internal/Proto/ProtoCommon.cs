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
    using System.Text;
    using Buffers;

    /// <summary>
    /// Common protocol data.
    /// </summary>
    internal static class ProtoCommon
    {
        /// <summary>
        /// Message prefix size.
        /// </summary>
        public const int MessagePrefixSize = 4 + 5 + 9; // Size (4 bytes) + OpCode (5 bytes) + RequestId (9 bytes)/

        /// <summary>
        /// Magic bytes.
        /// </summary>
        public static readonly byte[] MagicBytes = { (byte)'I', (byte)'G', (byte)'N', (byte)'I' };

        /// <summary>
        /// UTF8 encoding without preamble (as opposed to <see cref="Encoding.UTF8"/>).
        /// </summary>
        public static readonly Encoding StringEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

        /// <summary>
        /// Gets a new message writer.
        /// </summary>
        /// <returns>Message writer.</returns>
        public static PooledArrayBuffer GetMessageWriter() => new(prefixSize: MessagePrefixSize);
    }
}
