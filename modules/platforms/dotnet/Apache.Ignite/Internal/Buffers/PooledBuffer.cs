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

namespace Apache.Ignite.Internal.Buffers
{
    using System;
    using System.Buffers;
    using MessagePack;

    /// <summary>
    /// Pooled byte buffer.
    /// </summary>
    internal readonly struct PooledBuffer
    {
        /** Bytes. */
        private readonly byte[] _bytes;

        /** Position. */
        private readonly int _position;

        /** Length. */
        private readonly int _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledBuffer"/> struct.
        /// </summary>
        /// <param name="bytes">Bytes.</param>
        /// <param name="position">Data position within specified byte array.</param>
        /// <param name="length">Data length within specified byte array.</param>
        public PooledBuffer(byte[] bytes, int position, int length)
        {
            _bytes = bytes;
            _position = position;
            _length = length;
        }

        /// <summary>
        /// Gets a <see cref="MessagePackReader"/> for this buffer.
        /// </summary>
        /// <returns><see cref="MessagePackReader"/> for this buffer.</returns>
        public MessagePackReader GetReader() => new(new ReadOnlyMemory<byte>(_bytes, _position, _length));

        public PooledBuffer Slice(int offset)
        {
            // TODO: Range checks.
            return new(_bytes, _position + offset, _length - offset);
        }

        /// <summary>
        /// Releases the pooled buffer.
        /// </summary>
        public void Release()
        {
            ArrayPool<byte>.Shared.Return(_bytes);
        }
    }
}
