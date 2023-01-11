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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using MessagePack;
    using Proto.MsgPack;

    /// <summary>
    /// Pooled byte buffer. Wraps a byte array rented from <see cref="ByteArrayPool"/>,
    /// returns it to the pool on <see cref="Dispose"/>.
    /// </summary>
    [SuppressMessage(
        "Microsoft.Performance",
        "CA1815:OverrideEqualsAndOperatorEqualsOnValueTypes",
        Justification = "Not used in comparisons")]
    internal readonly struct PooledBuffer : IDisposable
    {
        /// <summary>
        /// Default capacity for all buffers.
        /// </summary>
        public const int DefaultCapacity = 65_535;

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
        /// <param name="offset">Offset.</param>
        /// <returns><see cref="MessagePackReader"/> for this buffer.</returns>
        public MessagePackReader GetReader(int offset = 0) => new(AsMemory(offset));

        /// <summary>
        /// Gets a <see cref="MsgPackReader"/> for this buffer.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <returns><see cref="MsgPackReader"/> for this buffer.</returns>
        public MsgPackReader GetReader2(int offset = 0) => new(_bytes.AsSpan(_position + offset, _length - offset));

        /// <summary>
        /// Gets this buffer contents as memory.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <returns>Memory of byte.</returns>
        public ReadOnlyMemory<byte> AsMemory(int offset = 0) => new(_bytes, _position + offset, _length - offset);

        /// <summary>
        /// Gets a slice of the current buffer.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <returns>Sliced buffer.</returns>
        public PooledBuffer Slice(int offset)
        {
            Debug.Assert(offset > 0, "offset > 0");
            Debug.Assert(offset <= _length, "offset <= _length");

            return new(_bytes, _position + offset, _length - offset);
        }

        /// <summary>
        /// Releases the pooled buffer.
        /// </summary>
        public void Dispose()
        {
            ByteArrayPool.Return(_bytes);
        }
    }
}
