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
    using Proto.MsgPack;

    /// <summary>
    /// Pooled byte buffer. Wraps a byte array rented from <see cref="ByteArrayPool"/>,
    /// returns it to the pool on <see cref="Dispose"/>.
    /// </summary>
    internal sealed class PooledBuffer : IDisposable
    {
        /// <summary>
        /// Default capacity for all buffers.
        /// </summary>
        public const int DefaultCapacity = 65_535;

        private readonly byte[] _bytes;

        private readonly int _length;

        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledBuffer"/> class.
        /// </summary>
        /// <param name="bytes">Bytes.</param>
        /// <param name="position">Data position within specified byte array.</param>
        /// <param name="length">Data length within specified byte array.</param>
        public PooledBuffer(byte[] bytes, int position, int length)
        {
            _bytes = bytes;
            Position = position;
            _length = length;
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="PooledBuffer"/> class.
        /// </summary>
        ~PooledBuffer()
        {
            Dispose();
        }

        /// <summary>
        /// Gets or sets the position.
        /// </summary>
        public int Position { get; set; }

        /// <summary>
        /// Gets or sets the optional metadata.
        /// </summary>
        public object? Metadata { get; set; }

        /// <summary>
        /// Gets a <see cref="MsgPackReader"/> for this buffer.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <returns><see cref="MsgPackReader"/> for this buffer.</returns>
        public MsgPackReader GetReader(int offset = 0)
        {
            CheckDisposed();
            return new MsgPackReader(_bytes.AsSpan(Position + offset, _length - offset - Position));
        }

        /// <summary>
        /// Gets this buffer contents as memory.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <returns>Memory of byte.</returns>
        public ReadOnlyMemory<byte> AsMemory(int offset = 0)
        {
            CheckDisposed();
            return new ReadOnlyMemory<byte>(_bytes, Position + offset, _length - offset - Position);
        }

        /// <summary>
        /// Releases the pooled buffer.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            ByteArrayPool.Return(_bytes);
            _disposed = true;

            GC.SuppressFinalize(this);
        }

        private void CheckDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
