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
    using System.Diagnostics;
    using MessagePack;

    /// <summary>
    /// Pooled buffer writer: integrates <see cref="MessagePackWriter"/> with <see cref="ArrayPool{T}"/>,
    /// and adds the logic to prepend messages with size and other data (opcode, request id).
    /// <para />
    /// We reserve some bytes for the prefix because message size, op code and request ID are not known initially.
    /// <para />
    /// There are two ways to use <see cref="MessagePackWriter"/>: with a <see cref="SequencePool"/>,
    /// or with a <see cref="IBufferWriter{T}"/>. SequencePool approach uses buffer pooling too, but still allocates
    /// the final array with <c>MessagePackWriter.FlushAndGetArray</c>. We want to avoid all array allocations,
    /// so we implement our own <see cref="IBufferWriter{T}"/> here.
    /// <para />
    /// Based on <see cref="ArrayBufferWriter{T}"/>, but uses <see cref="ByteArrayPool"/> to allocate arrays.
    /// <para />
    /// Not a struct because <see cref="GetMessageWriter"/> will cause boxing.
    /// </summary>
    internal sealed class PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
    {
        /** Prefix size. */
        private readonly int _prefixSize;

        /** Underlying pooled array. */
        private byte[] _buffer;

        /** Index within the array. */
        private int _index;

        /** Disposed flag. */
        private volatile bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> class.
        /// </summary>
        /// <param name="initialCapacity">Initial capacity.</param>
        /// <param name="prefixSize">Size of the reserved space at the start of the buffer.</param>
        public PooledArrayBufferWriter(int initialCapacity = PooledBuffer.DefaultCapacity, int prefixSize = 0)
        {
            // NOTE: Shared pool has 1M elements limit before .NET 6.
            // https://devblogs.microsoft.com/dotnet/performance-improvements-in-net-6/#buffering
            _buffer = ByteArrayPool.Rent(initialCapacity);
            _prefixSize = prefixSize;
            _index = prefixSize;
        }

        /// <summary>
        /// Gets the current position.
        /// </summary>
        public int Position => _index - _prefixSize;

        /// <summary>
        /// Gets the free capacity.
        /// </summary>
        private int FreeCapacity => _buffer.Length - _index;

        /// <summary>
        /// Gets the written memory, including prefix, if any.
        /// </summary>
        /// <returns>Written array.</returns>
        public Memory<byte> GetWrittenMemory()
        {
            Debug.Assert(!_disposed, "!_disposed");

            return new(_buffer, start: 0, length: _index);
        }

        /// <inheritdoc />
        public void Advance(int count)
        {
            if (count < 0)
            {
                throw new ArgumentException(null, nameof(count));
            }

            if (_index > _buffer.Length - count)
            {
                throw new InvalidOperationException("Can't advance past buffer limit.");
            }

            _index += count;
        }

        /// <inheritdoc />
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            return _buffer.AsMemory(_index);
        }

        /// <inheritdoc />
        public Span<byte> GetSpan(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            return _buffer.AsSpan(_index);
        }

        /// <summary>
        /// Gets a span for writing at the specified position.
        /// </summary>
        /// <param name="position">Position.</param>
        /// <param name="size">Size.</param>
        /// <returns>Span for writing.</returns>
        public Span<byte> GetSpan(int position, int size)
        {
            var overflow = _prefixSize + position + size - _index;

            if (overflow > 0)
            {
                CheckAndResizeBuffer(overflow);
            }

            return _buffer.AsSpan(_prefixSize + position, size);
        }

        /// <summary>
        /// Gets the <see cref="MessagePackWriter"/> for this buffer.
        /// </summary>
        /// <returns><see cref="MessagePackWriter"/> for this buffer.</returns>
        public MessagePackWriter GetMessageWriter() => new(this);

        /// <inheritdoc />
        public void Dispose()
        {
            if (!_disposed)
            {
                ByteArrayPool.Return(_buffer);
                _disposed = true;
            }
        }

        /// <summary>
        /// Checks underlying buffer and resizes if necessary.
        /// </summary>
        /// <param name="sizeHint">Size hint.</param>
        private void CheckAndResizeBuffer(int sizeHint)
        {
            if (sizeHint < 0)
            {
                throw new ArgumentException(null, nameof(sizeHint));
            }

            if (sizeHint == 0)
            {
                sizeHint = 1;
            }

            if (sizeHint <= FreeCapacity)
            {
                return;
            }

            int length = _buffer.Length;
            int increase = Math.Max(sizeHint, length);

            int newSize = length + increase;

            if ((uint)newSize > int.MaxValue)
            {
                newSize = length + sizeHint;
                if ((uint)newSize > int.MaxValue)
                {
                    throw new OutOfMemoryException($"Buffer can't be larger than {int.MaxValue} (int.MaxValue) bytes.");
                }
            }

            // Arrays from ArrayPool are sized to powers of 2, so we don't need to implement the same logic here.
            // Even if requested size is 1 byte more than current, we'll get at lest 2x bigger array.
            var newBuf = ByteArrayPool.Rent(newSize);

            Array.Copy(_buffer, newBuf, _index);

            ByteArrayPool.Return(_buffer);

            _buffer = newBuf;
        }
    }
}
