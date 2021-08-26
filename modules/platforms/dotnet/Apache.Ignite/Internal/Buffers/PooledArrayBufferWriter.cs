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
    /// Pooled buffer writer.
    ///
    /// TODO:
    /// * Internal.
    /// * Start with passed stackalloc buffer for small payloads.
    /// * Use unmanaged memory and a built-in pool?
    /// * Use a string of segments?
    /// USE <see cref="SequencePool"/>! (MessagePack lib).
    /// </summary>
    public sealed class PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private byte[] _buffer;

        private int _index;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> class.
        /// </summary>
        public PooledArrayBufferWriter()
        {
            _buffer = Array.Empty<byte>();
            _index = 0;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> class.
        /// </summary>
        /// <param name="initialCapacity">Initial capacity.</param>
        /// <exception cref="ArgumentException">When capacity is less than 1.</exception>
        public PooledArrayBufferWriter(int initialCapacity)
        {
            _buffer = initialCapacity > 0
                ? ArrayPool<byte>.Shared.Rent(initialCapacity)
                : throw new ArgumentException(null, nameof(initialCapacity));

            _index = 0;
        }

        /// <summary>
        /// Gets written data as memory..
        /// </summary>
        public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _index);

        /// <summary>
        /// Gets written data as span.
        /// </summary>
        public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);

        /// <summary>
        /// Gets the written count.
        /// </summary>
        public int WrittenCount => _index;

        /// <summary>
        /// Gets the capacity.
        /// </summary>
        public int Capacity => _buffer.Length;

        /// <summary>
        /// Gets the free capacity.
        /// </summary>
        public int FreeCapacity => _buffer.Length - _index;

        /// <summary>
        /// Gets the underlying array.
        /// </summary>
        /// <returns>Underlying array.</returns>
        public byte[] GetArray() => _buffer;

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

        /// <inheritdoc />
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }

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

            if (length == 0)
            {
                increase = Math.Max(increase, 256); // TODO: Use stackalloc for default small buffer.
            }

            int newSize = length + increase;
            if ((uint)newSize > int.MaxValue)
            {
                newSize = length + sizeHint;
                if ((uint)newSize > int.MaxValue)
                {
                    throw new OutOfMemoryException($"Buffer can't be larger than {int.MaxValue} (int.MaxValue) bytes.");
                }
            }

            var newBuf = ArrayPool<byte>.Shared.Rent(newSize);

            Array.Copy(_buffer, newBuf, _index);

            ArrayPool<byte>.Shared.Return(_buffer);

            _buffer = newBuf;
        }
    }
}
