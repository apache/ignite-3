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
    using System.Net;
    using MessagePack;

    /// <summary>
    /// Pooled buffer writer.
    /// Based on <see cref="ArrayBufferWriter{T}"/>, but uses <see cref="ArrayPool{T}.Shared"/> to allocate arrays.
    /// Not a struct because <see cref="GetMessageWriter"/> will cause boxing.
    /// </summary>
    internal sealed class PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
    {
        /** Underlying pooled array. */
        private byte[] _buffer;

        /** Index within the array. */
        private int _index;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> class.
        /// </summary>
        /// <param name="requestId">Optional associated request id.</param>
        public PooledArrayBufferWriter(long? requestId = null)
        {
            // NOTE: Shared pool has 1M elements limit before .NET 6.
            // https://devblogs.microsoft.com/dotnet/performance-improvements-in-net-6/#buffering
            _buffer = ArrayPool<byte>.Shared.Rent(PooledBuffer.DefaultCapacity);
            _index = 4; // Reserve for message length.
            RequestId = requestId;
        }

        /// <summary>
        /// Gets the associated request id.
        /// </summary>
        public long? RequestId { get; }

        /// <summary>
        /// Gets the free capacity.
        /// </summary>
        private int FreeCapacity => _buffer.Length - _index;

        /// <summary>
        /// Gets the underlying array with first 4 bytes set to the buffer size.
        /// </summary>
        /// <returns>Underlying array.</returns>
        public unsafe ReadOnlyMemory<byte> GetWrittenMemory()
        {
            // Write big-endian message size to the start of the buffer.
            fixed (byte* bufPtr = &_buffer[0])
            {
                var msgSize = IPAddress.HostToNetworkOrder(_index - 4);
                *(int*)bufPtr = msgSize;
            }

            return new ReadOnlyMemory<byte>(_buffer, 0, _index);
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
        /// Gets the <see cref="MessagePackWriter"/> for this buffer.
        /// </summary>
        /// <returns><see cref="MessagePackWriter"/> for this buffer.</returns>
        public MessagePackWriter GetMessageWriter() => new(this);

        /// <inheritdoc />
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buffer);
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

            var newBuf = ArrayPool<byte>.Shared.Rent(newSize);

            Array.Copy(_buffer, newBuf, _index);

            ArrayPool<byte>.Shared.Return(_buffer);

            _buffer = newBuf;
        }
    }
}
