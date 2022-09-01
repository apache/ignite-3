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

// TODO: Restore inspections
// ReSharper disable all
#pragma warning disable

namespace Apache.Ignite.Internal.Proto.BinaryTuple
{
    using System;
    using System.Buffers.Binary;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Text;
    using Buffers;

    // TODO: Support all types (IGNITE-15431).
    /// <summary>
    /// Binary tuple builder.
    /// </summary>
    internal sealed class BinaryTupleBuilder
    {
        private const int DefaultBufferSize = 4000; // TODO: Pooling

        /** Number of elements in the tuple. */
        private readonly int _numElements;

        /** Size of an offset table entry. */
        private readonly int _entrySize;

        /** Position of the varlen offset table. */
        private readonly int _entryBase;

        /** Starting position of variable-length values. */
        private readonly int _valueBase;

        /** Buffer for tuple content. */
        private readonly PooledArrayBufferWriter _buffer = new();

        /** Flag indicating if any NULL values were really put here. */
        private bool _hasNullValues;

        /** Current element. */
        private int _elementIndex;

        /** Current position in buffer. */
        private int _position;

        /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
        private BinaryTupleBuilder(int numElements, bool allowNulls, int totalValueSize)
        {
            this._numElements = numElements;

            int @base = BinaryTupleCommon.HeaderSize;
            if (allowNulls)
            {
                @base += BinaryTupleCommon.NullMapSize(numElements);
            }

            _entryBase = @base;

            if (totalValueSize < 0)
            {
                _entrySize = 4;
            }
            else
            {
                _entrySize = BinaryTupleCommon.FlagsToEntrySize(BinaryTupleCommon.ValueSizeToFlags(totalValueSize));
            }

            _valueBase = @base + _entrySize * numElements;

            _position = _valueBase;
        }

        /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @return Tuple builder.
     */
        public static BinaryTupleBuilder create(int numElements, bool allowNulls)
        {
            return Create(numElements, allowNulls, -1);
        }

        /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @return Tuple builder.
     */
        public static BinaryTupleBuilder Create(int numElements, bool allowNulls, int totalValueSize)
        {
            return new BinaryTupleBuilder(numElements, allowNulls, totalValueSize);
        }

        /**
     * Check if the binary tuple contains a null map.
     */
        public bool hasNullMap()
        {
            return _entryBase > BinaryTupleCommon.HeaderSize;
        }

        /**
     * Append a NULL value for the current element.
     *
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendNull()
        {
            if (!hasNullMap())
            {
                throw new InvalidOperationException("Appending a NULL value in binary tuple builder with disabled NULLs");
            }

            _hasNullValues = true;

            int nullIndex = BinaryTupleCommon.NullOffset(_elementIndex);
            byte nullMask = BinaryTupleCommon.NullMask(_elementIndex);

            var span = _buffer.GetSpan(nullIndex, 1);
            
            span[0] |= nullMask;

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendByte(sbyte value)
        {
            if (value != 0)
            {
                putByte(value);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendShort(short value)
        {
            if (sbyte.MinValue <= value && value <= sbyte.MaxValue)
            {
                return appendByte((sbyte)value);
            }

            putShort(value);
            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendInt(int value)
        {
            if (sbyte.MinValue <= value && value <= sbyte.MaxValue)
            {
                return appendByte((sbyte)value);
            }

            if (short.MinValue <= value && value <= short.MaxValue)
            {
                putShort((short)value);
            }
            else
            {
                putInt(value);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendLong(long value)
        {
            if (short.MinValue <= value && value <= short.MaxValue)
            {
                return appendShort((short)value);
            }

            if (int.MinValue <= value && value <= int.MaxValue)
            {
                putInt((int)value);
            }
            else
            {
                putLong(value);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendFloat(float value)
        {
            if (value != 0.0F)
            {
                putFloat(value);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendDouble(double value)
        {
            if (value == ((float)value))
            {
                return appendFloat((float)value);
            }

            putDouble(value);
            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendString(string value)
        {
            putString(value);

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendBytes(Span<byte> value)
        {
            putBytes(value);
            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendGuid(Guid value) => throw new NotSupportedException("TODO IGNITE-17593");

        /**
     * Gets the current element index.
     *
     * @return Element index.
     */
        public int ElementIndex => _elementIndex;

        /**
     * Finalize tuple building.
     *
     * <p>NOTE: This should be called only once as it messes up with accumulated internal data.
     *
     * @return Buffer with tuple bytes.
     */
        public Memory<byte> Build()
        {
            int offset = 0;

            int valueSize = _buffer.Position - _valueBase;
            byte flags = BinaryTupleCommon.ValueSizeToFlags(valueSize);
            int desiredEntrySize = BinaryTupleCommon.FlagsToEntrySize(flags);

            // Shrink the offset table if needed.
            if (desiredEntrySize != _entrySize)
            {
                if (desiredEntrySize > _entrySize)
                {
                    throw new InvalidOperationException("Offset entry overflow in binary tuple builder");
                }

                Debug.Assert(_entrySize == 4 || _entrySize == 2);
                Debug.Assert(desiredEntrySize == 2 || desiredEntrySize == 1);

                int getIndex = _valueBase;
                int putIndex = _valueBase;
                while (getIndex > _entryBase)
                {
                    getIndex -= _entrySize;
                    putIndex -= desiredEntrySize;

                    int value;
                    if (_entrySize == 4)
                    {
                        value = _buffer.ReadInt(getIndex);
                    }
                    else
                    {
                        value = _buffer.ReadShort(getIndex);
                    }

                    if (desiredEntrySize == 1)
                    {
                        _buffer.WriteByte((byte)value, putIndex);
                    }
                    else
                    {
                        _buffer.WriteShort((short)value, putIndex);
                    }
                }

                offset = (_entrySize - desiredEntrySize) * _numElements;
            }

            // Drop or move null map if needed.
            if (hasNullMap())
            {
                if (!_hasNullValues)
                {
                    offset += BinaryTupleCommon.NullMapSize(_numElements);
                }
                else
                {
                    flags |= BinaryTupleCommon.NullmapFlag;
                    if (offset != 0)
                    {
                        int n = BinaryTupleCommon.NullMapSize(_numElements);
                        for (int i = BinaryTupleCommon.HeaderSize + n - 1; i >= BinaryTupleCommon.HeaderSize; i--)
                        {
                            _buffer.WriteByte(_buffer.ReadByte(i), i + offset);
                        }
                    }
                }
            }

            _buffer.WriteByte(flags, offset);

            return _buffer.GetWrittenMemory().Slice(offset);
        }

        /** Put a byte value to the buffer extending it if needed. */
        private void putByte(sbyte value) => _buffer.WriteByte(unchecked((byte)value));

        /** Put a short value to the buffer extending it if needed. */
        private void putShort(short value) => _buffer.WriteShort(value);

        /** Put an int value to the buffer extending it if needed. */
        private void putInt(int value) => _buffer.WriteInt(value);

        /** Put a long value to the buffer extending it if needed. */
        private void putLong(long value) => _buffer.WriteLong(value);

        /** Put a float value to the buffer extending it if needed. */
        private unsafe void putFloat(float value) => putInt(*(int*)&value);

        /** Put a double value to the buffer extending it if needed. */
        private unsafe void putDouble(double value) => putLong(*(long*)&value);

        /** Put bytes to the buffer extending it if needed. */
        private void putBytes(Span<byte> bytes) => bytes.CopyTo(GetSpan(bytes.Length));

        /** Put a string to the buffer extending it if needed. */
        private void putString(String value)
        {
            var maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
            var span = _buffer.GetSpan(maxByteCount);

            var actualBytes = Encoding.UTF8.GetBytes(value, span);

            _buffer.Advance(actualBytes);
        }

        /** Proceed to the next tuple element. */
        private BinaryTupleBuilder proceed()
        {
            Debug.Assert(_elementIndex < _numElements);

            int offset = _buffer.Position - _valueBase;

            switch (_entrySize)
            {
                case 1:
                    _buffer.WriteByte((byte)offset, _entryBase + _elementIndex);
                    break;

                case 2:
                    _buffer.WriteShort((short)offset, _entryBase + _elementIndex);
                    break;

                case 4:
                    _buffer.WriteInt(offset, _entryBase + _elementIndex);
                    break;

                default:
                    throw new ArgumentOutOfRangeException("Tuple entry size is invalid.");
            }

            _elementIndex++;
            return this;
        }

        /** Do our best to find initial buffer capacity. */
        private int estimateBufferCapacity(int totalValueSize)
        {
            if (totalValueSize < 0)
            {
                totalValueSize = Math.Max(_numElements * 8, DefaultBufferSize);
            }

            return _valueBase + totalValueSize;
        }

        private Span<byte> GetSpan(int size)
        {
            var span = _buffer.GetSpan(size);

            _buffer.Advance(size);

            return span;
        }
    }
}
