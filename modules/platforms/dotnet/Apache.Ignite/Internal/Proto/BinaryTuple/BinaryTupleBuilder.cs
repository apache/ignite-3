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
    using System.Diagnostics.CodeAnalysis;
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
        public BinaryTupleBuilder appendByte(byte value)
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
            if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE)
            {
                return appendByte((byte)value);
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
            if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE)
            {
                return appendByte((byte)value);
            }

            if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE)
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
            if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE)
            {
                return appendShort((short)value);
            }

            if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE)
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
        public BinaryTupleBuilder appendBytesNotNull(Span<byte> value)
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
        public BinaryTupleBuilder appendBytes(byte[] value)
        {
            return value == null ? appendNull() : appendBytesNotNull(value);
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendUuidNotNull(@NotNull UUID value)
        {
            long lsb = value.getLeastSignificantBits();
            long msb = value.getMostSignificantBits();
            if ((lsb | msb) != 0L)
            {
                putLong(lsb);
                putLong(msb);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendUuid(UUID value)
        {
            return value == null ? appendNull() : appendUuidNotNull(value);
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendBitmaskNotNull(@NotNull BitSet value)
        {
            putBytes(value.toByteArray());
            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendBitmask(BitSet value)
        {
            return value == null ? appendNull() : appendBitmaskNotNull(value);
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendDateNotNull(@NotNull LocalDate value)
        {
            if (value != BinaryTupleCommon.DEFAULT_DATE)
            {
                putDate(value);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendDate(LocalDate value)
        {
            return value == null ? appendNull() : appendDateNotNull(value);
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendTimeNotNull(@NotNull LocalTime value)
        {
            if (value != BinaryTupleCommon.DEFAULT_TIME)
            {
                putTime(value);
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendTime(LocalTime value)
        {
            return value == null ? appendNull() : appendTimeNotNull(value);
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendDateTimeNotNull(@NotNull LocalDateTime value)
        {
            if (value != BinaryTupleCommon.DEFAULT_DATE_TIME)
            {
                putDate(value.toLocalDate());
                putTime(value.toLocalTime());
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendDateTime(LocalDateTime value)
        {
            return value == null ? appendNull() : appendDateTimeNotNull(value);
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendTimestampNotNull(@NotNull Instant value)
        {
            if (value != BinaryTupleCommon.DEFAULT_TIMESTAMP)
            {
                long seconds = value.getEpochSecond();
                int nanos = value.getNano();
                putLong(seconds);
                if (nanos != 0)
                {
                    putInt(nanos);
                }
            }

            return proceed();
        }

        /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendTimestamp(Instant value)
        {
            return value == null ? appendNull() : appendTimestampNotNull(value);
        }

        /**
     * Append some arbitrary content as the current element.
     *
     * @param bytes Buffer with element raw bytes.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendElementBytes(@NotNull ByteBuffer bytes)
        {
            putElement(bytes);
            return proceed();
        }

        /**
     * Append some arbitrary content as the current element.
     *
     * @param bytes Buffer with element raw bytes.
     * @param offset Offset of the element in the buffer.
     * @param length Length of the element in the buffer.
     * @return {@code this} for chaining.
     */
        public BinaryTupleBuilder appendElementBytes(@NotNull ByteBuffer bytes, int offset, int length) {
            putElement(bytes, offset, length);
            return proceed();
        }

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
        public object Build()
        {
            int offset = 0;

            int valueSize = _buffer.position() - _valueBase;
            byte flags = BinaryTupleCommon.valueSizeToFlags(valueSize);
            int desiredEntrySize = BinaryTupleCommon.flagsToEntrySize(flags);

            // Shrink the offset table if needed.
            if (desiredEntrySize != _entrySize)
            {
                if (desiredEntrySize > _entrySize)
                {
                    throw new IllegalStateException("Offset entry overflow in binary tuple builder");
                }

                assert _entrySize == 4 || _entrySize == 2;
                assert desiredEntrySize == 2 || desiredEntrySize == 1;

                int getIndex = _valueBase;
                int putIndex = _valueBase;
                while (getIndex > _entryBase)
                {
                    getIndex -= _entrySize;
                    putIndex -= desiredEntrySize;

                    int value;
                    if (_entrySize == 4)
                    {
                        value = _buffer.getInt(getIndex);
                    }
                    else
                    {
                        value = Short.toUnsignedInt(_buffer.getShort(getIndex));
                    }

                    if (desiredEntrySize == 1)
                    {
                        _buffer.put(putIndex, (byte)value);
                    }
                    else
                    {
                        _buffer.putShort(putIndex, (short)value);
                    }
                }

                offset = (_entrySize - desiredEntrySize) * _numElements;
            }

            // Drop or move null map if needed.
            if (hasNullMap())
            {
                if (!_hasNullValues)
                {
                    offset += BinaryTupleCommon.nullMapSize(_numElements);
                }
                else
                {
                    flags |= BinaryTupleCommon.NULLMAP_FLAG;
                    if (offset != 0)
                    {
                        int n = BinaryTupleCommon.nullMapSize(_numElements);
                        for (int i = BinaryTupleCommon.HEADER_SIZE + n - 1; i >= BinaryTupleCommon.HEADER_SIZE; i--)
                        {
                            _buffer.put(i + offset, _buffer.get(i));
                        }
                    }
                }
            }

            _buffer.put(offset, flags);

            return _buffer.flip().position(offset).slice().order(ByteOrder.LITTLE_ENDIAN);
        }

        /** Put a byte value to the buffer extending it if needed. */
        private void putByte(byte value)
        {
            ensure(1);
            _buffer.put(value);
        }

        /** Put a short value to the buffer extending it if needed. */
        private void putShort(short value)
        {
            ensure(2);
            _buffer.putShort(value);
        }

        /** Put an int value to the buffer extending it if needed. */
        private void putInt(int value)
        {
            ensure(4);
            _buffer.putInt(value);
        }

        /** Put a long value to the buffer extending it if needed. */
        private void putLong(long value)
        {
            ensure(8);
            _buffer.putLong(value);
        }

        /** Put a float value to the buffer extending it if needed. */
        private void putFloat(float value)
        {
            ensure(Float.BYTES);
            _buffer.putFloat(value);
        }

        /** Put a double value to the buffer extending it if needed. */
        private void putDouble(double value)
        {
            ensure(Double.BYTES);
            _buffer.putDouble(value);
        }

        /** Put bytes to the buffer extending it if needed. */
        private void putBytes(byte[] bytes)
        {
            ensure(bytes.length);
            _buffer.put(bytes);
        }

        /** Put a string to the buffer extending it if needed. */
        private void putString(String value)
        {
            CharsetEncoder coder = encoder().reset();
            CharBuffer input = CharBuffer.wrap(value);

            CoderResult result = coder.encode(input, _buffer, true);
            while (result.isOverflow())
            {
                grow((int)coder.maxBytesPerChar());
                result = coder.encode(input, _buffer, true);
            }

            if (result.isUnderflow())
            {
                result = coder.flush(_buffer);
                while (result.isOverflow())
                {
                    grow((int)coder.maxBytesPerChar());
                    result = coder.flush(_buffer);
                }
            }

            if (result.isError())
            {
                result.throwException();
            }
        }

        /** Put a date to the buffer extending it if needed. */
        private void putDate(LocalDate value)
        {
            int year = value.getYear();
            int month = value.getMonthValue();
            int day = value.getDayOfMonth();

            int date = (year << 9) | (month << 5) | day;

            putShort((short)date);
            putByte((byte)(date >> 16));
        }

        /** Put a time to the buffer extending it if needed. */
        private void putTime(LocalTime value)
        {
            long hour = value.getHour();
            long minute = value.getMinute();
            long second = value.getSecond();
            long nanos = value.getNano();

            if ((nanos % 1000) != 0)
            {
                long time = (hour << 42) | (minute << 36) | (second << 30) | nanos;
                putInt((int)time);
                putShort((short)(time >>> 32));
            }
            else if ((nanos % 1000000) != 0)
            {
                long time = (hour << 32) | (minute << 26) | (second << 20) | (nanos / 1000);
                putInt((int)time);
                putByte((byte)(time >>> 32));
            }
            else
            {
                long time = (hour << 22) | (minute << 16) | (second << 10) | (nanos / 1000000);
                putInt((int)time);
            }
        }

        /** Put element bytes to the buffer extending it if needed. */
        private void putElement(ByteBuffer bytes)
        {
            ensure(bytes.remaining());
            _buffer.put(bytes);
        }

        /** Put element bytes to the buffer extending it if needed. */
        private void putElement(ByteBuffer bytes, int offset, int length)
        {
            assert bytes.limit() <= (offset + length);
            ensure(length);
            _buffer.put(bytes.asReadOnlyBuffer().position(offset).limit(offset + length));
        }

        /** Proceed to the next tuple element. */
        private BinaryTupleBuilder proceed()
        {
            assert _elementIndex < _numElements;

            int offset = _buffer.position() - _valueBase;
            switch (_entrySize)
            {
                case Byte.BYTES:
                    _buffer.put(_entryBase + _elementIndex, (byte)offset);
                    break;
                case 2:
                    _buffer.putShort(_entryBase + _elementIndex * 2, (short)offset);
                    break;
                case 4:
                    _buffer.putInt(_entryBase + _elementIndex * 4, offset);
                    break;
                default:
                    assert false;
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

        /** Ensure that the buffer can fit the required size. */
        private void ensure(int size)
        {
            if (_buffer.remaining() < size)
            {
                grow(size);
            }
        }

        /** Reallocate the buffer increasing its capacity to fit the required size. */
        private void grow(int size)
        {
            int capacity = _buffer.Length;
            do
            {
                capacity *= 2;
                if (capacity < 0)
                {
                    throw new IgniteClientException("Buffer overflow in binary tuple builder");
                }
            } while ((capacity - _buffer.position()) < size);

            ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
            newBuffer.order(ByteOrder.LITTLE_ENDIAN);
            newBuffer.put(_buffer.flip());

            _buffer = newBuffer;
        }
    }
}
