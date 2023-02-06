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

package org.apache.ignite.internal.binarytuple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Utility to construct a binary tuple.
 */
public class BinaryTupleBuilder {
    /** The buffer size allocated for values when we do not know anything better. */
    private static final int DEFAULT_BUFFER_SIZE = 4000;

    /** Current element. */
    private int elementIndex = 0;

    /** Number of elements in the tuple. */
    private final int numElements;

    /** Size of an offset table entry. */
    private final int entrySize;

    /** Position of the varlen offset table. */
    private final int entryBase;

    /** Starting position of variable-length values. */
    private final int valueBase;

    /** Buffer for tuple content. */
    protected ByteBuffer buffer;

    /** Charset encoder for strings. Initialized lazily. */
    private CharsetEncoder cachedEncoder;

    /** Flag indicating if any NULL values were really put here. */
    private boolean hasNullValues = false;

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     */
    public BinaryTupleBuilder(int numElements, boolean allowNulls) {
        this(numElements, allowNulls, -1);
    }

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    public BinaryTupleBuilder(int numElements, boolean allowNulls, int totalValueSize) {
        this.numElements = numElements;

        int base = BinaryTupleCommon.HEADER_SIZE;
        if (allowNulls) {
            base += BinaryTupleCommon.nullMapSize(numElements);
        }

        entryBase = base;

        if (totalValueSize < 0) {
            entrySize = Integer.BYTES;
        } else {
            entrySize = BinaryTupleCommon.flagsToEntrySize(BinaryTupleCommon.valueSizeToFlags(totalValueSize));
        }

        valueBase = base + entrySize * numElements;

        allocate(totalValueSize);
    }

    /**
     * Check if the binary tuple contains a null map.
     */
    public boolean hasNullMap() {
        return entryBase > BinaryTupleCommon.HEADER_SIZE;
    }

    /**
     * Append a NULL value for the current element.
     *
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendNull() {
        if (!hasNullMap()) {
            throw new IllegalStateException("Appending a NULL value in binary tuple builder with disabled NULLs");
        }

        hasNullValues = true;

        int nullIndex = BinaryTupleCommon.nullOffset(elementIndex);
        byte nullMask = BinaryTupleCommon.nullMask(elementIndex);
        buffer.put(nullIndex, (byte) (buffer.get(nullIndex) | nullMask));

        return proceed();
    }

    /**
     * Append a default (empty) value for the current element.
     *
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDefault() {
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendByte(byte value) {
        if (value != 0) {
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
    public BinaryTupleBuilder appendByte(Byte value) {
        return value == null ? appendNull() : appendByte(value.byteValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendShort(short value) {
        if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
            return appendByte((byte) value);
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
    public BinaryTupleBuilder appendShort(Short value) {
        return value == null ? appendNull() : appendShort(value.shortValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendInt(int value) {
        if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
            return appendByte((byte) value);
        }
        if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
            putShort((short) value);
        } else {
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
    public BinaryTupleBuilder appendInt(@Nullable Integer value) {
        return value == null ? appendNull() : appendInt(value.intValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendLong(long value) {
        if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
            return appendShort((short) value);
        }
        if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
            putInt((int) value);
        } else {
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
    public BinaryTupleBuilder appendLong(Long value) {
        return value == null ? appendNull() : appendLong(value.longValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendFloat(float value) {
        if (value != 0.0F) {
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
    public BinaryTupleBuilder appendFloat(Float value) {
        return value == null ? appendNull() : appendFloat(value.floatValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDouble(double value) {
        if (value == ((float) value)) {
            return appendFloat((float) value);
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
    public BinaryTupleBuilder appendDouble(Double value) {
        return value == null ? appendNull() : appendDouble(value.doubleValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendNumberNotNull(BigInteger value) {
        if (!value.equals(BigInteger.ZERO)) {
            putBytes(value.toByteArray());
        }

        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendNumber(BigInteger value) {
        return value == null ? appendNull() : appendNumberNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @param scale Decimal scale.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDecimalNotNull(BigDecimal value, int scale) {
        putBytes(value.setScale(scale, RoundingMode.HALF_UP).unscaledValue().toByteArray());
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @param scale Decimal scale.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDecimal(BigDecimal value, int scale) {
        return value == null ? appendNull() : appendDecimalNotNull(value, scale);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendStringNotNull(String value) {
        try {
            putString(value);
        } catch (CharacterCodingException e) {
            throw new BinaryTupleFormatException("Failed to encode string in binary tuple builder", e);
        }
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendString(@Nullable String value) {
        return value == null ? appendNull() : appendStringNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBytesNotNull(byte[] value) {
        putBytes(value);
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBytes(byte[] value) {
        return value == null ? appendNull() : appendBytesNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendUuidNotNull(UUID value) {
        long lsb = value.getLeastSignificantBits();
        long msb = value.getMostSignificantBits();
        if ((lsb | msb) != 0L) {
            putLong(msb);
            putLong(lsb);
        }
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendUuid(UUID value) {
        return value == null ? appendNull() : appendUuidNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBitmaskNotNull(BitSet value) {
        putBytes(value.toByteArray());
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBitmask(BitSet value) {
        return value == null ? appendNull() : appendBitmaskNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDateNotNull(LocalDate value) {
        if (value != BinaryTupleCommon.DEFAULT_DATE) {
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
    public BinaryTupleBuilder appendDate(LocalDate value) {
        return value == null ? appendNull() : appendDateNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendTimeNotNull(LocalTime value) {
        if (value != BinaryTupleCommon.DEFAULT_TIME) {
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
    public BinaryTupleBuilder appendTime(LocalTime value) {
        return value == null ? appendNull() : appendTimeNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDateTimeNotNull(LocalDateTime value) {
        if (value != BinaryTupleCommon.DEFAULT_DATE_TIME) {
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
    public BinaryTupleBuilder appendDateTime(LocalDateTime value) {
        return value == null ? appendNull() : appendDateTimeNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendTimestampNotNull(Instant value) {
        if (value != BinaryTupleCommon.DEFAULT_TIMESTAMP) {
            long seconds = value.getEpochSecond();
            int nanos = value.getNano();
            putLong(seconds);
            if (nanos != 0) {
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
    public BinaryTupleBuilder appendTimestamp(Instant value) {
        return value == null ? appendNull() : appendTimestampNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDurationNotNull(Duration value) {
        if (value != BinaryTupleCommon.DEFAULT_DURATION) {
            long seconds = value.getSeconds();
            int nanos = value.getNano();
            putLong(seconds);
            if (nanos != 0) {
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
    public BinaryTupleBuilder appendDuration(Duration value) {
        return value == null ? appendNull() : appendDurationNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendPeriodNotNull(Period value) {
        if (value != BinaryTupleCommon.DEFAULT_PERIOD) {
            int years = value.getYears();
            int months = value.getMonths();
            int days = value.getDays();

            if (Byte.MIN_VALUE <= years && years <= Byte.MAX_VALUE
                    && Byte.MIN_VALUE <= months && months <= Byte.MAX_VALUE
                    && Byte.MIN_VALUE <= days && days <= Byte.MAX_VALUE) {
                putByte((byte) years);
                putByte((byte) months);
                putByte((byte) days);
            } else if (Short.MIN_VALUE <= years && years <= Short.MAX_VALUE
                    && Short.MIN_VALUE <= months && months <= Short.MAX_VALUE
                    && Short.MIN_VALUE <= days && days <= Short.MAX_VALUE) {
                putShort((short) years);
                putShort((short) months);
                putShort((short) days);
            } else {
                putInt(years);
                putInt(months);
                putInt(days);
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
    public BinaryTupleBuilder appendPeriod(Period value) {
        return value == null ? appendNull() : appendPeriodNotNull(value);
    }

    /**
     * Append some arbitrary content as the current element.
     *
     * @param bytes Buffer with element raw bytes.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendElementBytes(ByteBuffer bytes) {
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
    public BinaryTupleBuilder appendElementBytes(ByteBuffer bytes, int offset, int length) {
        putElement(bytes, offset, length);
        return proceed();
    }

    /**
     * Gets the current element index.
     *
     * @return Element index.
     */
    public int elementIndex() {
        return elementIndex;
    }

    /**
     * Gets the expected number of tuple elements.
     *
     * @return Expected number of tuple elements.
     */
    public int numElements() {
        return numElements;
    }

    /**
     * Finalize tuple building.
     *
     * <p>NOTE: This should be called only once as it messes up with accumulated internal data.
     *
     * @return Buffer with tuple bytes.
     */
    public ByteBuffer build() {
        return buildInternal().slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    protected ByteBuffer buildInternal() {
        int offset = 0;

        int valueSize = buffer.position() - valueBase;
        byte flags = BinaryTupleCommon.valueSizeToFlags(valueSize);
        int desiredEntrySize = BinaryTupleCommon.flagsToEntrySize(flags);

        // Shrink the offset table if needed.
        if (desiredEntrySize != entrySize) {
            if (desiredEntrySize > entrySize) {
                throw new IllegalStateException("Offset entry overflow in binary tuple builder");
            }

            assert entrySize == 4 || entrySize == 2;
            assert desiredEntrySize == 2 || desiredEntrySize == 1;

            int getIndex = valueBase;
            int putIndex = valueBase;
            while (getIndex > entryBase) {
                getIndex -= entrySize;
                putIndex -= desiredEntrySize;

                int value;
                if (entrySize == 4) {
                    value = buffer.getInt(getIndex);
                } else {
                    value = Short.toUnsignedInt(buffer.getShort(getIndex));
                }
                if (desiredEntrySize == 1) {
                    buffer.put(putIndex, (byte) value);
                } else {
                    buffer.putShort(putIndex, (short) value);
                }
            }

            offset = (entrySize - desiredEntrySize) * numElements;
        }

        // Drop or move null map if needed.
        if (hasNullMap()) {
            if (!hasNullValues) {
                offset += BinaryTupleCommon.nullMapSize(numElements);
            } else {
                flags |= BinaryTupleCommon.NULLMAP_FLAG;
                if (offset != 0) {
                    int n = BinaryTupleCommon.nullMapSize(numElements);
                    for (int i = BinaryTupleCommon.HEADER_SIZE + n - 1; i >= BinaryTupleCommon.HEADER_SIZE; i--) {
                        buffer.put(i + offset, buffer.get(i));
                    }
                }
            }
        }

        buffer.put(offset, flags);

        return buffer.flip().position(offset);
    }

    /** Put a byte value to the buffer extending it if needed. */
    private void putByte(byte value) {
        ensure(Byte.BYTES);
        buffer.put(value);
    }

    /** Put a short value to the buffer extending it if needed. */
    private void putShort(short value) {
        ensure(Short.BYTES);
        buffer.putShort(value);
    }

    /** Put an int value to the buffer extending it if needed. */
    private void putInt(int value) {
        ensure(Integer.BYTES);
        buffer.putInt(value);
    }

    /** Put a long value to the buffer extending it if needed. */
    private void putLong(long value) {
        ensure(Long.BYTES);
        buffer.putLong(value);
    }

    /** Put a float value to the buffer extending it if needed. */
    private void putFloat(float value) {
        ensure(Float.BYTES);
        buffer.putFloat(value);
    }

    /** Put a double value to the buffer extending it if needed. */
    private void putDouble(double value) {
        ensure(Double.BYTES);
        buffer.putDouble(value);
    }

    /** Put bytes to the buffer extending it if needed. */
    private void putBytes(byte[] bytes) {
        ensure(bytes.length);
        buffer.put(bytes);
    }

    /** Put a string to the buffer extending it if needed. */
    private void putString(String value) throws CharacterCodingException {
        CharsetEncoder coder = encoder().reset();
        CharBuffer input = CharBuffer.wrap(value);

        CoderResult result = coder.encode(input, buffer, true);
        while (result.isOverflow()) {
            grow((int) coder.maxBytesPerChar());
            result = coder.encode(input, buffer, true);
        }

        if (result.isUnderflow()) {
            result = coder.flush(buffer);
            while (result.isOverflow()) {
                grow((int) coder.maxBytesPerChar());
                result = coder.flush(buffer);
            }
        }

        if (result.isError()) {
            result.throwException();
        }
    }

    /** Put a date to the buffer extending it if needed. */
    private void putDate(LocalDate value) {
        int year = value.getYear();
        int month = value.getMonthValue();
        int day = value.getDayOfMonth();

        int date = (year << 9) | (month << 5) | day;

        putShort((short) date);
        putByte((byte) (date >> 16));
    }

    /** Put a time to the buffer extending it if needed. */
    private void putTime(LocalTime value) {
        long hour = value.getHour();
        long minute = value.getMinute();
        long second = value.getSecond();
        long nanos = value.getNano();

        if ((nanos % 1000) != 0) {
            long time = (hour << 42) | (minute << 36) | (second << 30) | nanos;
            putInt((int) time);
            putShort((short) (time >>> 32));
        } else if ((nanos % 1000000) != 0) {
            long time = (hour << 32) | (minute << 26) | (second << 20) | (nanos / 1000);
            putInt((int) time);
            putByte((byte) (time >>> 32));
        } else {
            long time = (hour << 22) | (minute << 16) | (second << 10) | (nanos / 1000000);
            putInt((int) time);
        }
    }

    /** Put element bytes to the buffer extending it if needed. */
    private void putElement(ByteBuffer bytes) {
        ensure(bytes.remaining());

        int pos = bytes.position();

        buffer.put(bytes);

        bytes.position(pos);
    }

    /** Put element bytes to the buffer extending it if needed. */
    private void putElement(ByteBuffer bytes, int offset, int length) {
        assert bytes.limit() >= (offset + length);
        ensure(length);
        buffer.put(bytes.asReadOnlyBuffer().position(offset).limit(offset + length));
    }

    /** Proceed to the next tuple element. */
    private BinaryTupleBuilder proceed() {
        assert elementIndex < numElements;

        int offset = buffer.position() - valueBase;
        switch (entrySize) {
            case Byte.BYTES:
                buffer.put(entryBase + elementIndex, (byte) offset);
                break;
            case Short.BYTES:
                buffer.putShort(entryBase + elementIndex * Short.BYTES, (short) offset);
                break;
            case Integer.BYTES:
                buffer.putInt(entryBase + elementIndex * Integer.BYTES, offset);
                break;
            default:
                assert false;
        }

        elementIndex++;
        return this;
    }

    /** Allocate a non-direct buffer for tuple. */
    private void allocate(int totalValueSize) {
        buffer = allocateBuffer(estimateBufferCapacity(totalValueSize));
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(valueBase);
    }

    /** Allocate a non-direct buffer for tuple. */
    protected ByteBuffer allocateBuffer(int capacity) {
        return ByteBuffer.allocate(capacity);
    }

    /** Do our best to find initial buffer capacity. */
    private int estimateBufferCapacity(int totalValueSize) {
        if (totalValueSize < 0) {
            totalValueSize = Integer.max(numElements * 8, DEFAULT_BUFFER_SIZE);
        }
        return valueBase + totalValueSize;
    }

    /** Ensure that the buffer can fit the required size. */
    private void ensure(int size) {
        if (buffer.remaining() < size) {
            grow(size);
        }
    }

    /** Reallocate the buffer increasing its capacity to fit the required size. */
    private void grow(int size) {
        int capacity = buffer.capacity();
        do {
            capacity *= 2;
            if (capacity < 0) {
                throw new BinaryTupleFormatException("Buffer overflow in binary tuple builder");
            }
        } while ((capacity - buffer.position()) < size);

        buffer = reallocateBuffer(capacity);
    }

    /** Reallocate the buffer to the new capacity. */
    protected ByteBuffer reallocateBuffer(int capacity) {
        ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
        newBuffer.order(ByteOrder.LITTLE_ENDIAN);
        newBuffer.put(buffer.flip());
        return newBuffer;
    }

    /**
     * Get UTF-8 string encoder.
     */
    private CharsetEncoder encoder() {
        if (cachedEncoder == null) {
            cachedEncoder = StandardCharsets.UTF_8.newEncoder();
        }
        return cachedEncoder;
    }
}
