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
import org.apache.ignite.internal.util.ByteUtils;
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
    private ByteBuffer buffer;

    /** Charset encoder for strings. Initialized lazily. */
    private CharsetEncoder cachedEncoder;

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     */
    public BinaryTupleBuilder(int numElements) {
        this(numElements, -1);
    }

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    public BinaryTupleBuilder(int numElements, int totalValueSize) {
        this(numElements, totalValueSize, true);
    }

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @param exactEstimate Whether the total size is exact estimate or approximate. The
     *      difference here is with exact estimate allocation will be optimal, while with
     *      approximate estimate some excess allocation is possible.
     */
    public BinaryTupleBuilder(int numElements, int totalValueSize, boolean exactEstimate) {
        this.numElements = numElements;

        entryBase = BinaryTupleCommon.HEADER_SIZE;

        if (totalValueSize < 0 || !exactEstimate) {
            entrySize = Integer.BYTES;
        } else {
            entrySize = BinaryTupleCommon.flagsToEntrySize(BinaryTupleCommon.valueSizeToFlags(totalValueSize));
        }

        valueBase = entryBase + entrySize * numElements;

        allocate(totalValueSize);
    }

    /**
     * Append a NULL value for the current element.
     *
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendNull() {
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBoolean(boolean value) {
        putByte(ByteUtils.booleanToByte(value));
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBoolean(Boolean value) {
        return value == null ? appendNull() : appendBoolean(value.booleanValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendByte(byte value) {
        putByte(value);
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
        putFloat(value);
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
        putBytes(value.toByteArray());
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
        if (value.scale() > scale) {
            value = value.setScale(scale, RoundingMode.HALF_UP);
        }

        BigDecimal noZeros = value.stripTrailingZeros();
        if (noZeros.scale() <= Short.MAX_VALUE && noZeros.scale() >= Short.MIN_VALUE) {
            // Use more compact representation if possible.
            value = noZeros;
        }

        // See CatalogUtils.MAX_DECIMAL_SCALE = Short.MAX_VALUE
        if (value.scale() > Short.MAX_VALUE) {
            throw new BinaryTupleFormatException("Decimal scale is too large: " + value.scale() + " > " + Short.MAX_VALUE);
        }

        if (value.scale() < Short.MIN_VALUE) {
            throw new BinaryTupleFormatException("Decimal scale is too small: " + value.scale() + " < " + Short.MIN_VALUE);
        }

        putShort((short) value.scale());
        putBytes(value.unscaledValue().toByteArray());

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
        putBytesWithEmptyCheck(value);
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBytes(byte @Nullable [] value) {
        return value == null ? appendNull() : appendBytesNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendUuidNotNull(UUID value) {
        putLong(value.getMostSignificantBits());
        putLong(value.getLeastSignificantBits());
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
        putBytesWithEmptyCheck(value.toByteArray());
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
        putDate(value);
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
        putTime(value);
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
        putDate(value.toLocalDate());
        putTime(value.toLocalTime());
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
        long seconds = value.getEpochSecond();
        int nanos = value.getNano();
        putLong(seconds);
        if (nanos != 0) {
            putInt(nanos);
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
        long seconds = value.getSeconds();
        int nanos = value.getNano();
        putLong(seconds);
        if (nanos != 0) {
            putInt(nanos);
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

    private void putBytesWithEmptyCheck(byte[] bytes) {
        if (bytes.length == 0 || bytes[0] == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            ensure(bytes.length + 1);
            buffer.put(BinaryTupleCommon.VARLEN_EMPTY_BYTE);
        } else {
            ensure(bytes.length);
        }
        buffer.put(bytes);
    }

    /** Put a string to the buffer extending it if needed. */
    private void putString(String value) throws CharacterCodingException {
        if (value.isEmpty()) {
            ensure(1);
            buffer.put(BinaryTupleCommon.VARLEN_EMPTY_BYTE);
            return;
        }

        int begin = buffer.position();

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

        // UTF-8 encoded strings should not start with 0x80 (character codes larger than 127 have a multi-byte encoding).
        // We trust this but verify.
        if (buffer.get(begin) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            throw new BinaryTupleFormatException("Failed to encode a string element: resulting payload starts with invalid 0x80 byte");
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
    private void putElement(ByteBuffer bytes, int offset, int length) {
        assert bytes.limit() >= (offset + length);
        ensure(length);
        buffer.put(bytes.asReadOnlyBuffer().position(offset).limit(offset + length));
    }

    /** Proceed to the next tuple element. */
    private BinaryTupleBuilder proceed() {
        assert elementIndex < numElements : "Element index overflow: " + elementIndex + " >= " + numElements;

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
        buffer = ByteBuffer.allocate(estimateBufferCapacity(totalValueSize));
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(valueBase);
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

        ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
        newBuffer.order(ByteOrder.LITTLE_ENDIAN);
        newBuffer.put(buffer.flip());

        buffer = newBuffer;
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
