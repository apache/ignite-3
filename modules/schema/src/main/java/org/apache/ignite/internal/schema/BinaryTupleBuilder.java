/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;

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

    /** Flag indicating if any NULL values were really put here. */
    private boolean hasNullValues = false;

    /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    private BinaryTupleBuilder(int numElements, boolean allowNulls, int totalValueSize) {
        this.numElements = numElements;

        int base = BinaryTupleSchema.HEADER_SIZE;
        if (allowNulls) {
            base += BinaryTupleSchema.nullMapSize(numElements);
        }

        entryBase = base;

        if (totalValueSize < 0) {
            entrySize = 4;
        } else {
            entrySize = BinaryTupleSchema.flagsToEntrySize(BinaryTupleSchema.valueSizeToFlags(totalValueSize));
        }

        valueBase = base + entrySize * numElements;
    }

    /**
     * Creates a builder.
     *
     * @param schema Tuple schema.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder create(BinaryTupleSchema schema) {
        return create(schema.elementCount(), schema.hasNullableElements());
    }

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder create(int numElements, boolean allowNulls) {
        return create(numElements, allowNulls, -1);
    }

    /**
     * Creates a builder.
     *
     * @param schema Tuple schema.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder create(BinaryTupleSchema schema, int totalValueSize) {
        return create(schema.elementCount(), schema.hasNullableElements(), totalValueSize);
    }

    /**
     * Creates a builder.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder create(int numElements, boolean allowNulls, int totalValueSize) {
        var builder = new BinaryTupleBuilder(numElements, allowNulls, totalValueSize);
        builder.allocate(totalValueSize);
        return builder;
    }

    /**
     * Creates a builder with direct buffer.
     *
     * @param schema Tuple schema.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder createWithDirectBuffer(BinaryTupleSchema schema) {
        return createWithDirectBuffer(schema.elementCount(), schema.hasNullableElements());
    }

    /**
     * Creates a builder with direct buffer.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder createWithDirectBuffer(int numElements, boolean allowNulls) {
        return createWithDirectBuffer(numElements, allowNulls, -1);
    }

    /**
     * Creates a builder with direct buffer.
     *
     * @param schema Tuple schema.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder createWithDirectBuffer(BinaryTupleSchema schema, int totalValueSize) {
        return createWithDirectBuffer(schema.elementCount(), schema.hasNullableElements(), totalValueSize);
    }

    /**
     * Creates a builder with direct buffer.
     *
     * @param numElements Number of tuple elements.
     * @param allowNulls True if NULL values are possible, false otherwise.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @return Tuple builder.
     */
    public BinaryTupleBuilder createWithDirectBuffer(int numElements, boolean allowNulls, int totalValueSize) {
        var builder = new BinaryTupleBuilder(numElements, allowNulls, totalValueSize);
        builder.allocateDirect(totalValueSize);
        return builder;
    }

    /**
     * Check if the binary tuple contains a null map.
     */
    public boolean hasNullMap() {
        return entryBase > BinaryTupleSchema.HEADER_SIZE;
    }

    /**
     * Append a NULL value for the current element.
     *
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendNull() {
        if (!hasNullMap()) {
            throw new IgniteInternalException("Appending a NULL value in binary tuple builder with disabled NULLs");
        }

        hasNullValues = true;

        int nullIndex = BinaryTupleSchema.HEADER_SIZE + elementIndex / 8;
        byte nullMask = (byte) (1 << (elementIndex % 8));
        buffer.put(nullIndex, (byte) (buffer.get(nullIndex) | nullMask));

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
        if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
            if (value != 0) {
                putByte((byte) value);
            }
        } else {
            putShort(value);
        }
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
        if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
            if (value != 0) {
                putByte((byte) value);
            }
        } else {
            if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
                putShort((short) value);
            } else {
                putInt(value);
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
    public BinaryTupleBuilder appendInt(Integer value) {
        return value == null ? appendNull() : appendInt(value.intValue());
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendLong(long value) {
        if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
            if (value != 0) {
                putByte((byte) value);
            }
        } else {
            if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
                putShort((short) value);
            } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                putInt((int) value);
            } else {
                putLong(value);
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
            if (value != 0.0) {
                putFloat((float) value);
            }
        } else {
            putDouble(value);
        }
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
    public BinaryTupleBuilder appendNumberNotNull(@NotNull BigInteger value) {
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
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDecimalNotNull(@NotNull BigDecimal value) {
        putBytes(value.unscaledValue().toByteArray());
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendDecimal(BigDecimal value) {
        return value == null ? appendNull() : appendDecimalNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendStringNotNull(@NotNull String value) {
        try {
            putString(value);
        } catch (CharacterCodingException e) {
            throw new IgniteInternalException("Failed to encode string in binary tuple builder", e);
        }
        return proceed();
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendString(String value) {
        return value == null ? appendNull() : appendStringNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBytesNotNull(@NotNull byte[] value) {
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
    public BinaryTupleBuilder appendUuidNotNull(@NotNull UUID value) {
        long lsb = value.getLeastSignificantBits();
        long msb = value.getMostSignificantBits();
        if ((lsb | msb) != 0L) {
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
    public BinaryTupleBuilder appendUuid(UUID value) {
        return value == null ? appendNull() : appendUuidNotNull(value);
    }

    /**
     * Append a value for the current element.
     *
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendBitmaskNotNull(@NotNull BitSet value) {
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
    public BinaryTupleBuilder appendDateNotNull(@NotNull LocalDate value) {
        if (value != BinaryTupleSchema.DEFAULT_DATE) {
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
    public BinaryTupleBuilder appendTimeNotNull(@NotNull LocalTime value) {
        if (value != BinaryTupleSchema.DEFAULT_TIME) {
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
    public BinaryTupleBuilder appendDateTimeNotNull(@NotNull LocalDateTime value) {
        if (value != BinaryTupleSchema.DEFAULT_DATE_TIME) {
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
    public BinaryTupleBuilder appendTimestampNotNull(@NotNull Instant value) {
        if (value != BinaryTupleSchema.DEFAULT_TIMESTAMP) {
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
     * @param schema Tuple schema.
     * @param value Element value.
     * @return {@code this} for chaining.
     */
    public BinaryTupleBuilder appendValue(BinaryTupleSchema schema, Object value) {
        var element = schema.element(elementIndex);

        if (value == null) {
            if (!element.nullable) {
                throw new SchemaMismatchException("NULL value for non-nullable column in binary tuple builder.");
            }

            appendNull();
            return this;
        }

        switch (element.typeSpec) {
            case INT8:
                appendByte((byte) value);
                break;

            case INT16:
                appendShort((short) value);
                break;

            case INT32:
                appendInt((int) value);
                break;

            case INT64:
                appendLong((long) value);
                break;

            case FLOAT:
                appendFloat((float) value);
                break;

            case DOUBLE:
                appendDouble((double) value);
                break;

            case NUMBER:
                appendNumberNotNull((BigInteger) value);
                break;

            case DECIMAL:
                appendDecimalNotNull((BigDecimal) value);
                break;

            case UUID:
                appendUuidNotNull((UUID) value);
                break;

            case BYTES:
                appendBytesNotNull((byte[]) value);
                break;

            case STRING:
                appendStringNotNull((String) value);
                break;

            case BITMASK:
                appendBitmaskNotNull((BitSet) value);
                break;

            case DATE:
                appendDateNotNull((LocalDate) value);
                break;

            case TIME:
                appendTimeNotNull((LocalTime) value);
                break;

            case DATETIME:
                appendDateTimeNotNull((LocalDateTime) value);
                break;

            case TIMESTAMP:
                appendTimestampNotNull((Instant) value);
                break;

            default:
                throw new InvalidTypeException("Unexpected value: " + element.typeSpec);
        }

        return this;
    }

    /**
     * Finalize tuple building.
     *
     * NOTE: This should be called only once as it messes up with accumulated internal data.
     *
     * @return Buffer with tuple bytes.
     */
    public ByteBuffer build() {
        int offset = 0;

        int valueSize = buffer.limit() - valueBase;
        byte flags = BinaryTupleSchema.valueSizeToFlags(valueSize);
        int desiredEntrySize = BinaryTupleSchema.flagsToEntrySize(flags);

        // Shrink the offset table if needed.
        if (desiredEntrySize != entrySize) {
            if (desiredEntrySize > entrySize) {
                throw new IgniteInternalException("Offset entry overflow in binary tuple builder");
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

            offset = (desiredEntrySize - entrySize) * numElements;
        }

        // Drop or move null map if needed.
        if (hasNullMap()) {
            if (!hasNullValues) {
                offset += BinaryTupleSchema.nullMapSize(numElements);
            } else {
                flags |= BinaryTupleSchema.NULLMAP_FLAG;
                if (offset != 0) {
                    int n = BinaryTupleSchema.nullMapSize(numElements);
                    for (int i = BinaryTupleSchema.HEADER_SIZE + n - 1; i >= BinaryTupleSchema.HEADER_SIZE; i--) {
                        buffer.put(i + offset, buffer.get(i));
                    }
                }
            }
        }

        buffer.put(offset, flags);

        return buffer.position(offset).slice();
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

    /** Put bytes to the buffer extending it if needed. */
    private void putBytes(byte[] bytes, int offset, int length) {
        assert bytes.length <= (offset + length);
        ensure(length);
        buffer.put(bytes, offset, length);
    }

    /** Put bytes to the buffer extending it if needed. */
    private void putBytes(ByteBuffer bytes) {
        ensure(bytes.remaining());
        buffer.put(bytes);
    }

    /** Put bytes to the buffer extending it if needed. */
    private void putBytes(ByteBuffer bytes, int offset, int length) {
        assert bytes.limit() <= (offset + length);
        ensure(length);
        buffer.put(bytes.asReadOnlyBuffer().position(offset).limit(offset + length));
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

    /** Allocate a direct buffer for tuple. */
    private void allocateDirect(int totalValueSize) {
        buffer = ByteBuffer.allocateDirect(estimateBufferCapacity(totalValueSize));
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
                throw new IgniteInternalException("Buffer overflow in binary tuple builder");
            }
        } while ((capacity - buffer.position()) < size);

        ByteBuffer newBuffer;
        if (buffer.isDirect()) {
            newBuffer = ByteBuffer.allocateDirect(capacity);
        } else {
            newBuffer = ByteBuffer.allocate(capacity);
        }
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
