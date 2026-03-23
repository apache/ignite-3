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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.util.ByteUtils;

/**
 * Binary tuple parser allows to get bytes of individual elements from entirety of tuple bytes.
 */
public class BinaryTupleParser {
    /**
     * Receiver of parsed data.
     */
    public interface Sink {
        /**
         * Provides the location of the next tuple element.
         *
         * @param index Element index.
         * @param begin Start offset of the element, 0 for NULL.
         * @param end End offset of the element, 0 for NULL.
         */
        void nextElement(int index, int begin, int end);
    }

    /** Byte order of ByteBuffers that contain the tuple. */
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    /** UUID size in bytes. */
    private static final int UUID_SIZE = 16;

    /** Number of elements in the tuple. */
    private final int numElements;

    /** Size of an offset table entry. */
    private final int entrySize;

    /** Position of the varlen offset table. */
    private final int entryBase;

    /** Starting position of variable-length values. */
    private final int valueBase;

    /** ByteBuffer accessor for reading data from the underlying buffer. */
    protected final ByteBufferAccessor byteBufferAccessor;

    /** Reader for reading offsets from offset table in the buffer. */
    private final OffsetTableReader offsetTableReader;

    /** Binary tuple. */
    protected final ByteBuffer buffer;

    /**
     * This constructor uses a default `PlainByteBufferAccessor` for accessing the buffer.
     *
     * @param numElements The number of elements in the binary tuple.
     * @param buffer The `ByteBuffer` containing the binary tuple data.
     */
    public BinaryTupleParser(int numElements, ByteBuffer buffer) {
        this(numElements, buffer, PlainByteBufferAccessor::new);
    }

    /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTupleParser(int numElements, ByteBuffer buffer, Function<ByteBuffer, ByteBufferAccessor> byteBufferAccessorFactory) {
        this.numElements = numElements;

        assert buffer.order() == ORDER : "Buffer order must be LITTLE_ENDIAN, actual: " + buffer.order();
        assert buffer.position() == 0 : "Buffer position must be 0, actual: " + buffer.position();
        this.buffer = buffer;
        byteBufferAccessor = byteBufferAccessorFactory.apply(buffer);

        byte flags = byteBufferAccessor.get(0);

        entryBase = BinaryTupleCommon.HEADER_SIZE;
        entrySize = 1 << (flags & BinaryTupleCommon.VARSIZE_MASK);
        valueBase = entryBase + entrySize * numElements;

        offsetTableReader = OffsetTableReader.fromEntrySize(entrySize);
    }

    /**
     * Returns the binary tuple size in bytes.
     */
    public int size() {
        return valueBase + getOffset(valueBase - entrySize);
    }

    /**
     * Returns the number of elements in the tuple.
     */
    public int elementCount() {
        return numElements;
    }

    /**
     * Returns the content of this tuple as a byte buffer.
     */
    public ByteBuffer byteBuffer() {
        return buffer.slice().order(ORDER);
    }

    /**
     * Returns the byte buffer accessor associated with this parser.
     *
     * @return The original ByteBufferAccessor object.
     */
    public ByteBufferAccessor accessor() {
        return byteBufferAccessor;
    }

    /**
     * Locate the specified tuple element.
     *
     * @param index Index of the element.
     * @param sink Receiver.
     */
    public void fetch(int index, Sink sink) {
        assert index >= 0;
        assert index < numElements : "Index out of bounds: " + index + " >= " + numElements;

        int entry = entryBase + index * entrySize;

        int offset = valueBase;
        if (index > 0) {
            offset += getOffset(entry - entrySize);
        }

        int nextOffset = valueBase + getOffset(entry);
        if (nextOffset < offset) {
            throw new BinaryTupleFormatException("Corrupted offset table");
        }

        sink.nextElement(index, offset, nextOffset);
    }

    /**
     * Evaluates a read possibility for the specific element.
     *
     * @param index Index of the element.
     * @return Readability.
     */
    public Readability valueReadability(int index) {
        assert index >= 0;
        assert index < numElements : "Index out of bounds: " + index + " >= " + numElements;

        int entry = entryBase + index * entrySize;

        if (entry >= buffer.capacity()) {
            return Readability.NOT_READABLE;
        }

        int offset = valueBase;

        if (index > 0) {
            offset += getOffset(entry - entrySize);
        }

        int nextOffset = valueBase + getOffset(entry);

        if (offset == nextOffset) {
            return Readability.READABLE;
        }

        if (offset >= buffer.capacity()) {
            return Readability.NOT_READABLE;
        }

        if (nextOffset > buffer.capacity()) {
            return Readability.PARTIAL_READABLE;
        }

        return Readability.READABLE;
    }

    /**
     * The class is used to represent a read possibility.
     */
    public enum Readability {
        /** The element is unavailable to read. */
        NOT_READABLE,

        /** The element is fully available. */
        READABLE,

        /** Only part of the element is available to read. */
        PARTIAL_READABLE
    }

    /**
     * Feeds the receiver with all tuple elements.
     *
     * @param sink Receiver.
     */
    public void parse(Sink sink) {
        int entry = entryBase;
        int offset = valueBase;

        for (int i = 0; i < numElements; i++) {
            int nextOffset = valueBase + getOffset(entry);
            if (nextOffset < offset) {
                throw new BinaryTupleFormatException("Corrupted offset table");
            }

            sink.nextElement(i, offset, nextOffset);
            offset = nextOffset;

            entry += entrySize;
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static boolean booleanValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;

        if (len == Byte.BYTES) {
            return ByteUtils.byteToBoolean(byteBufferAccessor.get(begin));
        }

        throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static byte byteValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        switch (len) {
            case Byte.BYTES:
                return byteBufferAccessor.get(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static short shortValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        switch (len) {
            case Byte.BYTES:
                return byteBufferAccessor.get(begin);
            case Short.BYTES:
                return byteBufferAccessor.getShort(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static int intValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        switch (len) {
            case Byte.BYTES:
                return byteBufferAccessor.get(begin);
            case Short.BYTES:
                return byteBufferAccessor.getShort(begin);
            case Integer.BYTES:
                return byteBufferAccessor.getInt(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static long longValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        switch (len) {
            case Byte.BYTES:
                return byteBufferAccessor.get(begin);
            case Short.BYTES:
                return byteBufferAccessor.getShort(begin);
            case Integer.BYTES:
                return byteBufferAccessor.getInt(begin);
            case Long.BYTES:
                return byteBufferAccessor.getLong(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static float floatValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        switch (len) {
            case Float.BYTES:
                return byteBufferAccessor.getFloat(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static double doubleValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        switch (len) {
            case Float.BYTES:
                return byteBufferAccessor.getFloat(begin);
            case Double.BYTES:
                return byteBufferAccessor.getDouble(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public BigInteger numberValue(int begin, int end) {
        int len = end - begin;
        if (len <= 0) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }

        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
            begin += buffer.arrayOffset();
        } else {
            bytes = getBytes(begin, end);
            begin = 0;
        }
        return new BigInteger(bytes, begin, len);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final String stringValue(int begin, int end) {
        int len = end - begin;
        if (len <= 0) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }

        if (byteBufferAccessor.get(begin) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin++;
            len--;
        }

        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
            begin += buffer.arrayOffset();
        } else {
            bytes = getBytes(begin, end);
            begin = 0;
        }

        return new String(bytes, begin, len, StandardCharsets.UTF_8);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final byte[] bytesValue(int begin, int end) {
        int len = end - begin;
        if (len <= 0) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }

        if (byteBufferAccessor.get(begin) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin++;
        }

        return getBytes(begin, end);
    }

    /**
     * Reads value of specified element as a ByteBuffer.
     * The returned buffer is a slice of the original buffer.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final ByteBuffer bytesValueAsBuffer(int begin, int end) {
        int len = end - begin;
        if (len <= 0) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }

        if (byteBufferAccessor.get(begin) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin++;
        }

        return buffer.duplicate().position(begin).limit(end).slice();
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final UUID uuidValue(int begin, int end) {
        int len = end - begin;
        if (len != UUID_SIZE) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
        long msb = byteBufferAccessor.getLong(begin);
        long lsb = byteBufferAccessor.getLong(begin + 8);
        return new UUID(msb, lsb);
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static LocalDate dateValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        if (len != 3) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
        return getDate(byteBufferAccessor, begin);
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static LocalTime timeValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        if (len < 4 || len > 6) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
        return getTime(byteBufferAccessor, begin, len);
    }

    /**
     * Reads value of specified element.
     *
     * @param byteBufferAccessor Accessor for binary tuple bytes.
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public static LocalDateTime dateTimeValue(ByteBufferAccessor byteBufferAccessor, int begin, int end) {
        int len = end - begin;
        if (len < 7 || len > 9) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
        return LocalDateTime.of(getDate(byteBufferAccessor, begin), getTime(byteBufferAccessor, begin + 3, len - 3));
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final Instant timestampValue(int begin, int end) {
        int len = end - begin;
        if (len != 8 && len != 12) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
        long seconds = byteBufferAccessor.getLong(begin);
        int nanos = len == 8 ? 0 : byteBufferAccessor.getInt(begin + 8);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final Duration durationValue(int begin, int end) {
        int len = end - begin;
        if (len != 8 && len != 12) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }

        long seconds = byteBufferAccessor.getLong(begin);
        int nanos = len == 8 ? 0 : byteBufferAccessor.getInt(begin + 8);

        return Duration.ofSeconds(seconds, nanos);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    public final Period periodValue(int begin, int end) {
        int len = end - begin;
        switch (len) {
            case 3:
                return Period.of(byteBufferAccessor.get(begin), byteBufferAccessor.get(begin + 1), byteBufferAccessor.get(begin + 2));
            case 6:
                return Period.of(
                        byteBufferAccessor.getShort(begin),
                        byteBufferAccessor.getShort(begin + 2),
                        byteBufferAccessor.getShort(begin + 4)
                );
            case 12:
                return Period.of(
                        byteBufferAccessor.getInt(begin),
                        byteBufferAccessor.getInt(begin + 4),
                        byteBufferAccessor.getInt(begin + 8)
                );
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }
    }

    /**
     * Gets an entry from the value offset table.
     *
     * @param index Byte index of the table entry.
     * @return Entry value.
     */
    private int getOffset(int index) {
        return offsetTableReader.read(byteBufferAccessor, index);
    }

    /**
     * Gets array of bytes from a given range in the buffer.
     */
    private byte[] getBytes(int begin, int end) {
        byte[] bytes = new byte[end - begin];
        buffer.duplicate().position(begin).limit(end).get(bytes);
        return bytes;
    }

    /**
     * Decodes a Date element.
     */
    private static LocalDate getDate(ByteBufferAccessor byteBufferAccessor, int offset) {
        int date = Short.toUnsignedInt(byteBufferAccessor.getShort(offset));
        date |= byteBufferAccessor.get(offset + 2) << 16;

        int day = date & 31;
        int month = (date >> 5) & 15;
        int year = (date >> 9); // Sign matters.

        return LocalDate.of(year, month, day);
    }

    /**
     * Decodes a Time element.
     */
    private static LocalTime getTime(ByteBufferAccessor byteBufferAccessor, int offset, int length) {
        long time = Integer.toUnsignedLong(byteBufferAccessor.getInt(offset));

        int nanos;
        switch (length) {
            case 4:
                nanos = ((int) time & ((1 << 10) - 1)) * 1000 * 1000;
                time >>>= 10;
                break;
            case 5:
                time |= Byte.toUnsignedLong(byteBufferAccessor.get(offset + 4)) << 32;
                nanos = ((int) time & ((1 << 20) - 1)) * 1000;
                time >>>= 20;
                break;
            case 6:
                time |= Short.toUnsignedLong(byteBufferAccessor.getShort(offset + 4)) << 32;
                nanos = ((int) time & ((1 << 30) - 1));
                time >>>= 30;
                break;
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element: " + length);
        }

        int second = ((int) time) & 63;
        int minute = ((int) time >>> 6) & 63;
        int hour = ((int) time >>> 12) & 31;

        return LocalTime.of(hour, minute, second, nanos);
    }

    /**
     * A plain implementation of the `ByteBufferAccessor` interface.
     * This class provides methods to access various data types from a `ByteBuffer`.
     */
    private static class PlainByteBufferAccessor implements ByteBufferAccessor {
        private final ByteBuffer buffer;

        PlainByteBufferAccessor(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte get(int index) {
            return buffer.get(index);
        }

        @Override
        public short getShort(int index) {
            return buffer.getShort(index);
        }

        @Override
        public int getInt(int index) {
            return buffer.getInt(index);
        }

        @Override
        public long getLong(int index) {
            return buffer.getLong(index);
        }

        @Override
        public float getFloat(int index) {
            return buffer.getFloat(index);
        }

        @Override
        public double getDouble(int index) {
            return buffer.getDouble(index);
        }

        @Override
        public int capacity() {
            return buffer.capacity();
        }
    }

    private enum OffsetTableReader {
        BYTE_ENTRY {
            @Override
            int read(ByteBufferAccessor bufferAccessor, int index) {
                return Byte.toUnsignedInt(bufferAccessor.get(index));
            }
        },

        SHORT_ENTRY {
            @Override
            int read(ByteBufferAccessor bufferAccessor, int index) {
                return Short.toUnsignedInt(bufferAccessor.getShort(index));
            }
        },

        INTEGER_ENTRY {
            @Override
            int read(ByteBufferAccessor bufferAccessor, int index) {
                int offset = bufferAccessor.getInt(index);
                if (offset < 0) {
                    throw new BinaryTupleFormatException("Unsupported offset table size");
                }
                return offset;
            }
        };

        static OffsetTableReader fromEntrySize(int size) {
            switch (size) {
                case Byte.BYTES:
                    return BYTE_ENTRY;
                case Short.BYTES:
                    return SHORT_ENTRY;
                case Integer.BYTES: {
                    return INTEGER_ENTRY;
                }
                case Long.BYTES:
                    throw new BinaryTupleFormatException("Unsupported offset table size");
                default:
                    throw new BinaryTupleFormatException("Invalid offset table size");
            }
        }

        abstract int read(ByteBufferAccessor bufferAccessor, int index);
    } 
}
