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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

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

    /** Number of elements in the tuple. */
    private final int numElements;

    /** Size of an offset table entry. */
    private final int entrySize;

    /** Position of the varlen offset table. */
    private final int entryBase;

    /** Starting position of variable-length values. */
    private final int valueBase;

    /** Binary tuple. */
    protected final ByteBuffer buffer;

    /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTupleParser(int numElements, ByteBuffer buffer) {
        this.numElements = numElements;

        assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer.position() == 0;
        this.buffer = buffer;

        byte flags = buffer.get(0);

        int base = BinaryTupleSchema.HEADER_SIZE;
        if ((flags & BinaryTupleSchema.NULLMAP_FLAG) != 0) {
            base += BinaryTupleSchema.nullMapSize(numElements);
        }

        entryBase = base;
        entrySize = 1 << (flags & BinaryTupleSchema.VARSIZE_MASK);
        valueBase = base + entrySize * numElements;
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
     * Check if the binary tuple contains a null map.
     */
    public boolean hasNullMap() {
        return entryBase > BinaryTupleSchema.HEADER_SIZE;
    }

    /**
     * Locate the specified tuple element.
     *
     * @param index Index of the element.
     * @param sink Receiver.
     */
    public void fetch(int index, Sink sink) {
        assert index >= 0;
        assert index < numElements;

        int entry = entryBase + index * entrySize;

        int offset = valueBase;
        if (index > 0) {
            offset += getOffset(entry - entrySize);
        }

        int nextOffset = valueBase + getOffset(entry);
        if (nextOffset < offset) {
            throw new BinaryTupleFormatException("Corrupted offset table");
        }

        if (offset == nextOffset && hasNullMap()) {
            int nullIndex = BinaryTupleSchema.nullOffset(index);
            byte nullMask = BinaryTupleSchema.nullMask(index);
            if ((buffer.get(nullIndex) & nullMask) != 0) {
                sink.nextElement(index, 0, 0);
                return;
            }
        }

        sink.nextElement(index, offset, nextOffset);
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

            if (offset == nextOffset && hasNullMap()) {
                int nullIndex = BinaryTupleSchema.nullOffset(i);
                byte nullMask = BinaryTupleSchema.nullMask(i);
                if ((buffer.get(nullIndex) & nullMask) != 0) {
                    sink.nextElement(i, 0, 0);
                    entry += entrySize;
                    continue;
                }
            }

            sink.nextElement(i, offset, nextOffset);
            offset = nextOffset;
            entry += entrySize;
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final byte byteValue(int begin, int end) {
        switch (end - begin) {
            case 0:
                return 0;
            case Byte.BYTES:
                return buffer.get(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final short shortValue(int begin, int end) {
        switch (end - begin) {
            case 0:
                return 0;
            case Byte.BYTES:
                return buffer.get(begin);
            case Short.BYTES:
                return buffer.getShort(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final int intValue(int begin, int end) {
        switch (end - begin) {
            case 0:
                return 0;
            case Byte.BYTES:
                return buffer.get(begin);
            case Short.BYTES:
                return buffer.getShort(begin);
            case Integer.BYTES:
                return buffer.getInt(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final long longValue(int begin, int end) {
        switch (end - begin) {
            case 0:
                return 0;
            case Byte.BYTES:
                return buffer.get(begin);
            case Short.BYTES:
                return buffer.getShort(begin);
            case Integer.BYTES:
                return buffer.getInt(begin);
            case Long.BYTES:
                return buffer.getLong(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final float floatValue(int begin, int end) {
        switch (end - begin) {
            case 0:
                return 0.0F;
            case Float.BYTES:
                return buffer.getFloat(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final double doubleValue(int begin, int end) {
        switch (end - begin) {
            case 0:
                return 0.0;
            case Float.BYTES:
                return buffer.getFloat(begin);
            case Double.BYTES:
                return buffer.getDouble(begin);
            default:
                throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final BigInteger numberValue(int begin, int end) {
        byte[] bytes;
        int len = end - begin;
        if (buffer.hasArray()) {
            bytes = buffer.array();
            begin += buffer.arrayOffset();
        } else {
            bytes = bytesValue(begin, end);
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
    final String stringValue(int begin, int end) {
        byte[] bytes;
        int len = end - begin;
        if (buffer.hasArray()) {
            bytes = buffer.array();
            begin += buffer.arrayOffset();
        } else {
            bytes = bytesValue(begin, end);
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
    final byte[] bytesValue(int begin, int end) {
        byte[] bytes = new byte[end - begin];
        buffer.duplicate().position(begin).limit(end).get(bytes);
        return bytes;
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final UUID uuidValue(int begin, int end) {
        int len = end - begin;
        if (len != NativeTypes.UUID.sizeInBytes()) {
            if (len == 0) {
                return BinaryTupleSchema.DEFAULT_UUID;
            }
            throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
        long lsb = buffer.getLong(begin);
        long msb = buffer.getLong(begin + 8);
        return new UUID(msb, lsb);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final BitSet bitmaskValue(int begin, int end) {
        return BitSet.valueOf(buffer.duplicate().position(begin).limit(end));
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final LocalDate dateValue(int begin, int end) {
        int len = end - begin;
        if (len != 3) {
            if (len == 0) {
                return BinaryTupleSchema.DEFAULT_DATE;
            }
            throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
        return getDate(begin);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final LocalTime timeValue(int begin, int end) {
        int len = end - begin;
        if (len < 4 || len > 6) {
            if (len == 0) {
                return BinaryTupleSchema.DEFAULT_TIME;
            }
            throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
        return getTime(begin, len);
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final LocalDateTime dateTimeValue(int begin, int end) {
        int len = end - begin;
        if (len < 7 || len > 9) {
            if (len == 0) {
                return BinaryTupleSchema.DEFAULT_DATE_TIME;
            }
            throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
        return LocalDateTime.of(getDate(begin), getTime(begin + 3, len - 3));
    }

    /**
     * Reads value of specified element.
     *
     * @param begin Start offset of the element.
     * @param end End offset of the element.
     * @return Element value.
     */
    final Instant timestampValue(int begin, int end) {
        int len = end - begin;
        if (len != 8 && len != 12) {
            if (len == 0) {
                return BinaryTupleSchema.DEFAULT_TIMESTAMP;
            }
            throw new BinaryTupleFormatException("Invalid length for a tuple element");
        }
        long seconds = buffer.getLong(begin);
        int nanos = len == 8 ? 0 : buffer.getInt(begin + 8);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Get an entry from the value offset table.
     *
     * @param index Byte index of the table entry.
     * @return Entry value.
     */
    private int getOffset(int index) {
        switch (entrySize) {
            case Byte.BYTES:
                return Byte.toUnsignedInt(buffer.get(index));
            case Short.BYTES:
                return Short.toUnsignedInt(buffer.getShort(index));
            case Integer.BYTES: {
                int offset = buffer.getInt(index);
                if (offset < 0) {
                    throw new BinaryTupleFormatException("Unsupported offset table size");
                }
                return offset;
            }
            case Long.BYTES:
                throw new BinaryTupleFormatException("Unsupported offset table size");
            default:
                throw new BinaryTupleFormatException("Invalid offset table size");
        }
    }

    /**
     * Decode a non-trivial Date element.
     */
    private LocalDate getDate(int offset) {
        int date = Short.toUnsignedInt(buffer.getShort(offset));
        date |= ((int) buffer.get(offset + 2)) << 16;

        int day = date & 31;
        int month = (date >> 5) & 15;
        int year = (date >> 9); // Sign matters.

        return LocalDate.of(year, month, day);
    }

    /**
     * Decode a non-trivial Time element.
     */
    private LocalTime getTime(int offset, int length) {
        long time = Integer.toUnsignedLong(buffer.getInt(offset));

        int nanos;
        if (length == 4) {
            nanos = ((int) time & ((1 << 10) - 1)) * 1000 * 1000;
            time >>>= 10;
        } else if (length == 5) {
            time |= Byte.toUnsignedLong(buffer.get(offset + 4)) << 32;
            nanos = ((int) time & ((1 << 20) - 1)) * 1000;
            time >>>= 20;
        } else {
            time |= Short.toUnsignedLong(buffer.getShort(offset + 4)) << 32;
            nanos = ((int) time & ((1 << 30) - 1));
            time >>>= 30;
        }

        int second = ((int) time) & 63;
        int minute = ((int) time >>> 6) & 63;
        int hour = ((int) time >>> 12) & 31;

        return LocalTime.of(hour, minute, second, nanos);
    }
}
