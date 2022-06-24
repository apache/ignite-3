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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

/**
 * Utility for access to binary tuple elements as typed values.
 */
public class BinaryTupleReader extends BinaryTupleParser {
    /** A helper to locate and handle tuple elements. */
    private class ElementSink implements BinaryTupleParser.Sink {
        int offset;
        int length;

        /** {@inheritDoc} */
        @Override
        public void nextElement(int index, int begin, int end) {
            offset = begin;
            length = end - begin;
        }

        boolean isNull() {
            return offset == 0;
        }

        byte asByte() {
            if (length == 0) {
                return 0;
            }
            assert length == Byte.BYTES;
            return buffer.get(offset);
        }

        short asShort() {
            if (length == 0) {
                return 0;
            }
            if (length == Byte.BYTES) {
                return buffer.get(offset);
            }
            assert length == Short.BYTES;
            return buffer.getShort(offset);
        }

        int asInt() {
            if (length == 0) {
                return 0;
            }
            if (length == Byte.BYTES) {
                return buffer.get(offset);
            }
            if (length == Short.BYTES) {
                return buffer.getShort(offset);
            }
            assert length == Integer.BYTES;
            return buffer.getInt(offset);
        }

        long asLong() {
            if (length == 0) {
                return 0;
            }
            if (length == Byte.BYTES) {
                return buffer.get(offset);
            }
            if (length == Short.BYTES) {
                return buffer.getShort(offset);
            }
            if (length == Integer.BYTES) {
                return buffer.getInt(offset);
            }
            assert length == Long.BYTES;
            return buffer.getLong(offset);
        }

        float asFloat() {
            if (length == 0) {
                return 0.0F;
            }
            assert length == Float.BYTES;
            return buffer.getFloat(offset);
        }

        double asDouble() {
            if (length == 0) {
                return 0.0;
            }
            if (length == Float.BYTES) {
                return buffer.getFloat(offset);
            }
            assert length == Double.BYTES;
            return buffer.getDouble(offset);
        }
    }

    /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param bytes Binary tuple.
     */
    public BinaryTupleReader(int numElements, byte[] bytes) {
        this(numElements, ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTupleReader(int numElements, ByteBuffer buffer) {
        super(numElements, buffer);
    }

    /**
     * Get underlying buffer.
     *
     * @return Buffer.
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Checks whether the given element contains a null value.
     *
     * @param index Element index.
     * @return {@code true} if this element contains a null value, {@code false} otherwise.
     */
    public boolean hasNullValue(int index) {
        return seek(index).isNull();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public byte byteValue(int index) {
        return seek(index).asByte();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Byte byteValueBoxed(int index) {
        var element = seek(index);
        return element.isNull() ? null : element.asByte();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public short shortValue(int index) {
        return seek(index).asShort();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Short shortValueBoxed(int index) {
        var element = seek(index);
        return element.isNull() ? null : element.asShort();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public int intValue(int index) {
        return seek(index).asInt();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Integer intValueBoxed(int index) {
        var element = seek(index);
        return element.isNull() ? null : element.asInt();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public long longValue(int index) {
        return seek(index).asLong();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Long longValueBoxed(int index) {
        var element = seek(index);
        return element.isNull() ? null : element.asLong();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public float floatValue(int index) {
        return seek(index).asFloat();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Float floatValueBoxed(int index) {
        var element = seek(index);
        return element.isNull() ? null : element.asFloat();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public double doubleValue(int index) {
        return seek(index).asDouble();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Double doubleValueBoxed(int index) {
        var element = seek(index);
        return element.isNull() ? null : element.asDouble();
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public BigInteger numberValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }

        byte[] bytes = new byte[element.length];
        buffer.duplicate().position(element.offset).limit(element.offset + element.length).get(bytes);
        return new BigInteger(bytes);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @param scale Decimal scale.
     * @return Element value.
     */
    public BigDecimal decimalValue(int index, int scale) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }

        byte[] bytes = new byte[element.length];
        buffer.duplicate().position(element.offset).limit(element.offset + element.length).get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public String stringValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }

        byte[] bytes;
        int offset;
        if (buffer.hasArray()) {
            bytes = buffer.array();
            offset = element.offset + buffer.arrayOffset();
        } else {
            bytes = new byte[element.length];
            buffer.duplicate().position(element.offset).limit(element.offset + element.length).get(bytes);
            offset = 0;
        }

        return new String(bytes, offset, element.length, StandardCharsets.UTF_8);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public byte[] bytesValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }

        byte[] bytes = new byte[element.length];
        buffer.duplicate().position(element.offset).limit(element.offset + element.length).get(bytes);
        return bytes;
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public UUID uuidValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }
        if (element.length == 0) {
            return BinaryTupleSchema.DEFAULT_UUID;
        }

        assert element.length == NativeTypes.UUID.sizeInBytes();

        long lsb = buffer.getLong(element.offset);
        long msb = buffer.getLong(element.offset + 8);
        return new UUID(msb, lsb);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public BitSet bitmaskValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }
        return BitSet.valueOf(buffer.duplicate().position(element.offset).limit(element.offset + element.length));
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public LocalDate dateValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }
        if (element.length == 0) {
            return BinaryTupleSchema.DEFAULT_DATE;
        }

        assert element.length == 3;
        return getDate(element);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public LocalTime timeValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }
        if (element.length == 0) {
            return BinaryTupleSchema.DEFAULT_TIME;
        }

        assert element.length >= 4;
        assert element.length <= 6;
        return getTime(element);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public LocalDateTime dateTimeValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }
        if (element.length == 0) {
            return BinaryTupleSchema.DEFAULT_DATE_TIME;
        }

        assert element.length >= 7;
        assert element.length <= 9;
        LocalDate date = getDate(element);
        element.offset += 3;
        element.length -= 3;
        return LocalDateTime.of(date, getTime(element));
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Instant timestampValue(int index) {
        var element = seek(index);
        if (element.isNull()) {
            return null;
        }
        if (element.length == 0) {
            return BinaryTupleSchema.DEFAULT_TIMESTAMP;
        }

        assert element.length == 8 || element.length == 12;
        long seconds = buffer.getLong(element.offset);
        int nanos = element.length == 8 ? 0 : buffer.getInt(element.offset + 8);

        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Locate the specified tuple element.
     *
     * @param index Element index.
     * @return Element location info.
     */
    private ElementSink seek(int index) {
        ElementSink element = new ElementSink();
        fetch(index, element);
        return element;
    }

    /**
     * Decode a non-trivial Date element.
     */
    private LocalDate getDate(ElementSink element) {
        int date = Short.toUnsignedInt(buffer.getShort(element.offset));
        date |= ((int) buffer.get(element.offset)) << 16;

        int day = date & 31;
        int month = (date >> 5) & 15;
        int year = (date >> 9); // Sign matters.

        return LocalDate.of(year, month, day);
    }

    /**
     * Decode a non-trivial Time element.
     */
    private LocalTime getTime(ElementSink element) {
        long time = Integer.toUnsignedLong(buffer.getInt(element.offset));

        int nanos;
        if (element.length == 4) {
            nanos = ((int) time & ((1 << 10) - 1)) * 1000 * 1000;
            time >>>= 10;
        } else if (element.length == 5) {
            time |= Byte.toUnsignedLong(buffer.get(element.offset + 4)) << 32;
            nanos = ((int) time & ((1 << 20) - 1)) * 1000;
            time >>>= 20;
        } else {
            time |= Short.toUnsignedLong(buffer.getShort(element.offset + 4)) << 32;
            nanos = ((int) time & ((1 << 30) - 1));
            time >>>= 30;
        }

        int second = ((int) time) & 63;
        int minute = ((int) time >>> 6) & 63;
        int hour = ((int) time >>> 12) & 31;

        return LocalTime.of(hour, minute, second, nanos);
    }
}
