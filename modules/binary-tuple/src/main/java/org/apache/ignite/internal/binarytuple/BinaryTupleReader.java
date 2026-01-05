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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.lang.InternalTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Utility for access to binary tuple elements as typed values.
 */
public class BinaryTupleReader extends BinaryTupleParser implements BinaryTupleParser.Sink, InternalTuple {
    /** Start offset of the current element. */
    private int begin = 0;

    /** End offset of the current element. */
    private int end = 0;

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
     * Constructor with a specific factory function for creating a `ByteBufferAccessor`.
     *
     * @param numElements Number of tuple elements.
     * @param buffer Buffer with a binary tuple.
     * @param byteBufferAccessorFactory A factory function to create a `ByteBufferAccessor` for accessing the buffer.
     */
    public BinaryTupleReader(int numElements, ByteBuffer buffer, Function<ByteBuffer, ByteBufferAccessor> byteBufferAccessorFactory) {
        super(numElements, buffer, byteBufferAccessorFactory);
    }

    /** {@inheritDoc} */
    @Override
    public final void nextElement(int index, int begin, int end) {
        this.begin = begin;
        this.end = end;
    }

    /**
     * Checks whether the given element contains a null value.
     *
     * @param index Element index.
     * @return {@code true} if this element contains a null value, {@code false} otherwise.
     */
    @Override
    public boolean hasNullValue(int index) {
        seek(index);
        return begin == end;
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public boolean booleanValue(int index) {
        seek(index);
        return booleanValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Boolean booleanValueBoxed(int index) {
        seek(index);

        if (begin == end) {
            return null;
        }

        return booleanValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public byte byteValue(int index) {
        seek(index);
        return byteValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Byte byteValueBoxed(int index) {
        seek(index);
        return begin == end ? null : byteValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public short shortValue(int index) {
        seek(index);
        return shortValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Short shortValueBoxed(int index) {
        seek(index);
        return begin == end ? null : shortValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public int intValue(int index) {
        seek(index);
        return intValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Integer intValueBoxed(int index) {
        seek(index);
        return begin == end ? null : intValue(byteBufferAccessor, begin,  end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public long longValue(int index) {
        seek(index);
        return longValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Long longValueBoxed(int index) {
        seek(index);
        return begin == end ? null : longValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public float floatValue(int index) {
        seek(index);
        return floatValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Float floatValueBoxed(int index) {
        seek(index);
        return begin == end ? null : floatValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public double doubleValue(int index) {
        seek(index);
        return doubleValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Double doubleValueBoxed(int index) {
        seek(index);
        return begin == end ? null : doubleValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @param scale Decimal scale. If equal to {@link Integer#MIN_VALUE}, then the value will be returned with whatever scale it is
     *         stored in.
     * @return Element value.
     */
    @Override
    public @Nullable BigDecimal decimalValue(int index, int scale) {
        seek(index);
        if (begin == end) {
            return null;
        }

        short valScale = shortValue(byteBufferAccessor, begin, begin + 2);

        BigDecimal decimalValue = new BigDecimal(numberValue(begin + 2, end), valScale);

        return scale < 0 ? decimalValue : decimalValue.setScale(scale, RoundingMode.UNNECESSARY);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable String stringValue(int index) {
        seek(index);
        return begin == end ? null : stringValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public byte @Nullable [] bytesValue(int index) {
        seek(index);
        return begin == end ? null : bytesValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value as a {@link ByteBuffer}.
     */
    public @Nullable ByteBuffer bytesValueAsBuffer(int index) {
        seek(index);
        return begin == end ? null : bytesValueAsBuffer(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable UUID uuidValue(int index) {
        seek(index);
        return begin == end ? null : uuidValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable LocalDate dateValue(int index) {
        seek(index);
        return begin == end ? null : dateValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable LocalTime timeValue(int index) {
        seek(index);
        return begin == end ? null : timeValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable LocalDateTime dateTimeValue(int index) {
        seek(index);
        return begin == end ? null : dateTimeValue(byteBufferAccessor, begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Instant timestampValue(int index) {
        seek(index);
        return begin == end ? null : timestampValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Duration durationValue(int index) {
        seek(index);
        return begin == end ? null : durationValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    @Override
    public @Nullable Period periodValue(int index) {
        seek(index);
        return begin == end ? null : periodValue(begin, end);
    }

    /**
     * Gets the beginning of the current element.
     *
     * @return The beginning of the current element.
     */
    public int begin() {
        return begin;
    }

    /**
     * Gets the end of the current element.
     *
     * @return The end of the current element.
     */
    public int end() {
        return end;
    }

    /**
     * Locate the specified tuple element.
     *
     * @param index Element index.
     */
    public void seek(int index) {
        fetch(index, this);
    }

    /**
     * Copies raw value of the specified element to the builder.
     *
     * @param builder Builder to copy value to.
     * @param index Index of the element.
     */
    protected void copyRawValue(BinaryTupleBuilder builder, int index) {
        seek(index);
        if (begin == end) {
            builder.appendNull();
        } else {
            builder.appendElementBytes(buffer, begin, end - begin);
        }
    }
}
