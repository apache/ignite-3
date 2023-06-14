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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;

/**
 * Utility for access to binary tuple elements as typed values.
 */
public class BinaryTupleReader extends BinaryTupleParser implements BinaryTupleParser.Sink {
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
    public boolean hasNullValue(int index) {
        seek(index);
        return begin == 0;
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public byte byteValue(int index) {
        seek(index);
        return byteValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Byte byteValueBoxed(int index) {
        seek(index);
        return begin == 0 ? null : byteValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public short shortValue(int index) {
        seek(index);
        return shortValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Short shortValueBoxed(int index) {
        seek(index);
        return begin == 0 ? null : shortValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public int intValue(int index) {
        seek(index);
        return intValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Integer intValueBoxed(int index) {
        seek(index);
        return begin == 0 ? null : intValue(begin,  end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public long longValue(int index) {
        seek(index);
        return longValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Long longValueBoxed(int index) {
        seek(index);
        return begin == 0 ? null : longValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public float floatValue(int index) {
        seek(index);
        return floatValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Float floatValueBoxed(int index) {
        seek(index);
        return begin == 0 ? null : floatValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public double doubleValue(int index) {
        seek(index);
        return doubleValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Double doubleValueBoxed(int index) {
        seek(index);
        return begin == 0 ? null : doubleValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public BigInteger numberValue(int index) {
        seek(index);
        return begin == 0 ? null : numberValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @param scale Decimal scale.
     * @return Element value.
     */
    public BigDecimal decimalValue(int index, int scale) {
        seek(index);
        return begin == 0 ? null : new BigDecimal(numberValue(begin, end), scale);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public String stringValue(int index) {
        seek(index);
        return begin == 0 ? null : stringValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public byte[] bytesValue(int index) {
        seek(index);
        return begin == 0 ? null : bytesValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public UUID uuidValue(int index) {
        seek(index);
        return begin == 0 ? null : uuidValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public BitSet bitmaskValue(int index) {
        seek(index);
        return begin == 0 ? null : bitmaskValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public LocalDate dateValue(int index) {
        seek(index);
        return begin == 0 ? null : dateValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public LocalTime timeValue(int index) {
        seek(index);
        return begin == 0 ? null : timeValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public LocalDateTime dateTimeValue(int index) {
        seek(index);
        return begin == 0 ? null : dateTimeValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Instant timestampValue(int index) {
        seek(index);
        return begin == 0 ? null : timestampValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Duration durationValue(int index) {
        seek(index);
        return begin == 0 ? null : durationValue(begin, end);
    }

    /**
     * Reads value of specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Period periodValue(int index) {
        seek(index);
        return begin == 0 ? null : periodValue(begin, end);
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
}
