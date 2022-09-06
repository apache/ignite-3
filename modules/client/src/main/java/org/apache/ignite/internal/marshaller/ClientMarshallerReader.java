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

package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

/**
 * Binary reader over {@link ClientMessageUnpacker}.
 */
public class ClientMarshallerReader implements MarshallerReader {
    /** Unpacker. */
    private final BinaryTupleReader unpacker;

    /** Index. */
    private int index;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     */
    public ClientMarshallerReader(BinaryTupleReader unpacker) {
        this.unpacker = unpacker;
    }

    /** {@inheritDoc} */
    @Override
    public void skipValue() {
        index++;
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() {
        return unpacker.byteValue(index++);
    }

    /** {@inheritDoc} */
    @Override
    public Byte readByteBoxed() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.byteValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() {
        return unpacker.shortValue(index++);
    }

    /** {@inheritDoc} */
    @Override
    public Short readShortBoxed() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.shortValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() {
        return unpacker.intValue(index++);
    }

    /** {@inheritDoc} */
    @Override
    public Integer readIntBoxed() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.intValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() {
        return unpacker.longValue(index++);
    }

    /** {@inheritDoc} */
    @Override
    public Long readLongBoxed() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.longValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() {
        return unpacker.floatValue(index++);
    }

    /** {@inheritDoc} */
    @Override
    public Float readFloatBoxed() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.floatValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() {
        return unpacker.doubleValue(index++);
    }

    /** {@inheritDoc} */
    @Override
    public Double readDoubleBoxed() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.doubleValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public String readString() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.stringValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public UUID readUuid() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.uuidValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] readBytes() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.bytesValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public BitSet readBitSet() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.bitmaskValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public BigInteger readBigInt() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.numberValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal readBigDecimal() {
        // TODO IGNITE-17632: Get scale from schema.
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.decimalValue(idx, 100);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate readDate() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.dateValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime readTime() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.timeValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public Instant readTimestamp() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.timestampValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime readDateTime() {
        var idx = index++;
        return unpacker.hasNullValue(idx) ? null : unpacker.dateTimeValue(idx);
    }
}
