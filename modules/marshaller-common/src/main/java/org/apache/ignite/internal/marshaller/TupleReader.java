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

package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.table.Tuple;

/**
 * Adapter from a {@link Tuple} to a {@link MarshallerReader}.
 */
public class TupleReader implements MarshallerReader {
    private final Tuple tuple;

    private int index;

    public TupleReader(Tuple tuple) {
        this(tuple, 0);
    }

    TupleReader(Tuple tuple, int index) {
        this.tuple = tuple;
        this.index = index;
    }

    @Override
    public void skipValue() {
        index++;
    }

    @Override
    public boolean readBoolean() {
        return tuple.booleanValue(index++);
    }

    @Override
    public Boolean readBooleanBoxed() {
        return tuple.value(index++);
    }

    @Override
    public byte readByte() {
        return tuple.byteValue(index++);
    }

    @Override
    public Byte readByteBoxed() {
        return tuple.value(index++);
    }

    @Override
    public short readShort() {
        return tuple.shortValue(index++);
    }

    @Override
    public Short readShortBoxed() {
        return tuple.value(index++);
    }

    @Override
    public int readInt() {
        return tuple.intValue(index++);
    }

    @Override
    public Integer readIntBoxed() {
        return tuple.value(index++);
    }

    @Override
    public long readLong() {
        return tuple.longValue(index++);
    }

    @Override
    public Long readLongBoxed() {
        return tuple.value(index++);
    }

    @Override
    public float readFloat() {
        return tuple.floatValue(index++);
    }

    @Override
    public Float readFloatBoxed() {
        return tuple.value(index++);
    }

    @Override
    public double readDouble() {
        return tuple.doubleValue(index++);
    }

    @Override
    public Double readDoubleBoxed() {
        return tuple.value(index++);
    }

    @Override
    public String readString() {
        return tuple.stringValue(index++);
    }

    @Override
    public UUID readUuid() {
        return tuple.uuidValue(index++);
    }

    @Override
    public byte[] readBytes() {
        return tuple.value(index++);
    }

    @Override
    public BitSet readBitSet() {
        return tuple.bitmaskValue(index++);
    }

    @Override
    public BigInteger readBigInt() {
        return tuple.value(index++);
    }

    @Override
    public BigDecimal readBigDecimal(int scale) {
        return new BigDecimal(tuple.value(index++), scale);
    }

    @Override
    public LocalDate readDate() {
        return tuple.dateValue(index++);
    }

    @Override
    public LocalTime readTime() {
        return tuple.timeValue(index++);
    }

    @Override
    public Instant readTimestamp() {
        return tuple.timestampValue(index++);
    }

    @Override
    public LocalDateTime readDateTime() {
        return tuple.datetimeValue(index++);
    }
}
