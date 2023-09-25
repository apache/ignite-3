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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.marshaller.MarshallerReader;
import org.apache.ignite.internal.schema.row.Row;

/**
 * Adapter from a {@link Row} to a {@link MarshallerReader}.
 */
class RowReader implements MarshallerReader {
    private final Row row;

    private int index;

    RowReader(Row row) {
        this(row, 0);
    }

    RowReader(Row row, int index) {
        this.row = row;
        this.index = index;
    }

    @Override
    public void skipValue() {
        index++;
    }

    @Override
    public boolean readBoolean() {
        return row.booleanValue(index++);
    }

    @Override
    public Boolean readBooleanBoxed() {
        return row.booleanValueBoxed(index++);
    }

    @Override
    public byte readByte() {
        return row.byteValue(index++);
    }

    @Override
    public Byte readByteBoxed() {
        return row.byteValueBoxed(index++);
    }

    @Override
    public short readShort() {
        return row.shortValue(index++);
    }

    @Override
    public Short readShortBoxed() {
        return row.shortValueBoxed(index++);
    }

    @Override
    public int readInt() {
        return row.intValue(index++);
    }

    @Override
    public Integer readIntBoxed() {
        return row.intValueBoxed(index++);
    }

    @Override
    public long readLong() {
        return row.longValue(index++);
    }

    @Override
    public Long readLongBoxed() {
        return row.longValueBoxed(index++);
    }

    @Override
    public float readFloat() {
        return row.floatValue(index++);
    }

    @Override
    public Float readFloatBoxed() {
        return row.floatValueBoxed(index++);
    }

    @Override
    public double readDouble() {
        return row.doubleValue(index++);
    }

    @Override
    public Double readDoubleBoxed() {
        return row.doubleValueBoxed(index++);
    }

    @Override
    public String readString() {
        return row.stringValue(index++);
    }

    @Override
    public UUID readUuid() {
        return row.uuidValue(index++);
    }

    @Override
    public byte[] readBytes() {
        return row.bytesValue(index++);
    }

    @Override
    public BitSet readBitSet() {
        return row.bitmaskValue(index++);
    }

    @Override
    public BigInteger readBigInt() {
        return row.numberValue(index++);
    }

    @Override
    public BigDecimal readBigDecimal(int scale) {
        return row.decimalValue(index++, scale);
    }

    @Override
    public LocalDate readDate() {
        return row.dateValue(index++);
    }

    @Override
    public LocalTime readTime() {
        return row.timeValue(index++);
    }

    @Override
    public Instant readTimestamp() {
        return row.timestampValue(index++);
    }

    @Override
    public LocalDateTime readDateTime() {
        return row.dateTimeValue(index++);
    }
}
