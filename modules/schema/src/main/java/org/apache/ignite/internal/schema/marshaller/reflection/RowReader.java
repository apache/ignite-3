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

    private final int[] mapping;

    RowReader(Row row) {
        this(row, 0);
    }

    RowReader(Row row, int index) {
        this.row = row;
        this.index = index;
        this.mapping = null;
    }

    RowReader(Row row, int index, int[] mapping) {
        this.row = row;
        this.index = index;
        this.mapping = mapping;
    }


    @Override
    public void setIndex(int index) {
//        assert index < row.elementCount();
//
//        this.index = index;
    }

    @Override
    public int currentIndex() {
        return index;
    }

    @Override
    public void skipValue() {
        index++;
    }

    private int nextSchemaIndex() {
        int i = index++;
        return mapping == null ? i : mapping[i];
    }

    @Override
    public boolean readBoolean() {
        int idx = nextSchemaIndex();
        return row.booleanValue(idx);
    }

    @Override
    public Boolean readBooleanBoxed() {
        int idx = nextSchemaIndex();
        return row.booleanValueBoxed(idx);
    }

    @Override
    public byte readByte() {
        int idx = nextSchemaIndex();
        return row.byteValue(idx);
    }

    @Override
    public Byte readByteBoxed() {
        int idx = nextSchemaIndex();
        return row.byteValueBoxed(idx);
    }

    @Override
    public short readShort() {
        int idx = nextSchemaIndex();
        return row.shortValue(idx);
    }

    @Override
    public Short readShortBoxed() {
        int idx = nextSchemaIndex();
        return row.shortValueBoxed(idx);
    }

    @Override
    public int readInt() {
        int idx = nextSchemaIndex();
        return row.intValue(idx);
    }

    @Override
    public Integer readIntBoxed() {
        int idx = nextSchemaIndex();
        return row.intValueBoxed(idx);
    }

    @Override
    public long readLong() {
        int idx = nextSchemaIndex();
        return row.longValue(idx);
    }

    @Override
    public Long readLongBoxed() {
        int idx = nextSchemaIndex();
        return row.longValueBoxed(idx);
    }

    @Override
    public float readFloat() {
        int idx = nextSchemaIndex();
        return row.floatValue(idx);
    }

    @Override
    public Float readFloatBoxed() {
        int idx = nextSchemaIndex();
        return row.floatValueBoxed(idx);
    }

    @Override
    public double readDouble() {
        int idx = nextSchemaIndex();
        return row.doubleValue(idx);
    }

    @Override
    public Double readDoubleBoxed() {
        int idx = nextSchemaIndex();
        return row.doubleValueBoxed(idx);
    }

    @Override
    public String readString() {
        int idx = nextSchemaIndex();
        return row.stringValue(idx);
    }

    @Override
    public UUID readUuid() {
        int idx = nextSchemaIndex();
        return row.uuidValue(idx);
    }

    @Override
    public byte[] readBytes() {
        int idx = nextSchemaIndex();
        return row.bytesValue(idx);
    }

    @Override
    public BitSet readBitSet() {
        int idx = nextSchemaIndex();
        return row.bitmaskValue(idx);
    }

    @Override
    public BigInteger readBigInt() {
        int idx = nextSchemaIndex();
        return row.numberValue(idx);
    }

    @Override
    public BigDecimal readBigDecimal(int scale) {
        int idx = nextSchemaIndex();
        return row.decimalValue(idx);
    }

    @Override
    public LocalDate readDate() {
        int idx = nextSchemaIndex();
        return row.dateValue(idx);
    }

    @Override
    public LocalTime readTime() {
        int idx = nextSchemaIndex();
        return row.timeValue(idx);
    }

    @Override
    public Instant readTimestamp() {
        int idx = nextSchemaIndex();
        return row.timestampValue(idx);
    }

    @Override
    public LocalDateTime readDateTime() {
        int idx = nextSchemaIndex();
        return row.dateTimeValue(idx);
    }
}
