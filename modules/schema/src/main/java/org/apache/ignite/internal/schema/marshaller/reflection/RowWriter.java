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
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.marshaller.MarshallerWriter;
import org.apache.ignite.internal.schema.row.RowAssembler;

/**
 * Adapter from a {@link RowAssembler} to a {@link MarshallerWriter}.
 */
class RowWriter implements MarshallerWriter {
    private final RowAssembler rowAssembler;

    RowWriter(RowAssembler rowAssembler) {
        this.rowAssembler = rowAssembler;
    }

    @Override
    public void writeNull() {
        rowAssembler.appendNull();
    }

    @Override
    public void writeAbsentValue() {
        rowAssembler.appendDefault();
    }

    @Override
    public void writeBoolean(boolean val) {
        rowAssembler.appendBoolean(val);
    }

    @Override
    public void writeByte(byte val) {
        rowAssembler.appendByte(val);
    }

    @Override
    public void writeShort(short val) {
        rowAssembler.appendShort(val);
    }

    @Override
    public void writeInt(int val) {
        rowAssembler.appendInt(val);
    }

    @Override
    public void writeLong(long val) {
        rowAssembler.appendLong(val);
    }

    @Override
    public void writeFloat(float val) {
        rowAssembler.appendFloat(val);
    }

    @Override
    public void writeDouble(double val) {
        rowAssembler.appendDouble(val);
    }

    @Override
    public void writeString(String val) {
        rowAssembler.appendStringNotNull(val);
    }

    @Override
    public void writeUuid(UUID val) {
        rowAssembler.appendUuidNotNull(val);
    }

    @Override
    public void writeBytes(byte[] val) {
        rowAssembler.appendBytesNotNull(val);
    }

    @Override
    public void writeBitSet(BitSet val) {
        rowAssembler.appendBitmaskNotNull(val);
    }

    @Override
    public void writeBigInt(BigInteger val) {
        rowAssembler.appendNumberNotNull(val);
    }

    @Override
    public void writeBigDecimal(BigDecimal val, int scale) {
        rowAssembler.appendDecimalNotNull(val);
    }

    @Override
    public void writeDate(LocalDate val) {
        rowAssembler.appendDateNotNull(val);
    }

    @Override
    public void writeTime(LocalTime val) {
        rowAssembler.appendTimeNotNull(val);
    }

    @Override
    public void writeTimestamp(Instant val) {
        rowAssembler.appendTimestampNotNull(val);
    }

    @Override
    public void writeDateTime(LocalDateTime val) {
        rowAssembler.appendDateTimeNotNull(val);
    }

    @Override
    public void writeValue(MarshallerColumn col, Object val) {
        rowAssembler.appendValue(val);
    }
}
