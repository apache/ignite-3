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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader over {@link ClientMessageUnpacker}.
 */
public class ClientMarshallerReader implements MarshallerReader {
    /** Unpacker. */
    private final BinaryTupleReader unpacker;

    private final ClientColumn @Nullable [] columns;

    private final TuplePart part;

    /** Index. */
    private int index;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     * @param columns Columns.
     */
    public ClientMarshallerReader(BinaryTupleReader unpacker, ClientColumn @Nullable [] columns, TuplePart part) {
        this.unpacker = unpacker;
        this.columns = columns;
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override
    public void skipValue() {
        index++;
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean() {
        return unpacker.booleanValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Boolean readBooleanBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.booleanValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() {
        return unpacker.byteValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Byte readByteBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.byteValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() {
        return unpacker.shortValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Short readShortBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.shortValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() {
        return unpacker.intValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Integer readIntBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.intValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() {
        return unpacker.longValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Long readLongBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.longValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() {
        return unpacker.floatValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Float readFloatBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.floatValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() {
        return unpacker.doubleValue(nextSchemaIndex());
    }

    /** {@inheritDoc} */
    @Override
    public Double readDoubleBoxed() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.doubleValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public String readString() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.stringValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public UUID readUuid() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.uuidValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] readBytes() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.bytesValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal readBigDecimal(int scale) {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.decimalValue(idx, scale);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate readDate() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.dateValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime readTime() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.timeValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public Instant readTimestamp() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.timestampValue(idx);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime readDateTime() {
        var idx = nextSchemaIndex();
        return unpacker.hasNullValue(idx) ? null : unpacker.dateTimeValue(idx);
    }

    private int nextSchemaIndex() {
        int i = index++;
        if (columns == null) {
            return i;
        }

        switch (part) {
            case KEY:
                return columns[i].keyIndex();
            case VAL:
                return columns[i].valIndex();
            default:
                return columns[i].schemaIndex();
        }
    }
}
