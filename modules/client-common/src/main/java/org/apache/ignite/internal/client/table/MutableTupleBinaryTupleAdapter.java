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

package org.apache.ignite.internal.client.table;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Tuple} implementation over {@link BinaryTupleReader},
 * with mutable fallback.
 */
public abstract class MutableTupleBinaryTupleAdapter implements Tuple {
    // TODO: SchemaAware?
    // TODO: This class should be in client, not client-common: we need to deal with ClientSchema.
    // BUT at the same time, this might be needed on the server side to wrap BinaryTuple.
    // So we might abstract schema access somehow?
    // TODO: See MutableRowTupleAdapter.
    // TODO: Should this class replace ClientTuple completely?
    /** Underlying BinaryTuple. */
    private BinaryTupleReader binaryTuple;

    private final int schemaOffset;

    private final int schemaSize;

    /** Tuple with overwritten data. */
    private @Nullable Tuple tuple;

    /**
     * Constructor.
     *
     * @param binaryTuple Binary tuple.
     */
    public MutableTupleBinaryTupleAdapter(BinaryTupleReader binaryTuple, int schemaOffset, int schemaSize) {
        assert binaryTuple != null : "binaryTuple != null";
        assert schemaOffset >= 0 : "schemaOffset >= 0";
        assert schemaSize > 0 : "schemaSize > 0";

        this.binaryTuple = binaryTuple;
        this.schemaOffset = schemaOffset;
        this.schemaSize = schemaSize;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return tuple != null ? tuple.columnCount() : schemaSize - schemaOffset;
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        return tuple != null
                ? tuple.columnName(columnIndex)
                : schemaColumnName0(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(@NotNull String columnName) {
        if (tuple != null) {
            return tuple.columnIndex(columnName);
        }

        int internalIndex = schemaColumnIndex(columnName, null);

        return internalIndex < 0 || internalIndex >= schemaSize ? -1 : internalIndex - schemaOffset;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(@NotNull String columnName, T defaultValue) {
        if (tuple != null) {
            return tuple.valueOrDefault(columnName, defaultValue);
        }

        int internalIndex = schemaColumnIndex(columnName, null);

        return internalIndex < 0 || internalIndex >= schemaSize ? defaultValue : value(internalIndex);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(@NotNull String columnName) {
        if (tuple != null) {
            return tuple.value(columnName);
        }

        int internalIndex = schemaColumnIndex(columnName, null);

        if (internalIndex < 0 || internalIndex >= schemaSize) {
            throw new ColumnNotFoundException(columnName);
        }

        return (T)object(internalIndex);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        if (tuple != null) {
            return tuple.value(columnIndex);
        }

        Objects.checkIndex(columnIndex, schemaSize - schemaOffset);

        int internalIndex = columnIndex + schemaOffset;
        return (T)object(internalIndex);
    }

    /** {@inheritDoc} */
    @Override
    public BinaryObject binaryObjectValue(@NotNull String columnName) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public BinaryObject binaryObjectValue(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.byteValue(columnName)
                : binaryTuple.byteValue(validateSchemaColumnIndex(columnName, ColumnType.INT8));
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return tuple != null
                ? tuple.byteValue(columnIndex)
                : binaryTuple.byteValue(validateSchemaColumnIndex(columnIndex, ColumnType.INT8));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.shortValue(columnName)
                : binaryTuple.shortValue(validateSchemaColumnIndex(columnName, ColumnType.INT16));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return tuple != null
                ? tuple.shortValue(columnIndex)
                : binaryTuple.shortValue(validateSchemaColumnIndex(columnIndex, ColumnType.INT16));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.intValue(columnName)
                : binaryTuple.intValue(validateSchemaColumnIndex(columnName, ColumnType.INT32));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return tuple != null
                ? tuple.intValue(columnIndex)
                : binaryTuple.intValue(validateSchemaColumnIndex(columnIndex, ColumnType.INT32));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.longValue(columnName)
                : binaryTuple.longValue(validateSchemaColumnIndex(columnName, ColumnType.INT64));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return tuple != null
                ? tuple.longValue(columnIndex)
                : binaryTuple.longValue(validateSchemaColumnIndex(columnIndex, ColumnType.INT64));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.floatValue(columnName)
                : binaryTuple.floatValue(validateSchemaColumnIndex(columnName, ColumnType.FLOAT));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return tuple != null
                ? tuple.floatValue(columnIndex)
                : binaryTuple.floatValue(validateSchemaColumnIndex(columnIndex, ColumnType.FLOAT));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.doubleValue(columnName)
                : binaryTuple.doubleValue(validateSchemaColumnIndex(columnName, ColumnType.DOUBLE));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return tuple != null
                ? tuple.doubleValue(columnIndex)
                : binaryTuple.doubleValue(validateSchemaColumnIndex(columnIndex, ColumnType.DOUBLE));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.stringValue(columnName)
                : binaryTuple.stringValue(validateSchemaColumnIndex(columnName, ColumnType.STRING));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return tuple != null
                ? tuple.stringValue(columnIndex)
                : binaryTuple.stringValue(validateSchemaColumnIndex(columnIndex, ColumnType.STRING));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.uuidValue(columnName)
                : binaryTuple.uuidValue(validateSchemaColumnIndex(columnName, ColumnType.UUID));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return tuple != null
                ? tuple.uuidValue(columnIndex)
                : binaryTuple.uuidValue(validateSchemaColumnIndex(columnIndex, ColumnType.UUID));
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(@NotNull String columnName) {
        return tuple != null
                ? tuple.bitmaskValue(columnName)
                : binaryTuple.bitmaskValue(validateSchemaColumnIndex(columnName, ColumnType.BITMASK));
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(int columnIndex) {
        return tuple != null
                ? tuple.bitmaskValue(columnIndex)
                : binaryTuple.bitmaskValue(validateSchemaColumnIndex(columnIndex, ColumnType.BITMASK));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        return tuple != null
                ? tuple.dateValue(columnName)
                : binaryTuple.dateValue(validateSchemaColumnIndex(columnName, ColumnType.DATE));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        return tuple != null
                ? tuple.dateValue(columnIndex)
                : binaryTuple.dateValue(validateSchemaColumnIndex(columnIndex, ColumnType.DATE));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        return tuple != null
                ? tuple.timeValue(columnName)
                : binaryTuple.timeValue(validateSchemaColumnIndex(columnName, ColumnType.TIME));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        return tuple != null
                ? tuple.timeValue(columnIndex)
                : binaryTuple.timeValue(validateSchemaColumnIndex(columnIndex, ColumnType.TIME));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        return tuple != null
                ? tuple.datetimeValue(columnName)
                : binaryTuple.dateTimeValue(validateSchemaColumnIndex(columnName, ColumnType.DATETIME));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        return tuple != null
                ? tuple.datetimeValue(columnIndex)
                : binaryTuple.dateTimeValue(validateSchemaColumnIndex(columnIndex, ColumnType.DATETIME));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        return tuple != null
                ? tuple.timestampValue(columnName)
                : binaryTuple.timestampValue(validateSchemaColumnIndex(columnName, ColumnType.TIMESTAMP));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        return tuple != null
                ? tuple.timestampValue(columnIndex)
                : binaryTuple.timestampValue(validateSchemaColumnIndex(columnIndex, ColumnType.TIMESTAMP));
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(@NotNull String columnName, Object value) {
        if (tuple == null) {
            tuple = Tuple.create(this);

            //noinspection DataFlowIssue
            binaryTuple = null;
        }

        tuple.set(columnName, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Tuple.hashCode(this);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Tuple) {
            return Tuple.equals(this, (Tuple) obj);
        }

        return false;
    }

    protected abstract String schemaColumnName(int internalIndex);

    private String schemaColumnName0(int publicIndex) {
        Objects.checkIndex(publicIndex, schemaSize - schemaOffset);

        return schemaColumnName(publicIndex + schemaOffset);
    }

    protected abstract int schemaColumnIndex(@NotNull String columnName, @Nullable ColumnType type);

    private int validateSchemaColumnIndex(int publicIndex, ColumnType type) {
        Objects.checkIndex(publicIndex, schemaSize - schemaOffset);

        int internalIndex = publicIndex + schemaOffset;
        var actualType = schemaColumnType(internalIndex);

        if (type != actualType) {
            throw new IllegalStateException("Column with index " + publicIndex + " has type " + actualType +
                    " but " + type + " was requested");
        }

        return internalIndex;
    }

    protected abstract ColumnType schemaColumnType(int columnIndex);

    protected abstract int schemaDecimalScale(int columnIndex);

    private int validateSchemaColumnIndex(@NotNull String columnName, ColumnType type) {
        var index = schemaColumnIndex(columnName, type);

        if (index < 0) {
            throw new ColumnNotFoundException(columnName);
        }

        return index;
    }

    private @Nullable Object object(int internalIndex) {
        if (binaryTuple.hasNullValue(internalIndex)) {
            return null;
        }

        var type = schemaColumnType(internalIndex);

        switch (type) {
            case BOOLEAN:
                return binaryTuple.byteValue(internalIndex) != 0;

            case INT8:
                return binaryTuple.byteValue(internalIndex);

            case INT16:
                return binaryTuple.shortValue(internalIndex);

            case INT32:
                return binaryTuple.intValue(internalIndex);

            case INT64:
                return binaryTuple.longValue(internalIndex);

            case FLOAT:
                return binaryTuple.floatValue(internalIndex);

            case DOUBLE:
                return binaryTuple.doubleValue(internalIndex);

            case DECIMAL:
                return binaryTuple.decimalValue(internalIndex, schemaDecimalScale(internalIndex));

            case DATE:
                return binaryTuple.dateValue(internalIndex);

            case TIME:
                return binaryTuple.timeValue(internalIndex);

            case DATETIME:
                return binaryTuple.dateTimeValue(internalIndex);

            case TIMESTAMP:
                return binaryTuple.timestampValue(internalIndex);

            case UUID:
                return binaryTuple.uuidValue(internalIndex);

            case BITMASK:
                return binaryTuple.bitmaskValue(internalIndex);

            case STRING:
                return binaryTuple.stringValue(internalIndex);

            case BYTE_ARRAY:
                return binaryTuple.bytesValue(internalIndex);

            case PERIOD:
                return binaryTuple.periodValue(internalIndex);

            case DURATION:
                return binaryTuple.durationValue(internalIndex);

            case NUMBER:
                return binaryTuple.numberValue(internalIndex);

            default:
                throw new IllegalStateException("Unsupported type: " + type);
        }
    }
}
