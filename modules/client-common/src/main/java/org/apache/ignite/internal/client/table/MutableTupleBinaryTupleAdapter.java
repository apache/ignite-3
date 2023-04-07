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
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link org.apache.ignite.table.Tuple} implementation over {@link org.apache.ignite.internal.binarytuple.BinaryTupleReader},
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

    /** Tuple with overwritten data. */
    private @Nullable Tuple tuple;

    /**
     * Constructor.
     *
     * @param binaryTuple Binary tuple.
     */
    public MutableTupleBinaryTupleAdapter(BinaryTupleReader binaryTuple) {
        this.binaryTuple = binaryTuple;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return tuple != null ? tuple.columnCount() : schemaColumnCount();
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        return tuple != null ? tuple.columnName(columnIndex) : schemaColumnName(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(@NotNull String columnName) {
        return tuple != null ? tuple.columnIndex(columnName) : schemaColumnIndex(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(@NotNull String columnName, T defaultValue) {
        if (tuple != null) {
            return tuple.valueOrDefault(columnName, defaultValue);
        }

        var idx = schemaColumnIndex(columnName);

        return idx < 0 ? defaultValue : value(idx);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(@NotNull String columnName) {
        if (tuple != null) {
            return tuple.value(columnName);
        }

        var idx = schemaColumnIndex(columnName);

        if (idx < 0) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }

        return value(idx);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        if (tuple != null) {
            return tuple.value(columnIndex);
        }

        return (T)object(columnIndex);
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
                : binaryTuple.byteValue(schemaColumnIndex(columnName, ColumnType.INT8));
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
                : binaryTuple.shortValue(schemaColumnIndex(columnName, ColumnType.INT16));
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
                : binaryTuple.intValue(schemaColumnIndex(columnName, ColumnType.INT32));
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
                : binaryTuple.longValue(schemaColumnIndex(columnName, ColumnType.INT64));
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
                : binaryTuple.floatValue(schemaColumnIndex(columnName, ColumnType.FLOAT));
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
                : binaryTuple.doubleValue(schemaColumnIndex(columnName, ColumnType.DOUBLE));
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
                : binaryTuple.stringValue(schemaColumnIndex(columnName, ColumnType.STRING));
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
                : binaryTuple.uuidValue(schemaColumnIndex(columnName, ColumnType.UUID));
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
                : binaryTuple.bitmaskValue(schemaColumnIndex(columnName, ColumnType.BITMASK));
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
                : binaryTuple.dateValue(schemaColumnIndex(columnName, ColumnType.DATE));
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
                : binaryTuple.timeValue(schemaColumnIndex(columnName, ColumnType.TIME));
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
                : binaryTuple.dateTimeValue(schemaColumnIndex(columnName, ColumnType.DATETIME));
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
                : binaryTuple.timestampValue(schemaColumnIndex(columnName, ColumnType.TIMESTAMP));
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

    protected abstract int schemaColumnCount();

    protected abstract String schemaColumnName(int index);

    protected abstract int schemaColumnIndex(@NotNull String columnName);

    protected abstract int schemaColumnIndex(@NotNull String columnName, ColumnType type);

    protected abstract int validateSchemaColumnIndex(int columnIndex, ColumnType type);

    protected abstract ColumnType schemaColumnType(int columnIndex);

    protected abstract int schemaDecimalScale(int columnIndex);

    private Object object(int columnIndex) {
        var type = schemaColumnType(columnIndex);

        switch (type) {
            case BOOLEAN:
                return binaryTuple.byteValue(columnIndex) != 0;

            case INT8:
                return binaryTuple.byteValue(columnIndex);

            case INT16:
                return binaryTuple.shortValue(columnIndex);

            case INT32:
                return binaryTuple.intValue(columnIndex);

            case INT64:
                return binaryTuple.longValue(columnIndex);

            case FLOAT:
                return binaryTuple.floatValue(columnIndex);

            case DOUBLE:
                return binaryTuple.doubleValue(columnIndex);

            case DECIMAL:
                return binaryTuple.decimalValue(columnIndex, schemaDecimalScale(columnIndex));

            case DATE:
                return binaryTuple.dateValue(columnIndex);

            case TIME:
                return binaryTuple.timeValue(columnIndex);

            case DATETIME:
                return binaryTuple.dateTimeValue(columnIndex);

            case TIMESTAMP:
                return binaryTuple.timestampValue(columnIndex);

            case UUID:
                return binaryTuple.uuidValue(columnIndex);

            case BITMASK:
                return binaryTuple.bitmaskValue(columnIndex);

            case STRING:
                return binaryTuple.stringValue(columnIndex);

            case BYTE_ARRAY:
                return binaryTuple.bytesValue(columnIndex);

            case PERIOD:
                return binaryTuple.periodValue(columnIndex);

            case DURATION:
                return binaryTuple.durationValue(columnIndex);

            case NUMBER:
                return binaryTuple.numberValue(columnIndex);

            default:
                throw new IllegalStateException("Unsupported type: " + type);
        }
    }
}
