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
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Tuple} implementation over {@link BinaryTupleReader},
 * with mutable fallback.
 */
public abstract class MutableTupleBinaryTupleAdapter implements Tuple, BinaryTupleContainer {
    /** Underlying BinaryTuple. */
    private BinaryTupleReader binaryTuple;

    /** Column count. Can be less than binaryTuple.elementCount in case of a slice. */
    private final int columnCount;

    /** Tuple with overwritten data. */
    private @Nullable Tuple tuple;

    /** No-value set. */
    private final @Nullable BitSet noValueSet;

    /**
     * Constructor.
     *
     * @param binaryTuple Binary tuple.
     * @param columnCount Column count.
     * @param noValueSet No-value set.
     */
    public MutableTupleBinaryTupleAdapter(BinaryTupleReader binaryTuple, int columnCount, @Nullable BitSet noValueSet) {
        assert binaryTuple != null : "binaryTuple != null";

        this.binaryTuple = binaryTuple;
        this.columnCount = columnCount;
        this.noValueSet = noValueSet;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        if (tuple != null) {
            return tuple.columnCount();
        }

        int cnt = columnCount;

        if (noValueSet != null) {
            cnt -= noValueSet.cardinality();
        }

        return cnt;
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
    public int columnIndex(String columnName) {
        if (tuple != null) {
            return tuple.columnIndex(columnName);
        }

        int binaryTupleIndex = binaryTupleIndex(columnName, null);

        return binaryTupleIndex < 0 ? -1 : publicIndex(binaryTupleIndex);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(String columnName, T defaultValue) {
        if (tuple != null) {
            return tuple.valueOrDefault(columnName, defaultValue);
        }

        int binaryTupleIndex = binaryTupleIndex(columnName, null);

        return binaryTupleIndex < 0
                || publicIndex(binaryTupleIndex) < 0
                || (noValueSet != null && noValueSet.get(binaryTupleIndex))
                ? defaultValue
                : (T) object(binaryTupleIndex);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(String columnName) {
        if (tuple != null) {
            return tuple.value(columnName);
        }

        int binaryTupleIndex = binaryTupleIndex(columnName, null);

        if (binaryTupleIndex < 0 || publicIndex(binaryTupleIndex) < 0) {
            throw new IllegalArgumentException("Column doesn't exist [name=" + columnName + ']');
        }

        return (T) object(binaryTupleIndex);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        if (tuple != null) {
            return tuple.value(columnIndex);
        }

        Objects.checkIndex(columnIndex, columnCount);

        int binaryTupleIndex = binaryTupleIndex(columnIndex);
        return (T) object(binaryTupleIndex);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        return tuple != null
                ? tuple.booleanValue(columnName)
                : binaryTuple.booleanValue(validateSchemaColumnType(columnName, ColumnType.BOOLEAN));
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        return tuple != null
                ? tuple.booleanValue(columnIndex)
                : binaryTuple.booleanValue(validateSchemaColumnType(columnIndex, ColumnType.BOOLEAN));
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        return tuple != null
                ? tuple.byteValue(columnName)
                : binaryTuple.byteValue(validateSchemaColumnType(columnName, ColumnType.INT8));
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return tuple != null
                ? tuple.byteValue(columnIndex)
                : binaryTuple.byteValue(validateSchemaColumnType(columnIndex, ColumnType.INT8));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        return tuple != null
                ? tuple.shortValue(columnName)
                : binaryTuple.shortValue(validateSchemaColumnType(columnName, ColumnType.INT16));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return tuple != null
                ? tuple.shortValue(columnIndex)
                : binaryTuple.shortValue(validateSchemaColumnType(columnIndex, ColumnType.INT16));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        return tuple != null
                ? tuple.intValue(columnName)
                : binaryTuple.intValue(validateSchemaColumnType(columnName, ColumnType.INT32));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return tuple != null
                ? tuple.intValue(columnIndex)
                : binaryTuple.intValue(validateSchemaColumnType(columnIndex, ColumnType.INT32));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        return tuple != null
                ? tuple.longValue(columnName)
                : binaryTuple.longValue(validateSchemaColumnType(columnName, ColumnType.INT64));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return tuple != null
                ? tuple.longValue(columnIndex)
                : binaryTuple.longValue(validateSchemaColumnType(columnIndex, ColumnType.INT64));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        return tuple != null
                ? tuple.floatValue(columnName)
                : binaryTuple.floatValue(validateSchemaColumnType(columnName, ColumnType.FLOAT));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return tuple != null
                ? tuple.floatValue(columnIndex)
                : binaryTuple.floatValue(validateSchemaColumnType(columnIndex, ColumnType.FLOAT));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        return tuple != null
                ? tuple.doubleValue(columnName)
                : binaryTuple.doubleValue(validateSchemaColumnType(columnName, ColumnType.DOUBLE));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return tuple != null
                ? tuple.doubleValue(columnIndex)
                : binaryTuple.doubleValue(validateSchemaColumnType(columnIndex, ColumnType.DOUBLE));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        return tuple != null
                ? tuple.stringValue(columnName)
                : binaryTuple.stringValue(validateSchemaColumnType(columnName, ColumnType.STRING));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return tuple != null
                ? tuple.stringValue(columnIndex)
                : binaryTuple.stringValue(validateSchemaColumnType(columnIndex, ColumnType.STRING));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        return tuple != null
                ? tuple.uuidValue(columnName)
                : binaryTuple.uuidValue(validateSchemaColumnType(columnName, ColumnType.UUID));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return tuple != null
                ? tuple.uuidValue(columnIndex)
                : binaryTuple.uuidValue(validateSchemaColumnType(columnIndex, ColumnType.UUID));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        return tuple != null
                ? tuple.dateValue(columnName)
                : binaryTuple.dateValue(validateSchemaColumnType(columnName, ColumnType.DATE));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        return tuple != null
                ? tuple.dateValue(columnIndex)
                : binaryTuple.dateValue(validateSchemaColumnType(columnIndex, ColumnType.DATE));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        return tuple != null
                ? tuple.timeValue(columnName)
                : binaryTuple.timeValue(validateSchemaColumnType(columnName, ColumnType.TIME));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        return tuple != null
                ? tuple.timeValue(columnIndex)
                : binaryTuple.timeValue(validateSchemaColumnType(columnIndex, ColumnType.TIME));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        return tuple != null
                ? tuple.datetimeValue(columnName)
                : binaryTuple.dateTimeValue(validateSchemaColumnType(columnName, ColumnType.DATETIME));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        return tuple != null
                ? tuple.datetimeValue(columnIndex)
                : binaryTuple.dateTimeValue(validateSchemaColumnType(columnIndex, ColumnType.DATETIME));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        return tuple != null
                ? tuple.timestampValue(columnName)
                : binaryTuple.timestampValue(validateSchemaColumnType(columnName, ColumnType.TIMESTAMP));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        return tuple != null
                ? tuple.timestampValue(columnIndex)
                : binaryTuple.timestampValue(validateSchemaColumnType(columnIndex, ColumnType.TIMESTAMP));
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, @Nullable Object value) {
        if (tuple == null) {
            tuple = Tuple.copy(this);

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

        //noinspection SimplifiableIfStatement
        if (obj instanceof Tuple) {
            return Tuple.equals(this, (Tuple) obj);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryTupleReader binaryTuple() {
        if (tuple != null) {
            return null;
        }

        if (noValueSet != null && !noValueSet.isEmpty()) {
            // Can't expose binary tuple if there are unset values.
            // On the server side, DefaultValueProvider is used to replace unset values with proper defaults.
            return null;
        }

        return binaryTuple;
    }

    protected abstract String schemaColumnName(int binaryTupleIndex);

    protected abstract ColumnType schemaColumnType(int binaryTupleIndex);

    protected abstract int schemaDecimalScale(int binaryTupleIndex);

    protected int publicIndex(int binaryTupleIndex) {
        return binaryTupleIndex;
    }

    protected abstract int binaryTupleIndex(String columnName);

    protected int binaryTupleIndex(int publicIndex) {
        return publicIndex;
    }

    private int binaryTupleIndex(String columnName, @Nullable ColumnType type) {
        var binaryTupleIndex = binaryTupleIndex(columnName);

        if (binaryTupleIndex < 0) {
            return binaryTupleIndex;
        }

        if (type != null) {
            ColumnType actualType = schemaColumnType(binaryTupleIndex);

            if (type != actualType) {
                throw new ClassCastException("Column with name '" + columnName + "' has type " + actualType
                        + " but " + type + " was requested");
            }
        }

        return binaryTupleIndex;
    }

    private int validateSchemaColumnType(String columnName, ColumnType type) {
        var index = binaryTupleIndex(columnName, type);

        if (index < 0) {
            throw new IllegalArgumentException("Column doesn't exist [name=" + columnName + ']');
        }

        return index;
    }

    private int validateSchemaColumnType(int publicIndex, ColumnType type) {
        Objects.checkIndex(publicIndex, columnCount);

        int binaryTupleIndex = binaryTupleIndex(publicIndex);
        var actualType = schemaColumnType(binaryTupleIndex);

        if (type != actualType) {
            throw new ClassCastException("Column with index " + publicIndex + " has type " + actualType
                    + " but " + type + " was requested");
        }

        return binaryTupleIndex;
    }

    private String schemaColumnName0(int publicIndex) {
        Objects.checkIndex(publicIndex, columnCount);

        return IgniteNameUtils.quoteIfNeeded(schemaColumnName(binaryTupleIndex(publicIndex)));
    }

    private @Nullable Object object(int binaryTupleIndex) {
        if (binaryTuple.hasNullValue(binaryTupleIndex)) {
            return null;
        }

        var type = schemaColumnType(binaryTupleIndex);

        switch (type) {
            case BOOLEAN:
                return binaryTuple.booleanValue(binaryTupleIndex);

            case INT8:
                return binaryTuple.byteValue(binaryTupleIndex);

            case INT16:
                return binaryTuple.shortValue(binaryTupleIndex);

            case INT32:
                return binaryTuple.intValue(binaryTupleIndex);

            case INT64:
                return binaryTuple.longValue(binaryTupleIndex);

            case FLOAT:
                return binaryTuple.floatValue(binaryTupleIndex);

            case DOUBLE:
                return binaryTuple.doubleValue(binaryTupleIndex);

            case DECIMAL:
                return binaryTuple.decimalValue(binaryTupleIndex, schemaDecimalScale(binaryTupleIndex));

            case DATE:
                return binaryTuple.dateValue(binaryTupleIndex);

            case TIME:
                return binaryTuple.timeValue(binaryTupleIndex);

            case DATETIME:
                return binaryTuple.dateTimeValue(binaryTupleIndex);

            case TIMESTAMP:
                return binaryTuple.timestampValue(binaryTupleIndex);

            case UUID:
                return binaryTuple.uuidValue(binaryTupleIndex);

            case STRING:
                return binaryTuple.stringValue(binaryTupleIndex);

            case BYTE_ARRAY:
                return binaryTuple.bytesValue(binaryTupleIndex);

            case PERIOD:
                return binaryTuple.periodValue(binaryTupleIndex);

            case DURATION:
                return binaryTuple.durationValue(binaryTupleIndex);

            default:
                throw new IllegalStateException("Unsupported type: " + type);
        }
    }
}
