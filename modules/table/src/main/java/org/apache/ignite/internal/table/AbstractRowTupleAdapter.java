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

package org.apache.ignite.internal.table;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract row tuple adapter.
 */
public abstract class AbstractRowTupleAdapter implements Tuple, SchemaAware {
    /**
     * Underlying row. Note: Marked transient to prevent unwanted serialization of the schema and\or other context.
     */
    protected transient @Nullable Row row;

    /**
     * Creates Tuple adapter for row.
     *
     * @param row Row.
     */
    public AbstractRowTupleAdapter(Row row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema() {
        return row.schema();
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return row.elementCount();
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        return rowColumnByIndex(columnIndex).name();
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        Objects.requireNonNull(columnName);

        Column col = row.schema().column(IgniteNameUtils.parseSimpleName(columnName));

        if (col == null) {
            return -1;
        }

        return correctIndex(col);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(String columnName, T defaultValue) {
        Objects.requireNonNull(columnName);

        Column col = row.schema().column(IgniteNameUtils.parseSimpleName(columnName));

        return col == null ? defaultValue : (T) row.value(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(String columnName) {
        Column col = rowColumnByName(columnName);

        return (T) row.value(correctIndex(col));
    }

    @Override
    public <T> T value(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return (T) row.value(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.booleanValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.booleanValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.byteValue(col.positionInRow());
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.byteValue(col.positionInRow());
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.shortValue(col.positionInRow());
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.shortValue(col.positionInRow());
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.intValue(col.positionInRow());
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.intValue(col.positionInRow());
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.longValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.longValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.floatValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.floatValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.doubleValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.doubleValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.stringValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.stringValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.uuidValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.uuidValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.bitmaskValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.bitmaskValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.dateValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.dateValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.timeValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.timeValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.dateTimeValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.dateTimeValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.timestampValue(correctIndex(col));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.timestampValue(correctIndex(col));
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

    /**
     * Returns row column for name.
     *
     * @param columnName Column name.
     * @return Column.
     */
    protected Column rowColumnByName(String columnName) {
        Objects.requireNonNull(columnName);

        Column col = row.schema().column(IgniteNameUtils.parseSimpleName(columnName));

        if (col == null) {
            throw new IllegalArgumentException(IgniteStringFormatter.format("Column doesn't exist [name={}]", columnName));
        }

        return col;
    }

    /**
     * Returns row column for given column index.
     *
     * @param columnIndex Column index.
     * @return Column.
     */
    protected Column rowColumnByIndex(int columnIndex) {
        List<Column> columns = row.keyOnly() ? row.schema().keyColumns() : row.schema().columns();

        Objects.checkIndex(columnIndex, columns.size());

        return columns.get(columnIndex);
    }

    private int correctIndex(Column col) {
        return row.keyOnly() ? col.positionInKey() : col.positionInRow();
    }
}
