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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.lang.util.IgniteNameUtils.quoteIfNeeded;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Projection under {@link SqlRow}.
 */
public class SqlRowProjection implements Tuple {
    private final SqlRow row;

    /** Column`s name map. */
    private final Object2IntMap<String> columnsIndices;

    private final int[] rowIndexMapping;

    /**
     * Constructor.
     *
     * @param row Row data.
     * @param meta Metadata for query results.
     * @param cols Target column names.
     */
    public SqlRowProjection(SqlRow row, ResultSetMetadata meta, String[] cols) {
        this.row = row;

        rowIndexMapping = new int[cols.length];
        columnsIndices = new Object2IntOpenHashMap<>(cols.length);

        for (int i = 0; i < cols.length; i++) {
            String quotedColumnName = quoteIfNeeded(cols[i]);

            columnsIndices.put(quotedColumnName, i);
            rowIndexMapping[i] = meta.indexOf(quotedColumnName);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return rowIndexMapping.length;
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        return row.columnName(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        return columnsIndices.getOrDefault(columnName, -1);
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T valueOrDefault(String columnName, @Nullable T defaultValue) {
        return row.valueOrDefault(columnName, defaultValue);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, @Nullable Object value) {
        return row.set(columnName, value);
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T value(String columnName) throws IllegalArgumentException {
        return row.value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T value(int columnIndex) {
        return row.value(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        return row.booleanValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        return row.booleanValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        return row.byteValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return row.byteValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        return row.shortValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return row.shortValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        return row.intValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return row.intValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        return row.longValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return row.longValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        return row.floatValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return row.floatValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        return row.doubleValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return row.doubleValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        return row.stringValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return row.stringValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        return row.uuidValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return row.uuidValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        return row.dateValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        return row.dateValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        return row.timeValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        return row.timeValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        return row.datetimeValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        return row.datetimeValue(rowIndexMapping[columnIndex]);
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        return row.timestampValue(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        return row.timestampValue(rowIndexMapping[columnIndex]);
    }
}
