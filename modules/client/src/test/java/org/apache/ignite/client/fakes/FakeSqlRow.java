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

package org.apache.ignite.client.fakes;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;

/**
 * Client SQL row.
 */
public class FakeSqlRow implements SqlRow {
    /** Row. */
    private final List<Object> row;

    /** Meta. */
    private final ResultSetMetadata metadata;

    /**
     * Constructor.
     *
     * @param row Row.
     * @param meta Meta.
     */
    public FakeSqlRow(List<Object> row, ResultSetMetadata meta) {
        assert row != null;
        assert meta != null;

        //noinspection AssignmentOrReturnOfFieldWithMutableType
        this.row = row;
        this.metadata = meta;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return row.size();
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        return metadata.columns().get(columnIndex).name();
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        return metadata.indexOf(columnName);
    }

    private int columnIndexChecked(String columnName) {
        int idx = columnIndex(columnName);

        if (idx == -1) {
            throw new IllegalArgumentException("Column doesn't exist [name=" + columnName + ']');
        }

        return idx;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(String columnName, T defaultValue) {
        T ret = (T) row.get(columnIndexChecked(columnName));

        return ret != null ? ret : defaultValue;
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, Object value) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(String columnName) throws IllegalArgumentException {
        return (T) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        return (T) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        return (boolean) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        return (boolean) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        return (byte) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return (byte) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        return (short) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return (short) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        return (int) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return (int) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        return (long) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return (long) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        return (float) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return (float) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        return (double) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return (double) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        return (String) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return (String) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        return (UUID) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return (UUID) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(String columnName) {
        return (BitSet) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(int columnIndex) {
        return (BitSet) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        return (LocalDate) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        return (LocalDate) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        return (LocalTime) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        return (LocalTime) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        return (LocalDateTime) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        return (LocalDateTime) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        return (Instant) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        return (Instant) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Object> iterator() {
        return row.iterator();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return metadata;
    }
}
