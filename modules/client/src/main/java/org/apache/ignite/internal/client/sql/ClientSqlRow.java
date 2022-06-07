/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.sql;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Client SQL row.
 */
public class ClientSqlRow implements SqlRow {
    /** Row. */
    private final List<Object> row;

    /**
     * Constructor.
     *
     * @param row Row.
     */
    ClientSqlRow(List<Object> row) {
        //noinspection AssignmentOrReturnOfFieldWithMutableType
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return row.size();
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        // TODO: IGNITE-17052
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(@NotNull String columnName) {
        // TODO: IGNITE-17052
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private int columnIndexChecked(@NotNull String columnName) {
        int idx = columnIndex(columnName);

        if (idx == -1) {
            throw new IllegalArgumentException("Column doesn't exist [name=" + columnName + ']');
        }

        return idx;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(@NotNull String columnName, T defaultValue) {
        T ret = (T) row.get(columnIndexChecked(columnName));

        return ret != null ? ret : defaultValue;
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(@NotNull String columnName, Object value) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(@NotNull String columnName) throws IllegalArgumentException {
        return (T) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        return (T) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public BinaryObject binaryObjectValue(@NotNull String columnName) {
        return (BinaryObject) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public BinaryObject binaryObjectValue(int columnIndex) {
        return (BinaryObject) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(@NotNull String columnName) {
        return (byte) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return (byte) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(@NotNull String columnName) {
        return (short) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return (short) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(@NotNull String columnName) {
        return (int) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return (int) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(@NotNull String columnName) {
        return (long) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return (long) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(@NotNull String columnName) {
        return (float) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return (float) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(@NotNull String columnName) {
        return (double) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return (double) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(@NotNull String columnName) {
        return (String) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return (String) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(@NotNull String columnName) {
        return (UUID) row.get(columnIndexChecked(columnName));
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return (UUID) row.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(@NotNull String columnName) {
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
    @NotNull
    @Override
    public Iterator<Object> iterator() {
        return row.iterator();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        // TODO: IGNITE-17052
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
