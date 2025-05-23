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

package org.apache.ignite.internal.sql.engine;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;

/**
 * Dummy table storage implementation.
 */
class TestRow implements SqlRow {
    /**
     * Columns values.
     */
    private final Map<String, Object> map = new HashMap<>();

    /** {@inheritDoc} */
    @Override
    public TestRow set(String columnName, Object value) {
        map.put(columnName, value);

        return this;
    }

    public SqlRow build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(String columnName, T def) {
        return (T) map.getOrDefault(columnName, def);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(String columnName) {
        return (T) map.get(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal decimalValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal decimalValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytesValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytesValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        throw new UnsupportedOperationException();
    }
}
