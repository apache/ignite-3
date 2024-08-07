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

package org.apache.ignite.table;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Simple tuple implementation.
 */
class TupleImpl implements Tuple, Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /**
     * Column name -&gt; index mapping.
     *
     * <p>Note: Transient because it's recoverable from {@link #colNames}.
     */
    private transient Map<String, Integer> colMapping;

    /** Columns names. */
    private final List<String> colNames;

    /** Columns values. */
    private final List<Object> colValues;

    /**
     * Creates a tuple.
     */
    TupleImpl() {
        this(new HashMap<>(), new ArrayList<>(), new ArrayList<>());
    }

    /**
     * Creates a tuple with the specified initial capacity.
     *
     * @param capacity Initial capacity.
     */
    TupleImpl(int capacity) {
        this(new HashMap<>(capacity), new ArrayList<>(capacity), new ArrayList<>(capacity));
    }

    /**
     * Copying constructor.
     *
     * @param tuple Tuple.
     */
    TupleImpl(Tuple tuple) {
        this(tuple.columnCount());

        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            set(tuple.columnName(i), tuple.value(i));
        }
    }

    /**
     * A private constructor.
     *
     * @param columnMapping Column name-to-idx mapping.
     * @param columnNames   List of column names.
     * @param columnValues  List of column values.
     */
    private TupleImpl(Map<String, Integer> columnMapping, List<String> columnNames, List<Object> columnValues) {
        this.colMapping = columnMapping;
        this.colNames = columnNames;
        this.colValues = columnValues;
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, @Nullable Object val) {
        String columnName0 = IgniteNameUtils.parseSimpleName(columnName);

        int idx = colMapping.computeIfAbsent(Objects.requireNonNull(columnName0), name -> colMapping.size());

        if (idx == colNames.size()) {
            colNames.add(idx, columnName0);
            colValues.add(idx, val);
        } else {
            colNames.set(idx, columnName0);
            colValues.set(idx, val);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        Objects.checkIndex(columnIndex, colValues.size());

        return IgniteNameUtils.quoteIfNeeded(colNames.get(columnIndex));
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        Objects.requireNonNull(columnName);

        Integer idx = colMapping.get(IgniteNameUtils.parseSimpleName(columnName));

        return idx == null ? -1 : idx;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return colNames.size();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(String columnName, T def) {
        int idx = columnIndex(columnName);

        return (idx == -1) ? def : (T) colValues.get(idx);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(String columnName) {
        int idx = columnIndex(columnName);

        if (idx == -1) {
            throw new IllegalArgumentException("Column doesn't exist [name=" + columnName + ']');
        }

        return (T) colValues.get(idx);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        Objects.checkIndex(columnIndex, colValues.size());

        return (T) colValues.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        return value(columnIndex);
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
     * Deserializes an object.
     *
     * @param in Input object stream.
     * @throws IOException            If failed.
     * @throws ClassNotFoundException If failed.
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // Recover column name->index mapping.
        colMapping = new HashMap<>(colNames.size());

        for (int i = 0; i < colNames.size(); i++) {
            colMapping.put(colNames.get(i), i);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();

        b.append(Tuple.class.getSimpleName()).append(" [");
        for (int i = 0; i < colNames.size(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(colNames.get(i)).append('=').append(colValues.get(i));
        }
        b.append(']');

        return b.toString();
    }
}
