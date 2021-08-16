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

package org.apache.ignite.table;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;

/**
 * Simple tuple implementation.
 */
public class TupleImpl implements Tuple {
    /** Column name -&gt; index. */
    protected Map<String, Integer> colIdxMap;

    /** Columns names. */
    private final ArrayList<String> colNames;

    /** Columns values. */
    private final ArrayList<Object> vals;

    /**
     * Creates tuple.
     */
    public TupleImpl() {
        colIdxMap = new HashMap<>();
        vals = new ArrayList();
        colNames = new ArrayList();
    }

    /** {@inheritDoc} */
    @Override public Tuple set(String columnName, Object val) {
        int idx = colIdxMap.computeIfAbsent(columnName, name -> colIdxMap.size());

        if (idx == colNames.size()) {
            colNames.add(idx, columnName);
            vals.add(idx, val);
        }
        else {
            colNames.set(idx, columnName);
            vals.set(idx, val);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        Objects.checkIndex(columnIndex, vals.size());

        return colNames.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int columnIndex(String columnName) {
        Integer idx = colIdxMap.get(columnName);

        return idx == null ? -1 : idx;
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return colNames.size();
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String columnName, T def) {
        int idx = columnIndex(columnName);

        return (idx == -1) ? def : (T)vals.get(idx);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String columnName) {
        int idx = columnIndex(columnName);

        if (idx == -1)
            throw new IgniteException("Column not found: columnName=" + columnName);

        return (T)vals.get(idx);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        Objects.checkIndex(columnIndex, vals.size());

        return (T)vals.get(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Object> iterator() {
        return new Iterator<>() {
            /** Current column index. */
            private int cur = 0;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < vals.size();
            }

            /** {@inheritDoc} */
            @Override public Object next() {
                return hasNext() ? vals.get(cur++) : null;
            }
        };
    }
}
