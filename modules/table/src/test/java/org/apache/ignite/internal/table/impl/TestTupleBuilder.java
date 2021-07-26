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

package org.apache.ignite.internal.table.impl;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjects;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;

/**
 * Dummy table storage implementation.
 */
public class TestTupleBuilder implements TupleBuilder, Tuple {
    /** Columns values. */
    private final Map<String, Object> map = new HashMap<>();

    /** {@inheritDoc} */
    @Override public TestTupleBuilder set(String colName, Object value) {
        map.put(colName, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String columnName, T def) {
        return (T)map.getOrDefault(columnName, def);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String columnName) {
        return (T)map.get(columnName);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectField(String columnName) {
        byte[] data = value(columnName);

        return BinaryObjects.wrap(data);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        return value(columnName);
    }
}
