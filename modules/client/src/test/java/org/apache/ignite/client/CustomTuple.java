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

package org.apache.ignite.client;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.Tuple;

/**
 * User-defined test {@link Tuple} implementation.
 */
public class CustomTuple implements Tuple {
    private final Long id;

    private final String name;

    /**
     * Constructor.
     *
     * @param id Id.
     */
    public CustomTuple(Long id) {
        this(id, null);
    }

    /**
     * Constructor.
     *
     * @param id Id.
     * @param name Name.
     */
    public CustomTuple(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return "ID";
            case 1:
                return "NAME";
            default:
                break;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        switch (IgniteNameUtils.parseSimpleName(columnName)) {
            case "ID":
                return 0;
            case "NAME":
                return 1;
            default:
                break;
        }

        return -1;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T valueOrDefault(String columnName, T def) {
        switch (IgniteNameUtils.parseSimpleName(columnName)) {
            case "ID":
                return (T) id;
            case "NAME":
                return (T) name;
            default:
                break;
        }

        return def;
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, Object value) {
        throw new UnsupportedOperationException("Tuple is immutable.");
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(String columnName) {
        return valueOrDefault(columnName, null);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T value(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return (T) id;
            case 1:
                return (T) name;
            default:
                break;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public BinaryObject binaryObjectValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public BinaryObject binaryObjectValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
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
}
