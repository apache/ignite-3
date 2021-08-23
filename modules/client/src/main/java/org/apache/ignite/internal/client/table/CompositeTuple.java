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

package org.apache.ignite.internal.client.table;

import java.util.BitSet;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Composite tuple combines two tuples into one.
 */
public class CompositeTuple implements Tuple {
    /** First tuple. */
    private final Tuple a;

    /** Second tuple. */
    private final Tuple b;

    /**
     * Constructor.
     *
     * @param a First tuple.
     * @param b Second tuple.
     */
    public CompositeTuple(Tuple a, Tuple b) {
        assert a != null;
        assert b != null;

        this.a = a;
        this.b = b;
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return a.columnCount() + b.columnCount();
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        return columnIndex < a.columnCount()
                ? a.columnName(columnIndex)
                : b.columnName(columnIndex - a.columnCount());
    }

    /** {@inheritDoc} */
    @Override public Integer columnIndex(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String columnName, T def) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        return columnIndex < a.columnCount()
                ? a.value(columnIndex)
                : b.value(columnIndex - a.columnCount());
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public short shortValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int intValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long longValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public float floatValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String stringValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
}
