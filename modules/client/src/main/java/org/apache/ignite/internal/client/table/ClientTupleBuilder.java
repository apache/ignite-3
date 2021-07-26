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
import java.util.UUID;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;

/**
 * Client tuple builder.
 */
public final class ClientTupleBuilder implements TupleBuilder, Tuple {
    /** Columns values. */
    private final Object[] vals;

    /** Schema. */
    private final ClientSchema schema;

    /**
     * Constructor.
     *
     * @param schema Schema.
     */
    public ClientTupleBuilder(ClientSchema schema) {
        assert schema != null;

        this.schema = schema;
        this.vals = new Object[schema.columns().length];
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String colName, Object value) {
        var col = schema.column(colName);

        vals[col.schemaIndex()] = value;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String colName, T def) {
        T res = value(colName);

        return res == null ? def : res;
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String colName) {
        var col = schema.column(colName);

        return (T)vals[col.schemaIndex()];
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        validateColumnIndex(columnIndex);

        return (T)vals[columnIndex];
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return vals.length;
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        validateColumnIndex(columnIndex);

        return schema.columns()[columnIndex].name();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectField(String colName) {
        throw new IgniteException("Not supported");
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String colName) {
        return value(colName);
    }

    private void validateColumnIndex(int columnIndex) {
        if (columnIndex < 0)
            throw new IllegalArgumentException("Column index can't be negative");

        if (columnIndex >= vals.length)
            throw new IllegalArgumentException("Column index can't be greater than " + (vals.length - 1));
    }
}
