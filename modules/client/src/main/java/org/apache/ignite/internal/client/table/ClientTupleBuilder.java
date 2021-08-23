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
import org.apache.ignite.table.TupleBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Client tuple builder.
 */
public final class ClientTupleBuilder implements TupleBuilder, Tuple {
    /** Null object to differentiate unset values and null values. */
    private static final Object NULL_OBJ = new Object();

    /** Columns values. */
    private final Object[] vals;

    /** Schema. */
    private final ClientSchema schema;

    /** Key-only tuple. */
    private final boolean keyOnly;

    /** Value-only tuple. */
    private final boolean valOnly;

    /**
     * Constructor.
     *
     * @param schema Schema.
     */
    public ClientTupleBuilder(ClientSchema schema) {
        this(schema, false, false);
    }

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param keyOnly Key only mode.
     * @param valOnly Val only mode.
     */
    public ClientTupleBuilder(ClientSchema schema, boolean keyOnly, boolean valOnly) {
        assert schema != null : "Schema can't be null.";
        assert schema.columns().length > 0 : "Schema can't be empty.";
        assert !(keyOnly && valOnly) : "keyOnly and valOnly can't be true at the same time.";

        this.schema = schema;
        this.keyOnly = keyOnly;
        this.valOnly = valOnly;
        this.vals = new Object[columnCount()];
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String columnName, Object value) {
        // TODO: Live schema and schema evolution support IGNITE-15194
        var col = schema.column(columnName);

        vals[getColumnIndex(col.schemaIndex())] = value == null ? NULL_OBJ : value;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String columnName, T def) {
        var col = schema.columnSafe(columnName);

        if (col == null)
            return def;

        var val = (T)vals[getColumnIndex(col.schemaIndex())];

        return val == null ? def : convertValue(val);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String columnName) {
        var col = schema.column(columnName);

        return getValue(getColumnIndex(col.schemaIndex()));
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        validateColumnIndex(columnIndex);

        return getValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        if (keyOnly)
            return schema.keyColumnCount();

        if (valOnly)
            return schema.columns().length - schema.keyColumnCount();

        return schema.columns().length;
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        validateColumnIndex(columnIndex);

        if (valOnly)
            columnIndex += schema.keyColumnCount();

        return schema.columns()[columnIndex].name();
    }

    /** {@inheritDoc} */
    @Override public Integer columnIndex(String columnName) {
        var col = schema.columnSafe(columnName);

        return col == null ? null : getColumnIndex(col.schemaIndex());
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
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < vals.length;
            }

            /** {@inheritDoc} */
            @Override public Object next() {
                return cur < vals.length ? vals[cur++] : null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        var sb = new StringBuilder("ClientTupleBuilder [");

        for (int i = 0; i < columnCount(); i++)
            sb.append(columnName(i)).append('=').append((Object) value(i));

        sb.append(']');

        return sb.toString();
    }

    /**
     * Sets column value by index.
     *
     * @param columnIndex Column index.
     * @param value Value to set.
     */
    public void setInternal(int columnIndex, Object value) {
        // Do not validate column index for internal needs.
        vals[getColumnIndex(columnIndex)] = value;
    }

    /**
     * Gets the schema.
     *
     * @return Schema.
     */
    public ClientSchema schema() {
        return schema;
    }

    private void validateColumnIndex(int columnIndex) {
        if (columnIndex < 0)
            throw new IllegalArgumentException("Column index can't be negative");

        if (columnIndex >= columnCount())
            throw new IllegalArgumentException("Column index can't be greater than " + (columnCount() - 1));
    }

    private int getColumnIndex(int columnIndex) {
        if (valOnly)
            columnIndex -= schema.keyColumnCount();

        return columnIndex;
    }

    private <T> T getValue(int columnIndex) {
        if (columnIndex >= vals.length)
            return null;

        return convertValue((T) vals[columnIndex]);
    }

    private static <T> T convertValue(T val) {
        return val == NULL_OBJ ? null : val;
    }
}
