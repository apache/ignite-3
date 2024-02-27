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

package org.apache.ignite.internal.client.table;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client schema.
 */
@SuppressWarnings({"rawtypes", "AssignmentOrReturnOfFieldWithMutableType"})
public class ClientSchema {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Columns. */
    private final ClientColumn[] columns;

    private final ClientColumn[] keyColumns;

    private final ClientColumn[] valColumns;

    /** Colocation columns. */
    private final ClientColumn[] colocationColumns;

    /** Columns map by name. */
    private final Map<String, ClientColumn> map = new HashMap<>();

    /** Marshaller provider. */
    private final MarshallersProvider marshallers;

    /** Marshaller schema. */
    private MarshallerSchema marshallerSchema;

    /**
     * Constructor.
     *
     * @param ver Schema version.
     * @param columns Columns.
     * @param colocationColumns Colocation columns. When null, all key columns are used.
     * @param marshallers Marshallers provider.
     */
    public ClientSchema(
            int ver,
            ClientColumn[] columns,
            ClientColumn[] keyColumns,
            ClientColumn @Nullable [] colocationColumns,
            MarshallersProvider marshallers) {
        assert ver >= 0;
        assert columns != null;

        this.ver = ver;
        this.columns = columns;
        this.keyColumns = keyColumns;
        this.colocationColumns = colocationColumns == null ? keyColumns : colocationColumns;
        this.marshallers = marshallers;

        for (var col : columns) {
            map.put(col.name(), col);
        }

        int valColumnCount = columns.length - keyColumns.length;
        this.valColumns = new ClientColumn[valColumnCount];

        int idx = 0;
        for (var col : columns) {
            if (!col.key()) {
                this.valColumns[idx++] = col;
            }
        }

        // TODO:
        // * Preserve column order from SchemaDescriptor
        // * Remove logic "key columns come first"
    }

    /**
     * Returns version.
     *
     * @return Version.
     */
    public int version() {
        return ver;
    }

    /**
     * Returns columns.
     *
     * @return Columns.
     */
    public ClientColumn[] columns() {
        return columns;
    }

    /**
     * Returns key columns.
     *
     * @return Key columns.
     */
    ClientColumn[] keyColumns() {
        return keyColumns;
    }

    /**
     * Returns key columns.
     *
     * @return Key columns.
     */
    ClientColumn[] valColumns() {
        return valColumns;
    }

    ClientColumn[] columns(TuplePart part) {
        if (part == TuplePart.KEY) {
            return keyColumns;
        }

        if (part == TuplePart.VAL) {
            return valColumns;
        }

        return columns;
    }

    /**
     * Returns colocation columns.
     *
     * @return Colocation columns.
     */
    ClientColumn[] colocationColumns() {
        return colocationColumns;
    }

    /**
     * Gets a column by name.
     *
     * @param name Column name.
     * @return Column by name.
     * @throws IgniteException When a column with the specified name does not exist.
     */
    public ClientColumn column(String name) {
        var column = map.get(name);

        if (column == null) {
            throw new ColumnNotFoundException(name);
        }

        return column;
    }

    /**
     * Gets a column by name.
     *
     * @param name Column name.
     * @return Column by name.
     */
    public @Nullable ClientColumn columnSafe(String name) {
        return map.get(name);
    }

    public <T> Marshaller getMarshaller(Mapper mapper, TuplePart part) {
        return getMarshaller(mapper, part, part == TuplePart.KEY);
    }

    /** Returns a marshaller for columns defined by this client schema. */
    public <T> Marshaller getMarshaller(Mapper mapper) {
        MarshallerColumn[] marshallerColumns = toMarshallerColumns(TuplePart.KEY_AND_VAL);

        return marshallers.getMarshaller(marshallerColumns, mapper, true, false);
    }

    <T> Marshaller getMarshaller(Mapper mapper, TuplePart part, boolean allowUnmappedFields) {
        switch (part) {
            case KEY:
                return marshallers.getKeysMarshaller(marshallerSchema(), mapper, true, allowUnmappedFields);
            case VAL:
                return marshallers.getValuesMarshaller(marshallerSchema(), mapper, true, allowUnmappedFields);
            case KEY_AND_VAL:
                return marshallers.getRowMarshaller(marshallerSchema(), mapper, true, allowUnmappedFields);
            default:
                throw new AssertionError("Unexpected tuple part: " + part);
        }
    }

    private MarshallerColumn[] toMarshallerColumns(TuplePart part) {
        if (part == TuplePart.VAL) {
            var res = new MarshallerColumn[columns.length - keyColumns.length];
            int idx = 0;

            for (var col : columns) {
                if (!col.key()) {
                    res[idx++] = marshallerColumn(col);
                }
            }

            return res;
        }

        ClientColumn[] cols = part == TuplePart.KEY_AND_VAL ? columns : keyColumns;
        var res = new MarshallerColumn[cols.length];

        for (int i = 0; i < cols.length; i++) {
            res[i] = marshallerColumn(cols[i]);
        }

        return res;
    }

    @NotNull
    private static MarshallerColumn marshallerColumn(ClientColumn col) {
        return new MarshallerColumn(col.name(), mode(col.type()), null, col.scale());
    }

    private static BinaryMode mode(ColumnType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return BinaryMode.BOOLEAN;

            case INT8:
                return BinaryMode.BYTE;

            case INT16:
                return BinaryMode.SHORT;

            case INT32:
                return BinaryMode.INT;

            case INT64:
                return BinaryMode.LONG;

            case FLOAT:
                return BinaryMode.FLOAT;

            case DOUBLE:
                return BinaryMode.DOUBLE;

            case UUID:
                return BinaryMode.UUID;

            case STRING:
                return BinaryMode.STRING;

            case BYTE_ARRAY:
                return BinaryMode.BYTE_ARR;

            case DECIMAL:
                return BinaryMode.DECIMAL;

            case NUMBER:
                return BinaryMode.NUMBER;

            case BITMASK:
                return BinaryMode.BITSET;

            case DATE:
                return BinaryMode.DATE;

            case TIME:
                return BinaryMode.TIME;

            case DATETIME:
                return BinaryMode.DATETIME;

            case TIMESTAMP:
                return BinaryMode.TIMESTAMP;

            default:
                throw new IgniteException(Client.PROTOCOL_ERR, "Unknown client data type: " + dataType);
        }
    }

    private MarshallerSchema marshallerSchema() {
        if (marshallerSchema == null) {
            marshallerSchema = new ClientMarshallerSchema(this);
        }
        return marshallerSchema;
    }

    private static class ClientMarshallerSchema implements MarshallerSchema {

        private final ClientSchema schema;

        private MarshallerColumn[] keys;

        private MarshallerColumn[] values;

        private MarshallerColumn[] row;

        private ClientMarshallerSchema(ClientSchema schema) {
            this.schema = schema;
        }

        @Override
        public int schemaVersion() {
            return schema.version();
        }

        @Override
        public MarshallerColumn[] keys() {
            if (keys == null) {
                keys = schema.toMarshallerColumns(TuplePart.KEY);
            }
            return keys;
        }

        @Override
        public MarshallerColumn[] values() {
            if (values == null) {
                values = schema.toMarshallerColumns(TuplePart.VAL);
            }
            return values;
        }

        @Override
        public MarshallerColumn[] row() {
            if (row == null) {
                row = schema.toMarshallerColumns(TuplePart.KEY_AND_VAL);
            }
            return row;
        }
    }
}
