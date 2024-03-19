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

package org.apache.ignite.client.handler.requests.table;

import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.table.MutableTupleBinaryTupleAdapter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Server-side Tuple implementation, wraps binary data coming from the client to pass to the table internals. This tuple implementation
 * does not end up in the user's hands, so mutability is not supported.
 */
class ClientHandlerTuple extends MutableTupleBinaryTupleAdapter implements SchemaAware {
    private final SchemaDescriptor schema;

    private final boolean keyOnly;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param noValueSet No-value set.
     * @param tuple Tuple.
     * @param keyOnly Key only.
     */
    ClientHandlerTuple(SchemaDescriptor schema, @Nullable BitSet noValueSet, BinaryTupleReader tuple, boolean keyOnly) {
        super(tuple, tuple.elementCount(), noValueSet);

        assert tuple.elementCount() == (keyOnly ? schema.keyColumns().size() : schema.length()) : "Tuple element count mismatch";

        this.schema = schema;
        this.keyOnly = keyOnly;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor schema() {
        return binaryTuple() != null ? schema : null;
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, @Nullable Object value) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /** {@inheritDoc} */
    @Override
    protected String schemaColumnName(int binaryTupleIndex) {
        return schema.column(binaryTupleIndex).name();
    }

    /** {@inheritDoc} */
    @Override
    protected int binaryTupleIndex(String columnName) {
        Column column = schema.column(columnName);

        if (column == null) {
            return -1;
        }

        if (keyOnly) {
            return column.positionInKey();
        }

        return column.positionInRow();
    }

    /** {@inheritDoc} */
    @Override
    protected int binaryTupleIndex(int publicIndex) {
        return keyOnly
                ? schema.keyColumns().get(publicIndex).positionInRow()
                : super.binaryTupleIndex(publicIndex);
    }

    /** {@inheritDoc} */
    @Override
    protected int publicIndex(int binaryTupleIndex) {
        if (keyOnly) {
            var col = schema.keyColumns().get(binaryTupleIndex);
            return col.positionInKey();
        }

        return super.publicIndex(binaryTupleIndex);
    }

    /** {@inheritDoc} */
    @Override
    protected ColumnType schemaColumnType(int binaryTupleIndex) {
        NativeTypeSpec spec = column(binaryTupleIndex).type().spec();

        return ClientTableCommon.getColumnType(spec);
    }

    /** {@inheritDoc} */
    @Override
    protected int schemaDecimalScale(int binaryTupleIndex) {
        return ClientTableCommon.getDecimalScale(column(binaryTupleIndex).type());
    }

    private Column column(int binaryTupleIndex) {
        List<Column> columns = keyOnly ? schema.keyColumns() : schema.columns();
        return columns.get(binaryTupleIndex);
    }
}
