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
 * Server-side client Tuple.
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
    ClientHandlerTuple(SchemaDescriptor schema, BitSet noValueSet, BinaryTupleReader tuple, boolean keyOnly) {
        super(tuple, tuple.elementCount(), noValueSet);

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
    protected String schemaColumnName(int internalIndex) {
        return schema.column(internalIndex).name();
    }

    /** {@inheritDoc} */
    @Override
    protected int internalIndex(String columnName) {
        Column column = schema.column(columnName);
        return column == null ? -1 : column.schemaIndex();
    }

    /** {@inheritDoc} */
    @Override
    protected int internalIndex(int publicIndex) {
        return keyOnly
                ? schema.keyColumns().column(publicIndex).schemaIndex()
                : super.internalIndex(publicIndex);
    }

    /** {@inheritDoc} */
    @Override
    protected int publicIndex(int internalIndex) {
        if (keyOnly) {
            var col = schema.keyColumns().column(internalIndex);
            return schema.keyIndex(col);
        }

        return super.publicIndex(internalIndex);
    }

    /** {@inheritDoc} */
    @Override
    protected ColumnType schemaColumnType(int internalIndex) {
        NativeTypeSpec spec = schema.column(internalIndex).type().spec();

        return ClientTableCommon.getColumnType(spec);
    }

    /** {@inheritDoc} */
    @Override
    protected int schemaDecimalScale(int internalIndex) {
        return ClientTableCommon.getDecimalScale(schema.column(internalIndex).type());
    }
}
