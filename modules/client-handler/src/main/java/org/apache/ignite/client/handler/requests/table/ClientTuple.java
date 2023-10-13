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
class ClientTuple extends MutableTupleBinaryTupleAdapter implements SchemaAware {
    /** Schema. */
    private final SchemaDescriptor schema;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param noValueSet No-value set.
     * @param tuple Tuple.
     * @param schemaOffset Schema offset.
     * @param schemaSize Schema size.
     */
    ClientTuple(SchemaDescriptor schema, BitSet noValueSet, BinaryTupleReader tuple, int schemaOffset, int schemaSize) {
        super(tuple, schemaOffset, schemaSize, noValueSet);

        this.schema = schema;
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
    protected int schemaColumnIndex(String columnName) {
        Column column = schema.column(columnName);
        return column == null ? -1 : column.schemaIndex();
    }

    /** {@inheritDoc} */
    @Override
    protected ColumnType schemaColumnType(int columnIndex) {
        NativeTypeSpec spec = schema.column(columnIndex).type().spec();

        return ClientTableCommon.getColumnType(spec);
    }

    /** {@inheritDoc} */
    @Override
    protected int schemaDecimalScale(int columnIndex) {
        return ClientTableCommon.getDecimalScale(schema.column(columnIndex).type());
    }
}
