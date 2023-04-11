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
import org.apache.ignite.internal.client.proto.ClientColumnTypeConverter;
import org.apache.ignite.internal.client.table.MutableTupleBinaryTupleAdapter;
import org.apache.ignite.internal.schema.BinaryTupleContainer;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Server-side client Tuple.
 */
class ClientTuple extends MutableTupleBinaryTupleAdapter implements BinaryTupleContainer {
    private final SchemaDescriptor schema;

    private final BitSet noValueSet;

    ClientTuple(SchemaDescriptor schema, BitSet noValueSet, BinaryTupleReader tuple, int schemaOffset, int schemaSize) {
        super(tuple, schemaOffset, schemaSize);

        this.schema = schema;
        this.noValueSet = noValueSet;
    }

    @Override
    public @Nullable BinaryTupleReader binaryTuple() {
        return super.binaryTuple();
    }

    @Override
    public Tuple set(@NotNull String columnName, @Nullable Object value) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    @Override
    protected String schemaColumnName(int internalIndex) {
        return schema.column(internalIndex).name();
    }

    @Override
    protected int schemaColumnIndex(@NotNull String columnName) {
        Column column = schema.column(columnName);
        return column == null ? -1 : column.schemaIndex();
    }

    @Override
    protected ColumnType schemaColumnType(int columnIndex) {
        // TODO: There must be a direct conversion - asked in Slack.
        NativeTypeSpec spec = schema.column(columnIndex).type().spec();
        int clientTypeCode = ClientTableCommon.getClientDataType(spec);

        return ClientColumnTypeConverter.clientDataTypeToSqlColumnType(clientTypeCode);
    }

    @Override
    protected int schemaDecimalScale(int columnIndex) {
        return ClientTableCommon.getDecimalScale(schema.column(columnIndex).type());
    }
}
