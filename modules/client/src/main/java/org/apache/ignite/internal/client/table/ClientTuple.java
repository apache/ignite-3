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

import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientColumnTypeConverter;
import org.apache.ignite.internal.util.IgniteNameUtils;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client tuple. Wraps {@link BinaryTupleReader} and allows mutability.
 */
public class ClientTuple extends MutableTupleBinaryTupleAdapter {
    private final ClientSchema schema;

    ClientTuple(ClientSchema schema, BinaryTupleReader tuple, int schemaOffset, int schemaSize) {
        super(tuple, schemaOffset, schemaSize);

        this.schema = schema;
    }

    @Override
    protected String schemaColumnName(int index) {
        return schema.columns()[index].name();
    }

    @Override
    protected int schemaColumnIndex(@NotNull String columnName, @Nullable ColumnType type) {
        ClientColumn column = column(columnName);

        if (column == null)
            return -1;

        if (type != null) {
            var actualType = ClientColumnTypeConverter.clientDataTypeToSqlColumnType(column.type());

            if (type != actualType) {
                throw new ColumnNotFoundException("Column '" + columnName + "' has type " + actualType +
                        " but " + type + " was requested");
            }
        }

        return column.schemaIndex();
    }

    @Override
    protected ColumnType schemaColumnType(int columnIndex) {
        ClientColumn column = schema.columns()[columnIndex];

        return ClientColumnTypeConverter.clientDataTypeToSqlColumnType(column.type());
    }

    @Override
    protected int schemaDecimalScale(int columnIndex) {
        ClientColumn column = schema.columns()[columnIndex];

        return column.scale();
    }

    @Nullable
    private ClientColumn column(@NotNull String columnName) {
        return schema.columnSafe(IgniteNameUtils.parseSimpleName(columnName));
    }
}
