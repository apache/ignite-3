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

import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientColumnTypeConverter;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.NotNull;

public class ClientTuple2 extends MutableTupleBinaryTupleAdapter {
    private final ClientSchema schema;

    public ClientTuple2(ClientSchema schema) {
        super(null); // TODO: Accept BinaryTuple here.

        this.schema = schema;
    }

    @Override
    protected int schemaColumnCount() {
        return schema.columns().length;
    }

    @Override
    protected String schemaColumnName(int index) {
        Objects.checkIndex(index, schema.columns().length);

        return schema.columns()[index].name();
    }

    @Override
    protected int schemaColumnIndex(@NotNull String columnName) {
        return schema.column(columnName).schemaIndex();
    }

    @Override
    protected int schemaColumnIndex(@NotNull String columnName, ColumnType type) {
        ClientColumn column = schema.column(columnName);

        var actualType = ClientColumnTypeConverter.ordinalToColumnType(column.type());

        if (type != actualType) {
            throw new ColumnNotFoundException("Column '" + columnName + "' has type " + actualType +
                " but " + type + " was requested");
        }

        return column.schemaIndex();
    }

    @Override
    protected int validateSchemaColumnIndex(int columnIndex, ColumnType type) {
        Objects.checkIndex(columnIndex, schema.columns().length);
        ClientColumn column = schema.columns()[columnIndex];

        var actualType = ClientColumnTypeConverter.ordinalToColumnType(column.type());

        if (type != actualType) {
            throw new ColumnNotFoundException("Column with index " + columnIndex + " has type " + actualType +
                    " but " + type + " was requested");
        }

        return column.schemaIndex();
    }

    @Override
    protected ColumnType schemaColumnType(int columnIndex) {
        Objects.checkIndex(columnIndex, schema.columns().length);
        ClientColumn column = schema.columns()[columnIndex];

        return ClientColumnTypeConverter.ordinalToColumnType(column.type());
    }

    @Override
    protected int schemaDecimalScale(int columnIndex) {
        Objects.checkIndex(columnIndex, schema.columns().length);
        ClientColumn column = schema.columns()[columnIndex];

        return column.scale();
    }
}
