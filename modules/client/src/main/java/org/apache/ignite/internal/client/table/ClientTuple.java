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
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Client tuple. Wraps {@link BinaryTupleReader} and allows mutability.
 */
public class ClientTuple extends MutableTupleBinaryTupleAdapter {
    /** Schema. */
    private final ClientColumn[] columns;

    private final ClientSchema schema;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param columns Columns.
     * @param tuple Tuple.
     */
    public ClientTuple(ClientSchema schema, ClientColumn[] columns, BinaryTupleReader tuple) {
        super(tuple, 0, columns.length, null);

        this.schema = schema;
        this.columns = columns;
    }

    @Override
    protected String schemaColumnName(int index) {
        return columns[index].name();
    }

    @Override
    protected int schemaColumnIndex(String columnName) {
        ClientColumn column = column(columnName);

        return column == null ? -1 : column.schemaIndex();
    }

    @Override
    protected ColumnType schemaColumnType(int columnIndex) {
        ClientColumn column = columns[columnIndex];

        return column.type();
    }

    @Override
    protected int schemaDecimalScale(int columnIndex) {
        return columns[columnIndex].scale();
    }

    @Nullable
    private ClientColumn column(String columnName) {
        return schema.columnSafe(IgniteNameUtils.parseSimpleName(columnName));
    }
}
