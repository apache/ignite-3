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

package org.apache.ignite.internal.client.sql;

import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.table.MutableTupleBinaryTupleAdapter;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;

/**
 * Client SQL row.
 */
public class ClientSqlRow extends MutableTupleBinaryTupleAdapter implements SqlRow {
    /** Meta. */
    private final ResultSetMetadata metadata;

    /**
     * Constructor.
     *
     * @param row Row.
     * @param meta Meta.
     */
    ClientSqlRow(BinaryTupleReader row, ResultSetMetadata meta) {
        super(row, meta.columns().size(), null);

        assert row != null;
        assert meta != null;

        this.metadata = meta;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount() {
        return metadata.columns().size();
    }

    /** {@inheritDoc} */
    @Override
    public String columnName(int columnIndex) {
        return metadata.columns().get(columnIndex).name();
    }

    /** {@inheritDoc} */
    @Override
    public int columnIndex(String columnName) {
        return metadata.indexOf(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple set(String columnName, Object value) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /** {@inheritDoc} */
    @Override
    protected String schemaColumnName(int binaryTupleIndex) {
        return columnName(binaryTupleIndex);
    }

    /** {@inheritDoc} */
    @Override
    protected int binaryTupleIndex(String columnName) {
        return columnIndex(columnName);
    }

    /** {@inheritDoc} */
    @Override
    protected ColumnType schemaColumnType(int binaryTupleIndex) {
        return metadata.columns().get(binaryTupleIndex).type();
    }

    /** {@inheritDoc} */
    @Override
    protected int schemaDecimalScale(int binaryTupleIndex) {
        return metadata.columns().get(binaryTupleIndex).scale();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return metadata;
    }
}
