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

package org.apache.ignite.internal.sql.engine;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@code InternalSqlRow} allowing to avoid earlier unnecessary row deserialization, for example when we need to pass it
 * to wire.
 *
 * @param <RowT> Type of the sql row.
 */
public class InternalSqlRowImpl<RowT> implements InternalSqlRow {
    private final RowT row;
    private final RowHandler<RowT> rowHandler;
    private final SchemaAwareConverter<Object, Object> internalTypeConverter;

    /**
     * Constructor.
     *
     * @param row Sql row.
     * @param rowHandler Handler to deserialize given row.
     * @param internalTypeConverter Function to convert internal representation of column value into external type.
     */
    public InternalSqlRowImpl(RowT row, RowHandler<RowT> rowHandler, SchemaAwareConverter<Object, Object> internalTypeConverter) {
        this.row = row;
        this.rowHandler = rowHandler;
        this.internalTypeConverter = internalTypeConverter;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object get(int idx) {
        Object res = rowHandler.get(idx, row);
        return internalTypeConverter.convert(idx, res);
    }

    /** {@inheritDoc} */
    @Override
    public int fieldCount() {
        return rowHandler.columnsCount(row);
    }

    /** {@inheritDoc} */
    @Override
    public BinaryTuple asBinaryTuple() {
        return rowHandler.toBinaryTuple(row);
    }

}
