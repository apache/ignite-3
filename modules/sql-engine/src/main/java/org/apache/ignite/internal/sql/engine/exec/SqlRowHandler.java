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

package org.apache.ignite.internal.sql.engine.exec;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.NullTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowType;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.jetbrains.annotations.Nullable;

/**
 * TODO Sql row handler.
 */
public class SqlRowHandler implements RowHandler<SqlRowWrapper> {
    public static final RowHandler<SqlRowWrapper> INSTANCE = new SqlRowHandler();

    private SqlRowHandler() {
    }

    @Override
    public @Nullable Object get(int field, SqlRowWrapper row) {
        return row.get(field);
    }

    @Override
    public void set(int field, SqlRowWrapper row, @Nullable Object val) {
        row.set(field, val);
    }

    @Override
    public SqlRowWrapper concat(SqlRowWrapper left, SqlRowWrapper right) {
        int leftLen = left.columnsCount();
        int rightLen = right.columnsCount();

        // TODO Use arraycopy for arrays
        Object[] arr = new Object[leftLen + rightLen];

        for (int i = 0; i < leftLen; i++) {
            arr[i] = left.get(i);
        }

        for (int i = 0; i < rightLen; i++) {
            arr[leftLen + i] = right.get(i);
        }

        // todo do we need any schema
        return new SqlArrayRow(arr, null);
    }

    @Override
    public int columnCount(SqlRowWrapper row) {
        return row.columnsCount();
    }

    @Override
    public String toString(SqlRowWrapper row) {
        return String.valueOf(row);
    }

    @Override
    public ByteBuffer toByteBuffer(SqlRowWrapper row) {
        return row.toByteBuffer();
    }

    @Override
    public RowFactory<SqlRowWrapper> factory(RowSchema rowSchema) {
        int rowLen = rowSchema.fields().size();

        Element[] elements = new Element[rowLen];

        for (int i = 0; i < rowLen; i++) {
            TypeSpec type = rowSchema.fields().get(i);

            elements[i] = new Element(toNativeType(rowSchema.fields().get(i)), type.isNullable());
        }

        BinaryTupleSchema schema = BinaryTupleSchema.create(elements);

        return new RowFactory<>() {
            /** {@inheritDoc} */
            @Override
            public RowHandler<SqlRowWrapper> handler() {
                return SqlRowHandler.this;
            }

            /** {@inheritDoc} */
            @Override
            public SqlRowWrapper create() {
                return new SqlArrayRow(new Object[rowLen], schema);
            }

            /** {@inheritDoc} */
            @Override
            public SqlRowWrapper create(Object... fields) {
                assert fields.length == rowLen;

                return new SqlArrayRow(fields, schema);
            }

            /** {@inheritDoc} */
            @Override
            public SqlRowWrapper create(ByteBuffer buf) {
                return new SqlBinaryRowWrapper(schema, buf);
            }

            @Override
            public SqlRowWrapper wrap(Row row, List<Integer> columns) {
                return new SqlBinaryRowWrapper(row.binaryTupleSchema(), row, columns);
            }
        };
    }

    private NativeType toNativeType(TypeSpec type) {
        if (type instanceof RowType) {
            // todo
            return toNativeType(((RowType) type).fields().get(0));
        }

        if (type instanceof BaseTypeSpec) {
            return ((BaseTypeSpec) type).nativeType();
        }

        // todo appendNull()
        if (type instanceof NullTypeSpec) {
            return NativeTypes.fromObject(Boolean.TRUE);
        }

        throw new UnsupportedOperationException("Not implemented for type " + type.getClass());
    }
}
