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
import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema.Builder;
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
        Builder builder = RowSchema.builder();

        List<TypeSpec> leftFields = left.rowSchema().fields();

        for (TypeSpec typeSpec : leftFields) {
            builder.addField(typeSpec);
        }

        List<TypeSpec> rightFields = right.rowSchema().fields();

        for (TypeSpec typeSpec : rightFields) {
            builder.addField(typeSpec);
        }

        return new SqlArrayRow(arr, builder.build());
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

        return new RowFactory<>() {
            /** {@inheritDoc} */
            @Override
            public RowHandler<SqlRowWrapper> handler() {
                return SqlRowHandler.this;
            }

            /** {@inheritDoc} */
            @Override
            public SqlRowWrapper create() {
                return new SqlArrayRow(new Object[rowLen], rowSchema);
            }

            /** {@inheritDoc} */
            @Override
            public SqlRowWrapper create(Object... fields) {
                assert fields.length == rowLen;

                return new SqlArrayRow(fields, rowSchema);
            }

            /** {@inheritDoc} */
            @Override
            public SqlRowWrapper create(ByteBuffer buf) {
                return new SqlBinaryRowWrapper(rowSchema, buf);
            }

            @Override
            public SqlRowWrapper wrap(InternalTuple row) {
                return new SqlBinaryRowWrapper(rowSchema, row);
            }
        };
    }
}
