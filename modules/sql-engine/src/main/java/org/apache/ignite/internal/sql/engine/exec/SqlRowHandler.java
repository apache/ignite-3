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
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema.Builder;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Handler that uses a {@link RowWrapper wrapper} to operate with two types of row implementations.
 *
 * <ul>
 *     <li>{@link ObjectsArrayRowWrapper Objects array}</li>
 *     <li>{@link BinaryTupleRowWrapper Binary tuple}</li>
 * </ul>
 *
 * <p>Each kind of rows is serialized to the same binary tuple format
 * using the {@link #toByteBuffer(RowWrapper) toByteBuffer} method.
 *
 * <p>Factory methods {@link RowFactory#create(InternalTuple) wrap(InternalTuple)} and
 * {@link RowFactory#create(ByteBuffer) create(ByteBuffer)} allow create rows without
 * any additional conversions. But the fields in binary tuple must match the
 * factory {@link RowSchema row schema}.
 */
public class SqlRowHandler implements RowHandler<RowWrapper> {
    public static final RowHandler<RowWrapper> INSTANCE = new SqlRowHandler();

    private SqlRowHandler() {
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object get(int field, RowWrapper row) {
        return row.get(field);
    }

    /** {@inheritDoc} */
    @Override
    public void set(int field, RowWrapper row, @Nullable Object val) {
        row.set(field, val);
    }

    /** {@inheritDoc} */
    @Override
    public RowWrapper concat(RowWrapper left, RowWrapper right) {
        int leftLen = left.columnsCount();
        int rightLen = right.columnsCount();
        List<TypeSpec> leftTypes = left.rowSchema().fields();
        List<TypeSpec> rightTypes = right.rowSchema().fields();

        Object[] values = new Object[leftLen + rightLen];
        Builder schemaBuilder = RowSchema.builder();

        for (int i = 0; i < leftLen; i++) {
            values[i] = left.get(i);
            schemaBuilder.addField(leftTypes.get(i));
        }

        for (int i = 0; i < rightLen; i++) {
            values[leftLen + i] = right.get(i);
            schemaBuilder.addField(rightTypes.get(i));
        }

        return new ObjectsArrayRowWrapper(schemaBuilder.build(), values);
    }

    /** {@inheritDoc} */
    @Override
    public RowWrapper map(RowWrapper row, int[] mapping, int offset) {
        Object[] fields = new Object[mapping.length];

        for (int i = 0; i < mapping.length; i++) {
            fields[i] = row.get(mapping[i] + offset);
        }

        return new ObjectsArrayRowWrapper(row.rowSchema(), fields);
    }

    @Override
    public int columnCount(RowWrapper row) {
        return row.columnsCount();
    }

    @Override
    public String toString(RowWrapper row) {
        IgniteStringBuilder buf = new IgniteStringBuilder("Row[");
        int maxIdx = columnCount(row) - 1;

        for (int i = 0; i <= maxIdx; i++) {
            buf.app(row.get(i));

            if (i != maxIdx) {
                buf.app(", ");
            }
        }

        return buf.app(']').toString();
    }

    @Override
    public ByteBuffer toByteBuffer(RowWrapper row) {
        return row.toByteBuffer();
    }

    @Override
    public RowFactory<RowWrapper> factory(RowSchema rowSchema) {
        int rowLen = rowSchema.fields().size();

        return new RowFactory<>() {
            /** {@inheritDoc} */
            @Override
            public RowHandler<RowWrapper> handler() {
                return SqlRowHandler.this;
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create() {
                return create(new Object[rowLen]);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(Object... fields) {
                assert fields.length == rowLen;

                return new ObjectsArrayRowWrapper(rowSchema, fields);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(ByteBuffer buf) {
                return create(new BinaryTuple(rowLen, buf));
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(InternalTuple tuple) {
                assert rowLen == tuple.elementCount();

                return new BinaryTupleRowWrapper(rowSchema, tuple);
            }
        };
    }

    /**
     * Provides the ability for a single {@link RowHandler} instance to interact with multiple row implementations.
     */
    public abstract static class RowWrapper {
        abstract int columnsCount();

        abstract RowSchema rowSchema();

        abstract @Nullable Object get(int field);

        abstract void set(int field, Object value);

        abstract ByteBuffer toByteBuffer();
    }

    /**
     * Wrapper over an array of objects.
     */
    private static class ObjectsArrayRowWrapper extends RowWrapper {
        private final RowSchema rowSchema;
        private final Object[] row;

        ObjectsArrayRowWrapper(RowSchema rowSchema, Object[] row) {
            this.rowSchema = rowSchema;
            this.row = row;
        }

        @Override
        int columnsCount() {
            return row.length;
        }

        @Override
        @Nullable Object get(int field) {
            return row[field];
        }

        @Override
        void set(int field, @Nullable Object value) {
            row[field] = value;
        }

        @Override
        ByteBuffer toByteBuffer() {
            return RowConverter.toByteBuffer(rowSchema, row);
        }

        @Override
        RowSchema rowSchema() {
            return rowSchema;
        }
    }

    /**
     * Wrapper over an {@link BinaryTuple}.
     *
     * <p>Since {@link BinaryTuple binary tuple} is immutable this wrapper doesn't support {@link #set(int, Object)} operation.
     */
    private static class BinaryTupleRowWrapper extends RowWrapper {
        private final RowSchema rowSchema;
        private final InternalTuple tuple;

        BinaryTupleRowWrapper(RowSchema rowSchema, InternalTuple tuple) {
            this.rowSchema = rowSchema;
            this.tuple = tuple;
        }

        @Override
        int columnsCount() {
            return tuple.elementCount();
        }

        @Override
        @Nullable Object get(int field) {
            Object value = rowSchema.value(tuple, field);

            if (value == null) {
                return null;
            }

            NativeType nativeType = RowSchemaTypes.toNativeType(rowSchema.fields().get(field));

            return TypeUtils.toInternal(value, Commons.nativeTypeToClass(nativeType));
        }

        @Override
        void set(int field, @Nullable Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        ByteBuffer toByteBuffer() {
            return tuple.byteBuffer();
        }

        @Override
        RowSchema rowSchema() {
            return rowSchema;
        }
    }
}
