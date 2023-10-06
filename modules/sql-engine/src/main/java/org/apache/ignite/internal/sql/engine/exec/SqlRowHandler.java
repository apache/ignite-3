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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl.UNSPECIFIED_VALUE_PLACEHOLDER;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema.Builder;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
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
    public RowWrapper map(RowWrapper row, int[] mapping) {
        Object[] fields = new Object[mapping.length];

        for (int i = 0; i < mapping.length; i++) {
            fields[i] = row.get(mapping[i]);
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
        int schemaLen = rowSchema.fields().size();

        return new RowFactory<>() {
            /** {@inheritDoc} */
            @Override
            public RowHandler<RowWrapper> handler() {
                return SqlRowHandler.this;
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create() {
                return create(new Object[schemaLen]);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(Object... fields) {
                assert fields.length == schemaLen;

                return new ObjectsArrayRowWrapper(rowSchema, fields);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(ByteBuffer buf) {
                return create(new BinaryTuple(schemaLen, buf));
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(InternalTuple tuple) {
                assert schemaLen == tuple.elementCount() : format("schemaLen={}, tupleSize={}", schemaLen, tuple.elementCount());

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
            BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(row.length);

            for (int i = 0; i < row.length; i++) {
                Object value = row[i];

                assert value != UNSPECIFIED_VALUE_PLACEHOLDER : "Invalid row value.";

                appendValue(tupleBuilder, rowSchema.fields().get(i), value);
            }

            return tupleBuilder.build();
        }

        @Override
        RowSchema rowSchema() {
            return rowSchema;
        }

        private void appendValue(BinaryTupleBuilder builder, TypeSpec schemaType, @Nullable Object value) {
            if (value == null) {
                builder.appendNull();

                return;
            }

            NativeType nativeType = RowSchemaTypes.toNativeType(schemaType);

            value = TypeUtils.fromInternal(value, NativeTypeSpec.toClass(nativeType.spec(), schemaType.isNullable()));

            assert value != null : nativeType;

            switch (nativeType.spec()) {
                case BOOLEAN:
                    builder.appendBoolean((boolean) value);
                    break;

                case INT8:
                    builder.appendByte((byte) value);
                    break;

                case INT16:
                    builder.appendShort((short) value);
                    break;

                case INT32:
                    builder.appendInt((int) value);
                    break;

                case INT64:
                    builder.appendLong((long) value);
                    break;

                case FLOAT:
                    builder.appendFloat((float) value);
                    break;

                case DOUBLE:
                    builder.appendDouble((double) value);
                    break;

                case NUMBER:
                    builder.appendNumberNotNull((BigInteger) value);
                    break;

                case DECIMAL:
                    builder.appendDecimalNotNull((BigDecimal) value, ((DecimalNativeType) nativeType).scale());
                    break;

                case UUID:
                    builder.appendUuidNotNull((UUID) value);
                    break;

                case BYTES:
                    builder.appendBytesNotNull((byte[]) value);
                    break;

                case STRING:
                    builder.appendStringNotNull((String) value);
                    break;

                case BITMASK:
                    builder.appendBitmaskNotNull((BitSet) value);
                    break;

                case DATE:
                    builder.appendDateNotNull((LocalDate) value);
                    break;

                case TIME:
                    builder.appendTimeNotNull((LocalTime) value);
                    break;

                case DATETIME:
                    builder.appendDateTimeNotNull((LocalDateTime) value);
                    break;

                case TIMESTAMP:
                    builder.appendTimestampNotNull((Instant) value);
                    break;

                default:
                    throw new UnsupportedOperationException("Unknown type " + nativeType);
            }
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
            NativeType nativeType = RowSchemaTypes.toNativeType(rowSchema.fields().get(field));

            if (nativeType == null) {
                return null;
            }

            Object value = readValue(tuple, nativeType, field);

            if (value == null) {
                return null;
            }

            return TypeUtils.toInternal(value, Commons.nativeTypeToClass(nativeType));
        }

        @Override
        void set(int field, @Nullable Object value) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-20356
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

        private @Nullable Object readValue(InternalTuple tuple, NativeType nativeType, int fieldIndex) {
            switch (nativeType.spec()) {
                case BOOLEAN: return tuple.booleanValueBoxed(fieldIndex);
                case INT8: return tuple.byteValueBoxed(fieldIndex);
                case INT16: return tuple.shortValueBoxed(fieldIndex);
                case INT32: return tuple.intValueBoxed(fieldIndex);
                case INT64: return tuple.longValueBoxed(fieldIndex);
                case FLOAT: return tuple.floatValueBoxed(fieldIndex);
                case DOUBLE: return tuple.doubleValueBoxed(fieldIndex);
                case DECIMAL: return tuple.decimalValue(fieldIndex, ((DecimalNativeType) nativeType).scale());
                case UUID: return tuple.uuidValue(fieldIndex);
                case STRING: return tuple.stringValue(fieldIndex);
                case BYTES: return tuple.bytesValue(fieldIndex);
                case BITMASK: return tuple.bitmaskValue(fieldIndex);
                case NUMBER: return tuple.numberValue(fieldIndex);
                case DATE: return tuple.dateValue(fieldIndex);
                case TIME: return tuple.timeValue(fieldIndex);
                case DATETIME: return tuple.dateTimeValue(fieldIndex);
                case TIMESTAMP: return tuple.timestampValue(fieldIndex);
                default: throw new InvalidTypeException("Unknown element type: " + nativeType);
            }
        }
    }
}
