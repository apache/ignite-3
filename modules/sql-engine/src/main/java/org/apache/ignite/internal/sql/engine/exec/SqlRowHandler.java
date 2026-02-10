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
import static org.apache.ignite.internal.sql.engine.util.Commons.readValue;
import static org.apache.ignite.sql.ColumnType.NULL;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory.RowBuilder;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
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
 * using the {@link #toBinaryTuple(RowWrapper)} method.
 *
 * <p>Factory method {@link RowFactory#create(InternalTuple)} allows to
 * create rows without any additional conversions. But the fields in
 * binary tuple must match the factory {@link StructNativeType row schema}.
 */
public class SqlRowHandler implements RowHandler<RowWrapper>, RowFactoryFactory<RowWrapper> {
    public static final SqlRowHandler INSTANCE = new SqlRowHandler();

    private static final ObjectsArrayRowWrapper EMPTY_ROW = new ObjectsArrayRowWrapper(NativeTypes.structBuilder().build(), new Object[0]);

    private SqlRowHandler() {
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object get(int field, RowWrapper row) {
        return row.get(field);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNull(int field, RowWrapper row) {
        return row.isNull(field);
    }

    @Override
    public int columnsCount(RowWrapper row) {
        return row.columnsCount();
    }

    @Override
    public String toString(RowWrapper row) {
        IgniteStringBuilder buf = new IgniteStringBuilder("Row[");
        int maxIdx = columnsCount(row) - 1;

        for (int i = 0; i <= maxIdx; i++) {
            buf.app(row.get(i));

            if (i != maxIdx) {
                buf.app(", ");
            }
        }

        return buf.app(']').toString();
    }

    @Override
    public BinaryTuple toBinaryTuple(RowWrapper row) {
        return row.toBinaryTuple();
    }

    @Override
    public RowFactory<RowWrapper> create(StructNativeType rowSchema) {
        int schemaLen = rowSchema.fields().size();

        return new RowFactory<>() {
            @Override
            public RowBuilder<RowWrapper> rowBuilder() {
                return new RowBuilderImpl(rowSchema);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create() {
                return create(new Object[schemaLen]);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(Object... fields) {
                assert fields.length == rowSchema.fields().size();

                return new ObjectsArrayRowWrapper(rowSchema, fields);
            }

            /** {@inheritDoc} */
            @Override
            public RowWrapper create(InternalTuple tuple) {
                assert schemaLen == tuple.elementCount() : format("schemaLen={}, tupleSize={}", schemaLen, tuple.elementCount());

                return new BinaryTupleRowWrapper(rowSchema, tuple);
            }

            /** {@inheritDoc} */
            @Override
            public StructNativeType rowSchema() {
                return rowSchema;
            }

            @Override
            public RowWrapper map(RowWrapper row, int[] mapping) {
                assert mapping.length == rowSchema.fields().size();
                Object[] fields = new Object[mapping.length];

                for (int i = 0; i < mapping.length; i++) {
                    fields[i] = row.get(mapping[i]);
                }

                return new ObjectsArrayRowWrapper(rowSchema, fields);
            }
        };
    }

    /**
     * Provides the ability for a single {@link RowHandler} instance to interact with multiple row implementations.
     */
    public abstract static class RowWrapper {
        abstract int columnsCount();

        abstract @Nullable Object get(int field);

        abstract boolean isNull(int field);

        abstract BinaryTuple toBinaryTuple();
    }

    /**
     * Wrapper over an array of objects.
     */
    private static class ObjectsArrayRowWrapper extends RowWrapper {
        private final StructNativeType rowType;
        private final Object[] row;

        ObjectsArrayRowWrapper(StructNativeType rowType, Object[] row) {
            assert row.length == rowType.fields().size();
            this.rowType = rowType;
            this.row = row;
        }

        @Override
        int columnsCount() {
            return row.length;
        }

        @Override
        @Nullable
        Object get(int field) {
            return row[field];
        }

        @Override
        boolean isNull(int field) {
            return row[field] == null;
        }

        @Override
        BinaryTuple toBinaryTuple() {
            int estimatedSize = 0;
            boolean exactEstimate = true;
            for (int i = 0; i < row.length; i++) {
                NativeType nativeType = rowType.fields().get(i).type();

                if (nativeType.spec() == NULL) {
                    assert row[i] == null;

                    continue;
                }

                Object value = row[i];

                if (value == null) {
                    continue;
                }

                if (nativeType.fixedLength()) {
                    estimatedSize += nativeType.sizeInBytes();
                } else {
                    if (value instanceof String) {
                        // every character in the string may contain up to 4 bytes.
                        // Let's be optimistic here and reserve buffer only for the smallest
                        // possible variant

                        estimatedSize += ((String) value).length();
                        exactEstimate = false;
                    } else if (value instanceof ByteString) {
                        estimatedSize += ((ByteString) value).length();
                    } else {
                        assert (value instanceof BigDecimal) : "unexpected value " + value.getClass();

                        exactEstimate = false;
                    }
                }
            }

            BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(row.length, estimatedSize, exactEstimate);

            for (int i = 0; i < row.length; i++) {
                Object value = row[i];

                appendValue(tupleBuilder, rowType.fields().get(i).type(), value);
            }

            return new BinaryTuple(row.length, tupleBuilder.build());
        }

        private static void appendValue(BinaryTupleBuilder builder, NativeType type, @Nullable Object value) {
            if (value == null) {
                builder.appendNull();

                return;
            }

            value = TypeUtils.fromInternal(value, type.spec());

            assert value != null : type;

            switch (type.spec()) {
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

                case DECIMAL:
                    builder.appendDecimalNotNull((BigDecimal) value, ((DecimalNativeType) type).scale());
                    break;

                case UUID:
                    builder.appendUuidNotNull((UUID) value);
                    break;

                case BYTE_ARRAY:
                    builder.appendBytesNotNull((byte[]) value);
                    break;

                case STRING:
                    builder.appendStringNotNull((String) value);
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

                case DURATION:
                    builder.appendDuration((Duration) value);
                    break;

                case PERIOD:
                    builder.appendPeriod((Period) value);
                    break;

                default:
                    throw new UnsupportedOperationException("Unknown type " + type);
            }
        }
    }

    /**
     * Wrapper over an {@link BinaryTuple}.
     */
    private static class BinaryTupleRowWrapper extends RowWrapper {
        private final StructNativeType rowType;
        private final InternalTuple tuple;

        BinaryTupleRowWrapper(StructNativeType rowType, InternalTuple tuple) {
            this.rowType = rowType;
            this.tuple = tuple;
        }

        @Override
        int columnsCount() {
            return tuple.elementCount();
        }

        @Override
        @Nullable
        Object get(int field) {
            NativeType nativeType = rowType.fields().get(field).type();

            if (nativeType.spec() == NULL) {
                return null;
            }

            Object value = readValue(tuple, nativeType, field);

            if (value == null) {
                return null;
            }

            return TypeUtils.toInternal(value, nativeType.spec());
        }

        @Override
        boolean isNull(int field) {
            return tuple.hasNullValue(field);
        }

        @Override
        BinaryTuple toBinaryTuple() {
            if (tuple instanceof BinaryTuple) {
                return (BinaryTuple) tuple;
            }

            return new BinaryTuple(tuple.elementCount(), tuple.byteBuffer());
        }
    }

    private static class RowBuilderImpl implements RowBuilder<RowWrapper> {

        private final int schemaLen;

        private final StructNativeType rowType;

        Object[] data;

        int fieldIdx;

        RowBuilderImpl(StructNativeType rowType) {
            this.rowType = rowType;
            this.schemaLen = rowType.fields().size();
            fieldIdx = 0;
        }

        /** {@inheritDoc} */
        @Override
        public RowBuilder<RowWrapper> addField(Object value) {
            if (fieldIdx == 0 && data == null) {
                data = new Object[schemaLen];
            }

            checkIndex();

            data[fieldIdx++] = value;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public RowWrapper build() {
            checkState();

            return rowType.fields().isEmpty() ? EMPTY_ROW : new ObjectsArrayRowWrapper(rowType, data);
        }

        /** {@inheritDoc} */
        @Override
        public void reset() {
            data = null;
            fieldIdx = 0;
        }

        private void checkState() {
            if (schemaLen != 0 && data == null) {
                throw new IllegalStateException("Row has not been initialised");
            }
            if (fieldIdx != schemaLen) {
                throw new IllegalStateException(format("Row has not been fully built. Index: {}, fields: {}", fieldIdx, schemaLen));
            }
        }

        private void checkIndex() {
            if (fieldIdx >= schemaLen) {
                throw new IllegalStateException(format("Field index is out of bounds. Index: {}, fields: {}", fieldIdx, schemaLen));
            }
        }
    }
}
