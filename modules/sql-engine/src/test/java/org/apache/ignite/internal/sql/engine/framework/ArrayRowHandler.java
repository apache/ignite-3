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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Handler for rows that implemented as a simple objects array.
 */
public class ArrayRowHandler implements RowHandler<Object[]> {
    public static final RowHandler<Object[]> INSTANCE = new ArrayRowHandler();

    private ArrayRowHandler() {
    }

    /** {@inheritDoc} */
    @Override
    public Object get(int field, Object[] row) {
        return row[field];
    }

    /** {@inheritDoc} */
    @Override
    public Object[] concat(Object[] left, Object[] right) {
        return ArrayUtils.concat(left, right);
    }

    /** {@inheritDoc} */
    @Override
    public Object[] map(Object[] row, int[] mapping) {
        Object[] newRow = new Object[mapping.length];

        for (int i = 0; i < mapping.length; i++) {
            newRow[i] = row[mapping[i]];
        }

        return newRow;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount(Object[] row) {
        return row.length;
    }

    @Override
    public BinaryTuple toBinaryTuple(Object[] row) {
        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(row.length);

        for (Object value : row) {
            appendValue(tupleBuilder, value);
        }

        return new BinaryTuple(row.length, tupleBuilder.build());
    }

    /** {@inheritDoc} */
    @Override
    public String toString(Object[] objects) {
        return "Row" + Arrays.toString(objects);
    }

    /** {@inheritDoc} */
    @Override
    public RowFactory<Object[]> factory(RowSchema rowSchema) {
        int schemaLen = rowSchema.fields().size();

        return new RowFactory<>() {
            /** {@inheritDoc} */
            @Override
            public RowHandler<Object[]> handler() {
                return ArrayRowHandler.this;
            }

            @Override
            public RowBuilder<Object[]> rowBuilder() {
                return new RowBuilderImpl(schemaLen);
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create() {
                return new Object[schemaLen];
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create(Object... fields) {
                assert fields.length == schemaLen;

                return fields;
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create(InternalTuple tuple) {
                Object[] row = new Object[tuple.elementCount()];

                for (int i = 0; i < row.length; i++) {
                    NativeType nativeType = RowSchemaTypes.toNativeType(rowSchema.fields().get(i));

                    if (nativeType == null) {
                        row[i] = null;

                        continue;
                    }

                    row[i] = readValue(tuple, nativeType, i);
                }

                return row;
            }
        };
    }

    private static void appendValue(BinaryTupleBuilder builder, @Nullable Object value) {
        if (value == null) {
            builder.appendNull();

            return;
        }

        NativeType nativeType = NativeTypes.fromObject(value);

        assert nativeType != null;

        value = TypeUtils.fromInternal(value, NativeTypeSpec.toClass(nativeType.spec(), true));

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

    private static @Nullable Object readValue(InternalTuple tuple, NativeType nativeType, int fieldIndex) {
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

    private static class RowBuilderImpl implements RowBuilder<Object[]> {

        private final int schemaLen;

        Object[] data;

        int fieldIdx;

        RowBuilderImpl(int schemaLen) {
            this.schemaLen = schemaLen;
            fieldIdx = 0;
        }

        /** {@inheritDoc} */
        @Override
        public void reset() {
            this.data = null;
            fieldIdx = 0;
        }

        /** {@inheritDoc} */
        @Override
        public RowBuilder<Object[]> addField(Object value) {
            if (fieldIdx == 0 && data == null) {
                data = new Object[schemaLen];
            }

            checkIndex();

            data[fieldIdx++] = value;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public Object[] build() {
            checkState();

            if (schemaLen == 0) {
                return new Object[0];
            } else {
                return data;
            }
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
