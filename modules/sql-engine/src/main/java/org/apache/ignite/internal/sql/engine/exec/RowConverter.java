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
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class provides method to convert binary tuple to rows and vice-versa.
 */
public final class RowConverter {
    /**
     * Creates binary tuple schema for index rows.
     */
    public static BinaryTupleSchema createIndexRowSchema(List<String> indexedColumns, TableDescriptor tableDescriptor) {
        Element[] elements = indexedColumns.stream()
                .map(tableDescriptor::columnDescriptor)
                .map(colDesc -> new Element(colDesc.physicalType(), true))
                .toArray(Element[]::new);

        return BinaryTupleSchema.create(elements);
    }

    /**
     * Converts a search row, which represents prefix condition, to a binary tuple.
     *
     * @param ectx Execution context.
     * @param binarySchema Binary tuple schema.
     * @param factory Row handler factory.
     * @param searchRow Search row.
     * @param <RowT> Row type.
     * @return Binary tuple.
     */
    public static <RowT> BinaryTuplePrefix toBinaryTuplePrefix(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            RowHandler.RowFactory<RowT> factory,
            RowT searchRow
    ) {
        RowHandler<RowT> handler = factory.handler();

        int indexedColumnsCount = binarySchema.elementCount();
        int prefixColumnsCount = handler.columnCount(searchRow);

        assert prefixColumnsCount == indexedColumnsCount : "Invalid range condition";

        int specifiedCols = 0;
        for (int i = 0; i < prefixColumnsCount; i++) {
            if (handler.get(i, searchRow) == UNSPECIFIED_VALUE_PLACEHOLDER) {
                break;
            }

            specifiedCols++;
        }

        BinaryTuplePrefixBuilder tupleBuilder = new BinaryTuplePrefixBuilder(specifiedCols, indexedColumnsCount);

        return new BinaryTuplePrefix(indexedColumnsCount, toByteBuffer(binarySchema, handler, tupleBuilder, specifiedCols, searchRow));
    }

    /**
     * Converts a search row, which represents exact value condition, to a binary tuple.
     *
     * @param ectx Execution context.
     * @param binarySchema Binary tuple schema.
     * @param factory Row handler factory.
     * @param searchRow Search row.
     * @param <RowT> Row type.
     * @return Binary tuple.
     */
    public static <RowT> BinaryTuple toBinaryTuple(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            RowHandler.RowFactory<RowT> factory,
            RowT searchRow
    ) {
        RowHandler<RowT> handler = factory.handler();

        int rowColumnsCount = handler.columnCount(searchRow);

        assert rowColumnsCount == binarySchema.elementCount() : "Invalid lookup key.";

        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(rowColumnsCount);

        return new BinaryTuple(rowColumnsCount, toByteBuffer(binarySchema, handler, tupleBuilder, rowColumnsCount, searchRow));
    }

    /**
     * Converts an array of objects to a byte buffer representation of a {@link BinaryTuple binary tuple}. This method uses
     * {@link RowSchema} to resolve object types.
     *
     * @param schema Row schema.
     * @param values Array of objects to be stored in the buffer.
     * @return Buffer in binary tuple format.
     */
    static ByteBuffer toByteBuffer(RowSchema schema, Object[] values) {
        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(values.length);

        for (int i = 0; i < values.length; i++) {
            Object value = values[i];

            assert value != UNSPECIFIED_VALUE_PLACEHOLDER : "Invalid row value.";

            appendValue(tupleBuilder, schema.fields().get(i), value);
        }

        return tupleBuilder.build();
    }

    private static <RowT> ByteBuffer toByteBuffer(
            BinaryTupleSchema binarySchema,
            RowHandler<RowT> handler,
            BinaryTupleBuilder tupleBuilder,
            int columnsCount,
            RowT searchRow
    ) {
        for (int i = 0; i < columnsCount; i++) {
            Object val = handler.get(i, searchRow);

            assert val != UNSPECIFIED_VALUE_PLACEHOLDER : "Invalid lookup key.";

            Element element = binarySchema.element(i);

            val = TypeUtils.fromInternal(val, NativeTypeSpec.toClass(element.typeSpec(), element.nullable()));

            BinaryRowConverter.appendValue(tupleBuilder, element, val);
        }

        return tupleBuilder.build();
    }

    private static BinaryTupleBuilder appendValue(BinaryTupleBuilder builder, TypeSpec schemaType, @Nullable Object value) {
        if (value == null) {
            return builder.appendNull();
        }

        NativeType nativeType = RowSchemaTypes.toNativeType(schemaType);

        value = TypeUtils.fromInternal(value, NativeTypeSpec.toClass(nativeType.spec(), schemaType.isNullable()));

        assert value != null : nativeType;

        switch (nativeType.spec()) {
            case BOOLEAN:
                return builder.appendBoolean((boolean) value);
            case INT8:
                return builder.appendByte((byte) value);
            case INT16:
                return builder.appendShort((short) value);
            case INT32:
                return builder.appendInt((int) value);
            case INT64:
                return builder.appendLong((long) value);
            case FLOAT:
                return builder.appendFloat((float) value);
            case DOUBLE:
                return builder.appendDouble((double) value);
            case NUMBER:
                return builder.appendNumberNotNull((BigInteger) value);
            case DECIMAL:
                return builder.appendDecimalNotNull((BigDecimal) value, ((DecimalNativeType) nativeType).scale());
            case UUID:
                return builder.appendUuidNotNull((UUID) value);
            case BYTES:
                return builder.appendBytesNotNull((byte[]) value);
            case STRING:
                return builder.appendStringNotNull((String) value);
            case BITMASK:
                return builder.appendBitmaskNotNull((BitSet) value);
            case DATE:
                return builder.appendDateNotNull((LocalDate) value);
            case TIME:
                return builder.appendTimeNotNull((LocalTime) value);
            case DATETIME:
                return builder.appendDateTimeNotNull((LocalDateTime) value);
            case TIMESTAMP:
                return builder.appendTimestampNotNull((Instant) value);
            default:
                throw new UnsupportedOperationException("Unknown type " + nativeType);
        }
    }
}
