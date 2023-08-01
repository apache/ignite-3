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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Helper class provides method to convert binary tuple to rows and vice-versa.
 */
public final class RowConverter {
    /**
     * Creates binary tuple schema for index rows.
     */
    public static BinaryTupleSchema createIndexRowSchema(List<Integer> indexedColumns, TableDescriptor tableDescriptor) {
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

        return new BinaryTuplePrefix(indexedColumnsCount, toByteBuffer(ectx, binarySchema, handler, tupleBuilder, searchRow));
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

        if (IgniteUtils.assertionsEnabled()) {
            for (int i = 0; i < rowColumnsCount; i++) {
                if (handler.get(i, searchRow) == UNSPECIFIED_VALUE_PLACEHOLDER) {
                    throw new AssertionError("Invalid lookup key.");
                }
            }
        }

        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(rowColumnsCount);

        return new BinaryTuple(rowColumnsCount, toByteBuffer(ectx, binarySchema, handler, tupleBuilder, searchRow));
    }

    private static <RowT> ByteBuffer toByteBuffer(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            RowHandler<RowT> handler,
            BinaryTupleBuilder tupleBuilder,
            RowT searchRow
    ) {
        int columnsCount = handler.columnCount(searchRow);

        for (int i = 0; i < columnsCount; i++) {
            Object val = handler.get(i, searchRow);

            if (val == UNSPECIFIED_VALUE_PLACEHOLDER) {
                break; // No more columns in prefix.
            }

            Element element = binarySchema.element(i);

            val = TypeUtils.fromInternal(val, NativeTypeSpec.toClass(element.typeSpec(), element.nullable()));

            BinaryRowConverter.appendValue(tupleBuilder, element, val);
        }

        return tupleBuilder.build();
    }
}
