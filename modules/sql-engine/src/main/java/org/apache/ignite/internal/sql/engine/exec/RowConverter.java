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
import java.util.stream.IntStream;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

/**
 * Helper class provides method to convert binary tuple to rows and vice-versa.
 */
public final class RowConverter {
    /**
     * Creates binary tuple schema for index rows.
     */
    public static BinaryTupleSchema createIndexRowSchema(TableDescriptor tableDescriptor, ImmutableIntList idxColumnMapping) {
        Element[] elements = IntStream.of(idxColumnMapping.toIntArray())
                .mapToObj(tableDescriptor::columnDescriptor)
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
            ImmutableIntList idxColumnMapper,
            RowHandler.RowFactory<RowT> factory,
            RowT searchRow
    ) {
        RowHandler<RowT> handler = factory.handler();

        int prefixColumnsCount = binarySchema.elementCount();

        //TODO IGNITE-18056: Uncomment. Search row must be a valid index row prefix.
        // assert handler.columnCount(searchRow) <= binarySchema.elementCount() : "Invalid range condition";
        //
        // int specifiedCols = handler.columnCount(searchRow);

        int specifiedCols = 0;
        for (int i = 0; i < prefixColumnsCount; i++) {
            if (handler.get(idxColumnMapper.get(i), searchRow) != ectx.unspecifiedValue()) {
                specifiedCols++;
            }
        }

        BinaryTuplePrefixBuilder tupleBuilder = new BinaryTuplePrefixBuilder(specifiedCols, prefixColumnsCount);

        return new BinaryTuplePrefix(binarySchema, toByteBuffer(ectx, binarySchema, idxColumnMapper, handler, tupleBuilder, searchRow));
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
            ImmutableIntList idxColumnMapper,
            RowHandler.RowFactory<RowT> factory,
            RowT searchRow
    ) {
        RowHandler<RowT> handler = factory.handler();

        int prefixColumnsCount = binarySchema.elementCount();

        //TODO IGNITE-18056: Uncomment. Search row must be a valid index row.
        // assert handler.columnCount(searchRow) == binarySchema.elementCount() : "Invalid lookup condition";

        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(prefixColumnsCount, binarySchema.hasNullableElements());

        return new BinaryTuple(binarySchema, toByteBuffer(ectx, binarySchema, idxColumnMapper, handler, tupleBuilder, searchRow));
    }

    private static <RowT> ByteBuffer toByteBuffer(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            ImmutableIntList idxColumnMapper,
            RowHandler<RowT> handler,
            BinaryTupleBuilder tupleBuilder,
            RowT searchRow
    ) {
        for (int i = 0; i < binarySchema.elementCount(); i++) {
            Object val = handler.get(idxColumnMapper.get(i), searchRow);

            if (val == ectx.unspecifiedValue()) {
                break;
            }

            Element element = binarySchema.element(i);

            val = TypeUtils.fromInternal(ectx, val, NativeTypeSpec.toClass(element.typeSpec(), element.nullable()));

            BinaryConverter.appendValue(tupleBuilder, element, val);
        }

        return tupleBuilder.build();
    }
}
