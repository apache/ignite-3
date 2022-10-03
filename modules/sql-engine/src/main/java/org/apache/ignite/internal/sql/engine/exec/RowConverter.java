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

import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

/**
 * Hepler class provides method to convert binary tuple to rows and vice-versa.
 */
public final class RowConverter {
    /**
     * Creates binary tuple schema for index rows.
     */
    public static BinaryTupleSchema createIndexRowSchema(TableDescriptor tableDescriptor, IndexDescriptor indexDescriptor) {
        Element[] elements = indexDescriptor.columns().stream()
                .map(colName -> tableDescriptor.columnDescriptor(colName))
                .map(colDesc -> new Element(colDesc.physicalType(), colDesc.nullable()))
                .toArray(Element[]::new);

        return BinaryTupleSchema.create(elements);
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

        int prefixColumnsCount = handler.columnCount(searchRow);
        int totalColumnsCount = binarySchema.elementCount();

        assert prefixColumnsCount == totalColumnsCount : "Invalid lookup condition";

        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(prefixColumnsCount, binarySchema.hasNullableElements());

        return toBinaryTuple(ectx, binarySchema, handler, tupleBuilder, searchRow);
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
    public static <RowT> BinaryTuple toBinaryTuplePrefix(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            RowHandler.RowFactory<RowT> factory,
            RowT searchRow
    ) {
        RowHandler<RowT> handler = factory.handler();

        int prefixColumnsCount = handler.columnCount(searchRow);
        int totalColumnsCount = binarySchema.elementCount();
        BinaryTupleBuilder tupleBuilder = new BinaryTuplePrefixBuilder(prefixColumnsCount, totalColumnsCount);

        return toBinaryTuple(ectx, binarySchema, handler, tupleBuilder, searchRow);
    }

    private static <RowT> BinaryTuple toBinaryTuple(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            RowHandler<RowT> handler,
            BinaryTupleBuilder tupleBuilder,
            RowT searchRow
    ) {
        int prefixColumnsCount = handler.columnCount(searchRow);

        for (int i = 0; i < prefixColumnsCount; i++) {
            Object val = handler.get(i, searchRow);

            Element element = binarySchema.element(i);

            val = TypeUtils.fromInternal(ectx, val, NativeTypeSpec.toClass(element.typeSpec(), element.nullable()));

            BinaryConverter.appendValue(tupleBuilder, element, val);
        }

        return new BinaryTuple(binarySchema, tupleBuilder.build());
    }

    /**
     * Converts binary tuple to row.
     *
     * @param ectx Execution context.
     * @param binTuple Binary tuple.
     * @param factory Row handler factory.
     * @param <RowT> Row type.
     * @return Row.
     */
    public static <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryTuple binTuple,
            RowHandler.RowFactory<RowT> factory
    ) {
        RowHandler<RowT> handler = factory.handler();
        RowT res = factory.create();

        for (int i = 0; i < binTuple.count(); i++) {
            handler.set(i, res, TypeUtils.toInternal(ectx, binTuple.value(i)));
        }

        return res;
    }
}
