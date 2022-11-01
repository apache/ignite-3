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

import java.util.stream.IntStream;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
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
                .map(colDesc -> new Element(colDesc.physicalType(), colDesc.nullable()))
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
    public static <RowT> BinaryTuple toBinaryTuplePrefix(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            ImmutableIntList idxColumnMapper,
            RowHandler.RowFactory<RowT> factory,
            RowT searchRow
    ) {
        RowHandler<RowT> handler = factory.handler();

        int prefixColumnsCount = binarySchema.elementCount();

        //TODO: create ticket. Make SearchRow conpatible with index row.
//        assert binarySchema.elementCount() >= handler.columnCount(searchRow) : "Invalid lookup condition";

        //TODO: avoid strems here
        int specifiedCols = (int)IntStream.range(0, handler.columnCount(searchRow))
                .mapToObj(i -> handler.get(i, searchRow))
                .filter(o -> o != ectx.unspecifiedValue())
                .count();

        BinaryTuplePrefixBuilder tupleBuilder = new BinaryTuplePrefixBuilder(specifiedCols, prefixColumnsCount);

        return toBinaryTuple(ectx, binarySchema, idxColumnMapper, handler, tupleBuilder, searchRow);
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

        //TODO: create ticket. Make SearchRow conpatible with index row.
//        assert prefixColumnsCount == handler.columnCount(searchRow) : "Invalid lookup condition";

        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(prefixColumnsCount, binarySchema.hasNullableElements());

        return toBinaryTuple(ectx, binarySchema, idxColumnMapper, handler, tupleBuilder, searchRow);
    }

    private static <RowT> BinaryTuple toBinaryTuple(
            ExecutionContext<RowT> ectx,
            BinaryTupleSchema binarySchema,
            ImmutableIntList idxColumnMapper,
            RowHandler<RowT> handler,
            BinaryTupleBuilder tupleBuilder,
            RowT searchRow
    ) {
        for (int i = 0; i < tupleBuilder.numElements(); i++) {
            Object val = handler.get(idxColumnMapper.get(i), searchRow);

            if (val == ectx.unspecifiedValue()) {
                break;
            }

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
