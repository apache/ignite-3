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

package org.apache.ignite.internal.storage.index.impl;

import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.type.NativeType;

/**
 * Class for converting an array of objects into a {@link BinaryTuple} and vice-versa using a given index schema.
 */
public class BinaryTupleRowSerializer {
    private static class ColumnDescriptor {
        final NativeType type;

        final boolean nullable;

        ColumnDescriptor(NativeType type, boolean nullable) {
            this.type = type;
            this.nullable = nullable;
        }
    }

    private final List<ColumnDescriptor> schema;

    private final BinaryTupleSchema tupleSchema;

    /**
     * Creates a new instance for an index.
     */
    public BinaryTupleRowSerializer(StorageIndexDescriptor descriptor) {
        this(descriptor.columns().stream()
                .map(colDesc -> new ColumnDescriptor(colDesc.type(), colDesc.nullable()))
                .collect(toUnmodifiableList()));
    }

    private BinaryTupleRowSerializer(List<ColumnDescriptor> schema) {
        this.schema = schema;

        Element[] elements = schema.stream()
                .map(columnDescriptor -> new Element(columnDescriptor.type, columnDescriptor.nullable))
                .toArray(Element[]::new);

        tupleSchema = BinaryTupleSchema.create(elements);
    }

    /**
     * Creates an {@link IndexRow} from the given index columns and a Row ID.
     */
    public IndexRow serializeRow(Object[] columnValues, RowId rowId) {
        if (columnValues.length != schema.size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected %d, got %d",
                    schema.size(),
                    columnValues.length
            ));
        }

        var builder = new BinaryTupleBuilder(tupleSchema.elementCount());

        for (Object value : columnValues) {
            appendValue(builder, value);
        }

        var tuple = new BinaryTuple(tupleSchema.elementCount(), builder.build());

        return new IndexRowImpl(tuple, rowId);
    }

    /**
     * Creates a prefix of an {@link IndexRow} using the provided columns.
     */
    public BinaryTuplePrefix serializeRowPrefix(Object... prefixColumnValues) {
        if (prefixColumnValues.length > schema.size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected not more than %d, got %d",
                    schema.size(),
                    prefixColumnValues.length
            ));
        }

        var builder = new BinaryTuplePrefixBuilder(prefixColumnValues.length, schema.size());

        for (Object value : prefixColumnValues) {
            appendValue(builder, value);
        }

        return new BinaryTuplePrefix(tupleSchema.elementCount(), builder.build());
    }

    /**
     * Converts a byte representation of index columns back into Java objects.
     */
    public Object[] deserializeColumns(IndexRow indexRow) {
        BinaryTuple tuple = indexRow.indexColumns();

        assert tuple.elementCount() == schema.size();

        var result = new Object[schema.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = tupleSchema.value(tuple, i);
        }

        return result;
    }

    /**
     * Append a value for the current element.
     *
     * @param builder Builder.
     * @param value Element value.
     */
    private void appendValue(BinaryTupleBuilder builder, Object value) {
        Element element = tupleSchema.element(builder.elementIndex());

        BinaryRowConverter.appendValue(builder, element, value);
    }
}
