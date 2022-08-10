/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowSerializer;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * {@link IndexRowSerializer} implementation that uses {@link BinaryTuple} as the index keys serialization mechanism.
 */
class BinaryTupleRowSerializer implements IndexRowSerializer {
    private final SortedIndexDescriptor descriptor;

    BinaryTupleRowSerializer(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public IndexRow createIndexRow(Object[] columnValues, RowId rowId) {
        if (columnValues.length != descriptor.indexColumns().size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected %d, got %d",
                    descriptor.indexColumns().size(),
                    columnValues.length
            ));
        }

        return new IndexRowImpl(createIndexRowPrefix(columnValues), rowId);
    }

    @Override
    public BinaryTuple createIndexRowPrefix(Object[] prefixColumnValues) {
        if (prefixColumnValues.length > descriptor.indexColumns().size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected not more than %d, got %d",
                    descriptor.indexColumns().size(),
                    prefixColumnValues.length
            ));
        }

        Element[] prefixElements = descriptor.indexColumns().stream()
                .limit(prefixColumnValues.length)
                .map(columnDescriptor -> new Element(columnDescriptor.type(), columnDescriptor.nullable()))
                .toArray(Element[]::new);

        BinaryTupleSchema prefixSchema = BinaryTupleSchema.create(prefixElements);

        BinaryTupleBuilder builder = BinaryTupleBuilder.create(prefixSchema);

        for (Object value : prefixColumnValues) {
            builder.appendValue(prefixSchema, value);
        }

        return new BinaryTuple(prefixSchema, builder.build());
    }
}
