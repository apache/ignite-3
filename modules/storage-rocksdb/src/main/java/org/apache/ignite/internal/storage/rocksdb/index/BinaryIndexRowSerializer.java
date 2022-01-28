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

package org.apache.ignite.internal.storage.rocksdb.index;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * {@link IndexRowDeserializer} implementation that uses {@link BinaryRow} infrastructure for deserialization purposes.
 */
class BinaryIndexRowSerializer implements IndexRowSerializer, IndexRowDeserializer {
    private final SortedIndexDescriptor descriptor;

    BinaryIndexRowSerializer(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public IndexRow deserialize(IndexBinaryRow binRow) {
        return new IndexRowImpl(binRow.keySlice(), binRow.valueSlice(), descriptor);
    }

    @Override
    public IndexBinaryRow serialize(IndexRow row) {
        RowAssembler rowAssembler = createRowAssembler(row);

        for (Column column : descriptor.schema().keyColumns().columns()) {
            Object columnValue = row.value(column.columnOrder());

            RowAssembler.writeValue(rowAssembler, column, columnValue);
        }

        return new IndexBinaryRowImpl(rowAssembler.build(), row.primaryKey());
    }

    /**
     * Creates a {@link RowAssembler} that can later be used to serialized the given column mapping.
     */
    private RowAssembler createRowAssembler(IndexRow row) {
        SchemaDescriptor schemaDescriptor = descriptor.schema();

        int nonNullVarlenKeyCols = 0;

        for (Column column : schemaDescriptor.keyColumns().columns()) {
            Object columnValue = row.value(column.columnOrder());

            if (!column.type().spec().fixedLength() && columnValue != null) {
                nonNullVarlenKeyCols += 1;
            }
        }

        return new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, 0);
    }
}
