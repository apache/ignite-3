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

package org.apache.ignite.internal.table.distributed.index;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;

/** This class encapsulates the logic of conversion from table row to a particular index key. */
class TableRowToIndexKeyConverter implements ColumnsExtractor {
    private final SchemaRegistry registry;

    private final String[] indexedColumns;

    private final Object mutex = new Object();

    private volatile VersionedConverter converter = new VersionedConverter(-1, null);

    TableRowToIndexKeyConverter(SchemaRegistry registry, String[] indexedColumns) {
        this.registry = registry;
        this.indexedColumns = indexedColumns;
    }

    @Override
    public BinaryTuple extractColumns(BinaryRow row) {
        return converter(row).extractColumns(row);
    }

    private ColumnsExtractor converter(BinaryRow row) {
        int schemaVersion = row.schemaVersion();

        VersionedConverter converter = this.converter;

        if (converter.version() != schemaVersion) {
            synchronized (mutex) {
                converter = this.converter;

                if (converter.version() != schemaVersion) {
                    converter = createConverter(schemaVersion);

                    this.converter = converter;
                }
            }
        }

        return converter;
    }

    /** Creates converter for given version of the schema. */
    private VersionedConverter createConverter(int schemaVersion) {
        SchemaDescriptor descriptor = registry.schema(schemaVersion);

        int[] indexedColumns = resolveColumnIndexes(descriptor);

        var rowConverter = BinaryRowConverter.columnsExtractor(descriptor, indexedColumns);

        return new VersionedConverter(descriptor.version(), rowConverter);
    }

    private int[] resolveColumnIndexes(SchemaDescriptor descriptor) {
        int[] result = new int[indexedColumns.length];

        for (int i = 0; i < indexedColumns.length; i++) {
            Column column = descriptor.column(indexedColumns[i]);

            assert column != null : "schemaVersion=" + descriptor.version() + ", column=" + indexedColumns[i];

            result[i] = column.positionInRow();
        }

        return result;
    }
}
