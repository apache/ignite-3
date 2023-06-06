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

package org.apache.ignite.internal.index;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for working with indexes.
 */
class IndexUtils {
    /**
     * Converts a catalog index descriptor to an event index descriptor.
     *
     * @param descriptor Catalog index descriptor.
     */
    static IndexDescriptor toEventIndexDescriptor(org.apache.ignite.internal.catalog.descriptors.IndexDescriptor descriptor) {
        if (descriptor instanceof org.apache.ignite.internal.catalog.descriptors.HashIndexDescriptor) {
            return toEventHashIndexDescriptor(((org.apache.ignite.internal.catalog.descriptors.HashIndexDescriptor) descriptor));
        }

        if (descriptor instanceof org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor) {
            return toEventSortedIndexDescriptor(((org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor) descriptor));
        }

        throw new IllegalArgumentException("Unknown index type: " + descriptor);
    }

    /**
     * Converts a catalog hash index descriptor to an event hash index descriptor.
     *
     * @param descriptor Catalog hash index descriptor.
     */
    static IndexDescriptor toEventHashIndexDescriptor(org.apache.ignite.internal.catalog.descriptors.HashIndexDescriptor descriptor) {
        return new IndexDescriptor(descriptor.name(), descriptor.columns());
    }

    /**
     * Converts a catalog sorted index descriptor to an event sorted index descriptor.
     *
     * @param descriptor Catalog sorted index descriptor.
     */
    static SortedIndexDescriptor toEventSortedIndexDescriptor(
            org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor descriptor
    ) {
        List<String> columns = new ArrayList<>(descriptor.columns().size());
        List<ColumnCollation> collations = new ArrayList<>(descriptor.columns().size());

        for (org.apache.ignite.internal.catalog.descriptors.IndexColumnDescriptor column : descriptor.columns()) {
            columns.add(column.name());

            collations.add(toEventCollation(column.collation()));
        }

        return new SortedIndexDescriptor(descriptor.name(), columns, collations);
    }

    private static ColumnCollation toEventCollation(org.apache.ignite.internal.catalog.descriptors.ColumnCollation collation) {
        return ColumnCollation.get(collation.asc(), collation.nullsFirst());
    }
}
