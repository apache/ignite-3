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

package org.apache.ignite.internal.storage.index;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * Index descriptor.
 */
public interface StorageIndexDescriptor {
    /**
     * Index column descriptor.
     */
    interface StorageColumnDescriptor {
        /**
         * Returns the name of an index column.
         */
        String name();

        /**
         * Returns a column type.
         */
        NativeType type();

        /**
         * Returns {@code true} if this column can contain null values or {@code false} otherwise.
         */
        boolean nullable();
    }

    /**
     * Returns the index ID.
     */
    int id();

    /**
     * Returns index column descriptions.
     */
    List<? extends StorageColumnDescriptor> columns();

    /** Returns {@code true} for the primary index. */
    boolean isPk();

    /**
     * Creates an index description based on the catalog descriptors.
     *
     * @param table Catalog table descriptor.
     * @param index Catalog index descriptor.
     */
    static StorageIndexDescriptor create(CatalogTableDescriptor table, CatalogIndexDescriptor index) {
        if (index instanceof CatalogHashIndexDescriptor) {
            return new StorageHashIndexDescriptor(table, (CatalogHashIndexDescriptor) index);
        }

        if (index instanceof CatalogSortedIndexDescriptor) {
            return new StorageSortedIndexDescriptor(table, ((CatalogSortedIndexDescriptor) index));
        }

        throw new IllegalArgumentException("Unknown type: " + index);
    }

    /**
     * Gets the column native type from the catalog table column descriptor.
     *
     * @param column Table column descriptor.
     */
    static NativeType getNativeType(CatalogTableColumnDescriptor column) {
        switch (column.type()) {
            case BOOLEAN:
                return NativeTypes.BOOLEAN;
            case INT8:
                return NativeTypes.INT8;
            case INT16:
                return NativeTypes.INT16;
            case INT32:
                return NativeTypes.INT32;
            case INT64:
                return NativeTypes.INT64;
            case FLOAT:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DECIMAL:
                return NativeTypes.decimalOf(column.precision(), column.scale());
            case DATE:
                return NativeTypes.DATE;
            case TIME:
                return NativeTypes.time(column.precision());
            case DATETIME:
                return NativeTypes.datetime(column.precision());
            case TIMESTAMP:
                return NativeTypes.timestamp(column.precision());
            case UUID:
                return NativeTypes.UUID;
            case STRING:
                return NativeTypes.stringOf(column.length());
            case BYTE_ARRAY:
                return NativeTypes.blobOf(column.length());
            default:
                throw new IllegalArgumentException("Unknown type: " + column.type());
        }
    }
}
